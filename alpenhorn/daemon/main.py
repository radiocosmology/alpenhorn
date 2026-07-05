"""Main update loop for the alpenhorn daemon."""

from __future__ import annotations

import logging
import socket
import time

import click
import peewee as pw

from ..common import config, util
from ..db import StorageHost, StorageNode
from .metrics import Metric
from .scheduler import EmptyPool, FairMultiFIFOQueue, WorkerPool, global_abort
from .update import UpdateableGroup, UpdateableNode

log = logging.getLogger(__name__)


# This stores the daemon's StorageHost.
_host = None


def host() -> StorageHost | None:
    """Return the current daemon host.

    If not called from within the daemon, or called before the start
    of the daemon main loop, this will be None.

    Returns
    -------
    StorageHost or None
        The daemon host, or None, if not called from within a running
        daemon.
    """
    global _host
    return _host


def _set_host() -> None:
    """Set the daemon host.

    If there is a ``daemon.host`` specified in the config, that is used.
    otherwise the local hostname up to the first '.' is used.

    Returns
    -------
    str
        The daemon host.
    """
    global _host

    hostname = config.get("daemon.host", default=None, as_type=str)
    if hostname is None:
        hostname = socket.gethostname().split(".")[0]

    # Try to find a StorageHost with this name
    try:
        _host = StorageHost.get(name=hostname)
    except pw.DoesNotExist:
        raise click.ClickException(f"No host record for this host ({hostname}).")

    return _host


def update_loop(
    queue: FairMultiFIFOQueue, pool: WorkerPool | EmptyPool, once: bool
) -> int:
    """Main loop of alepnhornd.

    This is the main update loop for the alpenhorn daemon.

    If `once` is false, the daemon cycles through the update loop until it is
    terminated in one of three ways:

    - receiving SIGINT (AKA KeyboardInterrupt).  This causes a clean exit.
    - a global abort caused by an uncaught exception in a worker thread.  This
        causes a clean exit.
    - a crash due to an uncaught exception in the main thread.  This does _not_
        cause a clean exit.

    During a clean exit, alpenhornd will try to finish in-progress tasks before
    shutting down.

    Parameters
    ----------
    queue : FairMultiFIFOQueue
        the task manager
    pool : WorkerPool
        the pool of worker threads (may be empty)
    once : bool
        If True, only run the loop once, wait for the queue to empty,
        and then exit.  If False, loop forever.

    Return
    ------
    result:
        0 if exiting after running once.  1 otherwise.
    """

    # The nodes and groups we're working on.  These will be updated
    # each time through the main loop, whenever the underlying storage objects
    # change.  These are stored as dicts with keys being the name of the node
    # or group for faster look-up
    nodes = {}
    groups = {}

    loop_time_metric = Metric(
        "main_loop_time_seconds", description="Main loop execution time", counter=False
    )
    loop_count_metric = Metric(
        "main_loops", description="Completed main loops", counter=True
    )
    node_avail_metric = Metric(
        "node_available",
        description="Node is available (active and initialized)",
        unbound={"name"},
    )
    group_avail_metric = Metric(
        "group_available",
        description="Group is available (active and initialized)",
        unbound={"name"},
    )
    worker_count_metric = Metric(
        "worker_count",
        description="Number of worker threads",
        bound={"pool_type": type(pool).__name__},
    )

    while not global_abort.is_set():
        loop_start = time.time()

        # Find the StorageHost record for this host.  We do this once
        # per update loop.  Raises ClickException if no host is found.
        host = _set_host()

        # Nodes are re-queried every loop iteration so we can
        # detect changes in available storage media
        try:
            new_nodes = {
                node.name: node
                for node in (
                    StorageNode.select()
                    .where(
                        StorageNode.host == host,
                        StorageNode.active == True,  # noqa: E712
                    )
                    .execute()
                )
            }
        except pw.DoesNotExist:
            new_nodes = {}

        if len(new_nodes) == 0:
            log.warning(f"No active nodes on host ({host.name})!")

        # Drop any nodes that have gone away
        vetted_nodes = {}
        for name, node in nodes.items():
            if name not in new_nodes:
                log.info(f'Node "{name}" no longer available.')
                # Stop updating
                node.stop()
                node_avail_metric.set(0, name=name)
            else:
                vetted_nodes[name] = node
        nodes = vetted_nodes

        # List of groups present this update loop
        new_groups = {}

        # Update the list of nodes:
        for name in new_nodes:
            if name in nodes:
                # Update the existing UpdateableNode.
                # This may result in the I/O instance for the
                # node being re-instantiated.
                nodes[name].reinit(new_nodes[name])
            else:
                # No existing node: create a new one.
                log.info(f'Node "{name}" now available.')
                node_avail_metric.set(1, name=name)
                nodes[name] = UpdateableNode(queue, new_nodes[name])

            node = nodes[name]

            # Check if we found the I/O class for this node:
            if node.io_class is None:
                del nodes[name]  # Can't do anything with this
                continue

            # Check if the node is actually active
            if not node.check_init():
                del nodes[name]  # Not active
                continue

            # Now update the list of new groups. This builds up a list of
            # groups which are currently active on this host and whether they
            # were idle before node I/O happened.
            group_name = node.db.group.name
            if group_name not in new_groups:
                new_groups[group_name] = {
                    "group": node.db.group,
                    "nodes": [node],
                    "idle": node.idle,
                }
            else:
                new_groups[group_name]["nodes"].append(node)
                new_groups[group_name]["idle"] = (
                    new_groups[group_name]["idle"] and node.idle
                )

        # Drop groups that are no longer available
        vetted_groups = {}
        for name, group in groups.items():
            if name not in new_groups:
                log.info(f'Group "{name}" no longer available.')
                # Stop updating
                group.stop()
                group_avail_metric.set(0, name=name)
            else:
                vetted_groups[name] = group
        groups = vetted_groups

        # Update the list of groups:
        for name in new_groups:
            if name in groups:
                # Update the existing UpdateableGroup.
                # This may result in the I/O instance for the
                # group being re-instantiated.
                groups[name].reinit(**new_groups[name])
            else:
                # No existing group: create a new one.
                log.info(f'Group "{name}" now available.')
                group_avail_metric.set(1, name=name)
                groups[name] = UpdateableGroup(queue=queue, **new_groups[name])

        # Node updates
        for node in nodes.values():
            # Perform the node update, maybe
            node.update()

        # Group updates
        for group in groups.values():
            group.update()

        # Regular I/O updates are done.  If any nodes or groups are idle after that,
        # run the idle updates, but only if the update happened for that group.

        for node in nodes.values():
            node.update_idle()

        # Ditto for groups, but we can also run the after-update hook already
        for group in groups.values():
            group.update_idle()
            group.io.after_update()

        # loop over all the nodes again and run their after-update hooks
        for node in nodes.values():
            node.io.after_update()

        # Done with the I/O updates, do some housekeeping:

        # Respawn workers that have exited (due to DB error)
        pool.check()

        # If we have no workers, handle some queued I/O tasks
        if len(pool) == 0:
            serial_io(queue)

        # Check the time spent so far
        loop_time = time.time() - loop_start
        log.info(f"Main loop execution was {util.pretty_deltat(loop_time)}.")

        # Update metrics
        loop_time_metric.set(loop_time)
        loop_count_metric.inc()
        worker_count_metric.set(len(pool))

        # Pool and queue info
        log.info(
            f"Tasks: {queue.qsize} queued, {queue.deferred_size} deferred, "
            f"{queue.inprogress_size} in-progress on {len(pool)} workers"
        )

        update_interval = config.get_int("daemon.update_interval", default=60, min=0)
        if once:
            # If we're in Exit-after-update mode, wait for updates to complete
            # and then return
            first_time = True
            while True:
                if queue.qsize + queue.inprogress_size + queue.deferred_size == 0:
                    log.info("Update complete.  Exiting.")
                    return 0

                if first_time:
                    first_time = False
                    log.info("Waiting for updates to complete.")

                # Wait a bit
                global_abort.wait(update_interval)
                log.info(
                    f"Tasks: {queue.qsize} queued, {queue.deferred_size} deferred, "
                    f"{queue.inprogress_size} in-progress on {len(pool)} workers"
                )
        else:
            # Not in EAU mode.  Avoid looping too fast.
            remaining = update_interval - loop_time
            if remaining > 0:
                # Stops waiting if a global abort is triggered
                global_abort.wait(remaining)

    # Warn on abnormal exit
    log.warning("Exiting due to global abort")
    return 1


def serial_io(queue: FairMultiFIFOQueue) -> None:
    """Execute I/O tasks from the queue

    This function is only called when alpenhorn has no worker threads.  It runs
    I/O tasks in the main loop for a limited period of time.
    """
    task_metric = Metric(
        "serialio_tasks", "Count of tasks run via Serial I/O", counter=True
    )

    # Handle tasks for a finite amount of time (15 minute by default)
    end_time = time.monotonic() + config.get_int(
        "daemon.serial_io_timeout", default=900, min=0
    )

    while time.monotonic() < end_time:
        # Get a task
        item = queue.get(timeout=1)

        # Out of things to do
        if item is None:
            break

        task_metric.inc()

        # Run the task
        task, key = item

        log.info(f"Beginning task {task}")
        task()
        queue.task_done(key)
        log.info(f"Finished task {task}")

    Metric("serialio_loops", "Count of Serial I/O loops", counter=True).inc()
