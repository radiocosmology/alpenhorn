"""Routines for updating the state of a node.
"""

import time
import logging

import peewee as pw

from . import auto_import, config
from .archive import ArchiveFileCopy, ArchiveFileCopyRequest
from .pool import global_abort
from .querywalker import QueryWalker
from .storage import StorageNode

log = logging.getLogger(__name__)


def update_loop(host, queue, pool):
    """Main loop of alepnhornd.

    This is the main update loop for the alpenhorn daemon.

    Parameters
    ----------
    host : string
        the name of the host we're running on (must match the value in the DB)
    queue : FairMultiFIFOQueue
        the task manager
    pool : WorkerPool
        the pool of worker threads (may be empty)

    The daemon cycles through the update loop until it is terminated in
    one of three ways:

    - receiving SIGINT (AKA KeyboardInterrupt).  This causes a clean exit.
    - a global abort caused by an uncaught exception in a worker thread.  This
        causes a clean exit.
    - a crash due to an uncaught exception in the main thread.  This does _not_
        cause a clean exit.

    During a clean exit, alpenhornd will try to finish in-progress tasks before
    shutting down.
    """
    while not global_abort.is_set():
        loop_start = time.time()

        # Used to remember the results if idle checks for later.
        group_idle = dict()
        node_idle = dict()

        # Iterate over nodes first
        #
        # nodes and are re-queried every loop iteration so we can
        # detect changes in available storage media
        for node in StorageNode.select().where(
            StorageNode.host == host, StorageNode.active == True
        ):
            # Init I/O, if necessary.
            node.io.set_queue(queue)

            # Update group_idle.  As we loop through the nodes, this builds up
            # a list of which groups are available on this host and whether they
            # were idle before node I/O happened.
            group_idle[node.group] = group_idle.get(node.group, True) and node.io.idle

            # Perform the node update, maybe
            updated = update_node(node, queue)

            # After the update, check again whether I/O is ongoing or if we're idle
            if updated:
                node_idle[node] = node.io.idle
            else:
                node_idle[node] = None

        # Group updates
        for group in group_idle:
            updated = update_group(group, host, queue, group_idle[group])

            # After the update, check again whether I/O is ongoing or if we're idle
            if updated:
                group_idle[group] = group.io.idle
            else:
                group_idle[group] = None

        # Regular I/O updates are done.  If any nodes or groups are idle after that,
        # run the idle updates, but only if the update happened for that group.

        # loop over all the nodes and run the idle updates if we're idle
        for node in node_idle:
            if node_idle[node] is True:
                node.io.idle_update()

                # Run auto-verfiy, if requested
                if node.auto_verify > 0:
                    auto_verify(node)

        # Ditto for groups
        for group in group_idle:
            if group_idle[group] is True:
                group.io.idle_update()

        # Lean into duck-typing and just mush everything together to
        # loop over all the things and run the post-update hooks
        # (also note: groups come first, here)
        for item in (group_idle | node_idle).items():
            thing, idle = item
            thing.io.after_update(idle)

        # Done with the I/O updates, do some housekeeping:

        # Respawn workers that have exited (due to DB error)
        pool.check()

        # If we have no workers, handle some queued I/O tasks
        if len(pool) == 0:
            serial_io()

        # Check the time spent so far
        loop_time = time.time() - loop_start
        log.info(f"Main loop execution was {loop_time} sec.")

        # Pool and queue info
        log.info(
            f"Tasks: {queue.qsize()} queued, {queue.deferred_size()} deferred, "
            f"{queue.inprogress_size()} in-progress on {len(pool)} workers"
        )

        # Avoid looping too fast.
        remaining = config.config["service"]["update_interval"] - loop_time
        if remaining > 0:
            global_abort.wait(remaining)  # Stops waiting if a global abort is triggered


def serial_io(queue):
    """Execute I/O tasks from the queue

    This function is only called when alpenhorn has no worker threads.  It runs
    I/O tasks in the main loop for a limited period of time.
    """
    start_time = time.time()

    # Handle tasks for, say, 15 minutes at most
    while time.time() - start_time < 900:
        # Get a task
        item = queue.get(timeout=1)

        # Out of things to do
        if item is None:
            break

        # Run the task
        task, key = item

        log.debug(f"Beginning task {task}")
        task()
        queue.task_done(key)
        log.debug(f"Finished task {task}")


def update_group(group, host, queue, idle):
    """Perform I/O updates on a StorageGroup.

    Returns a boolean indicating whether I/O happened or not (i.e. it was
    skipped).
    """

    # Find all active nodes in this group on this host
    nodes_in_group = list(
        StorageNode.select().where(
            StorageNode.active == True,
            StorageNode.host == host,
            StorageNode.group == group,
        )
    )

    # Call the before update hook
    do_update = group.io.before_update(nodes_in_group, idle)

    # Update only happens if the queue is empty and the I/O layer hasn't
    # cancelled the update
    if idle and do_update:
        log.info(f'Updating group "{group.name}".')

        # Process pulls into this group
        for req in ArchiveFileCopyRequest.select().where(
            ArchiveFileCopyRequest.completed == 0,
            ArchiveFileCopyRequest.cancelled == 0,
            ArchiveFileCopyRequest.group_to == group,
        ):
            # What's the current situation on the destination?
            copy_state = group.copy_state(req.file)
            if copy_state == "Y":
                log.info(
                    f"Cancelling pull request for "
                    f"{req.file.acq.name}/{req.file.name}: "
                    f"already present in group {group.name}."
                )
                ArchiveFileCopyRequest.update(cancelled=True).where(
                    ArchiveFileCopyRequest.id == req.id
                ).execute()
                continue
            elif copy_state == "M":
                log.warning(
                    f"Skipping pull request for "
                    f"{req.file.acq.name}/{req.file.name}: "
                    f"existing copy in group {group.name} needs check."
                )
                continue
            elif copy_state == "X":
                # If the file is corrupt, we continue with the
                # pull to overwrite the corrupt file
                pass
            elif copy_state == "N":
                # Check whether an actual file exists on the target.
                node = group.io.exists(req.file.path)
                if node is not None:
                    # file on disk: create/update the ArchiveFileCopy
                    # to force a check next pass
                    log.warning(
                        f"Skipping pull request for "
                        f"{req.file.acq.name}/{req.file.name}: "
                        f"file already on disk in group {group.name}."
                    )
                    log.info(
                        f"Requesting check of "
                        f"{req.file.acq.name}/{req.file.name} on node "
                        f"{node.name}."
                    )
                    # Upsert ArchiveFileCopy to force a check
                    ArchiveFileCopy.replace(
                        file=req.file,
                        node=node,
                        has_file="M",
                        wants_file="Y",
                        prepared=False,
                        size_b=node.io.filesize(req.file.path, actual=True),
                    ).execute()
                    continue
            else:
                # Shouldn't get here
                log.error(
                    f"Unexpected copy state: '{copy_state}' "
                    f"for file ID={req.file.id} in group {group.name}."
                )
                continue

            # Skip request unless the source node is active
            if not req.node_from.active:
                log.warning(
                    f"Skipping request for {req.file.acq.name}/{req.file.name}:"
                    f" source node {req.node_from.name} is not active."
                )
                continue

            # If the source file doesn't exist, skip the request.
            #
            # XXX Cancel instead?
            if not req.node_from.copy_present(req.file):
                log.warning(
                    f"Skipping request for {req.file.acq.name}/{req.file.name}:"
                    f" not available on node {req.node_from.name}. "
                    f"[file_id={req.file.id}]"
                )
                continue

            # If the source file is not ready, skip the request.
            if not req.node_from.remote.pull_ready(req.file):
                log.info(
                    f"Skipping request for {req.file.acq.name}/{req.file.name}:"
                    f" not ready on node {req.node_from.name}."
                )
                continue

            # Early checks passed: dispatch this request to the Group I/O layer
            group.io.pull(req)
    else:
        log.info(
            f"Skipping update for group {group.name}:"
            f"idle={idle} do_update={do_update}"
        )

    return idle and do_update


def update_node(node, queue):
    """Perform I/O updates on a StorageNode.

    Returns a boolean indicating whether I/O happened or not (i.e. it was
    skipped).
    """

    # Is this node's FIFO empty?  If not, we'll skip this
    # update since we can't know whether we'd duplicate tasks
    # or not
    idle = node.io.idle

    # Update (start or stop) an auto-import observer for this node if needed
    auto_import.update_observer(node, queue)

    # Pre-update hook
    do_update = node.io.before_update(idle)

    if idle and do_update:
        log.info(f'Updating node "{node.name}".')

        # Check if the node is actually active
        if not update_node_active(node):
            return

        # Check and update the amount of free space
        node.io.update_avail_gb()

        # Check the integrity of any questionable files (has_file=M)
        for copy in ArchiveFileCopy.select().where(
            ArchiveFileCopy.node == node, ArchiveFileCopy.has_file == "M"
        ):
            log.info(
                f'Checking copy "{copy.file.acq.name}/{copy.file.name}" on node'
                f" {node.name}."
            )

            # Dispatch integrity check to I/O layer
            node.io.check(copy)

        # Delete any unwanted files to cleanup space
        update_node_delete(node)

        # Process any pull requests from this node
        for req in ArchiveFileCopyRequest.select().where(
            ArchiveFileCopyRequest.completed == 0,
            ArchiveFileCopyRequest.cancelled == 0,
            ArchiveFileCopyRequest.node_from == node,
        ):
            node.io.ready_pull(req)
    else:
        log.info(
            f"Skipping update for node {node.name}:"
            f"idle={idle} do_update={do_update}"
        )

    # Update the amount of free space, again; this is always done
    node.io.update_avail_gb()

    return idle and do_update


def update_node_active(node):
    """Check if the node is actually active in the system.

    Returns True if it is."""

    if node.active:
        if node.io.check_active():
            return True
        else:
            # Mark the node as inactive in the database
            node.active = False
            node.save(only=StorageNode.active)
            log.info(
                f'Correcting the database: node "f{node.name}" is now set to '
                "inactive."
            )
    else:
        log.warning(f'Attempted to update inactive node "f{node.name}"')

    return False


def update_node_delete(node):
    """Process this node for files to delete."""

    # Find all file copies needing deletion on this node
    #
    # If we have less than the minimum available space, we should consider all
    # files not explicitly wanted (i.e. wants_file != 'Y') as candidates for
    # deletion, provided the copy is not on an archive node. If we have more
    # than the minimum space, or we are on archive node then only those
    # explicitly marked (wants_file == 'N') will be removed.
    if node.under_min() and not node.archive:
        log.info(
            f"Hit minimum available space on {node.name} -- "
            "considering all unwanted files for deletion!"
        )
        dfclause = ArchiveFileCopy.wants_file != "Y"
    else:
        dfclause = ArchiveFileCopy.wants_file == "N"

    # Search db for candidates on this node to delete.
    del_copies = list()
    for copy in (
        ArchiveFileCopy.select()
        .where(
            dfclause,
            ArchiveFileCopy.node == node,
            ArchiveFileCopy.has_file == "Y",
        )
        .order_by(ArchiveFileCopy.id)
    ):
        # Group a bunch of these together to reduce the number of I/O Tasks
        # created.  TODO: figure out if this actually helps
        if len(del_copies) >= 10:
            node.io.delete(del_copies)
            del_copies = [copy]
        else:
            del_copies.append(copy)

    # Handle the partial group at the end (which may be empty)
    node.io.delete(del_copies)


qws = dict()


def auto_verify(node):
    """Run auto-verification on nodes that request it during idle times."""

    global qws
    if node.name not in qws:
        try:
            qws[node.name] = QueryWalker(
                ArchiveFileCopy,
                ArchiveFileCopy.node == node,
                ArchiveFileCopy.has_file != "N",
            )
        except pw.DoesNotExist:
            return  # No files to verify

    # Get some files to re-verify
    try:
        copies = qws[node.name].get(node.auto_verify)
    except pw.DoesNotExist:
        # No files to verify; delete query walker to try to re-init next time
        del qws[node.name]
        return

    for copy in copies:
        log.info(
            f'Auto-verifing copy "{copy.file.acq.name}/{copy.file.name}" on node'
            f" {node.name}."
        )

        node.io.auto_verify(copy)
