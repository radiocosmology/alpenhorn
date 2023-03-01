"""Routines for updating the state of a node.
"""
from __future__ import annotations
from typing import TYPE_CHECKING

import os
import json
import time
import logging
import peewee as pw
import datetime as dt
from peewee import fn

from . import acquisition as ac
from . import archive as ar
from . import storage as st
from . import config, util
from .archive import ArchiveFileCopy
from .extensions import io_module
from .pool import global_abort, WorkerPool, EmptyPool
from .storage import StorageNode, StorageGroup

if TYPE_CHECKING:
    from .queue import FairMultiFIFOQueue

log = logging.getLogger(__name__)

# Parameters.
max_time_per_node_operation = 300  # Don't let node operations hog time.

# Globals.
done_transport_this_cycle = False


class updateable_base:
    """Abstract base class for UpdateableNode and UpdateableGroup.

    After instantiation, these subclasses provide access to the I/O instance
    via the `io` attribute and the underlying database object (StorageNode
    or StorageGroup) via the `db` attribute.
    """

    # Set to True or False in subclasses
    is_group = None

    def __init__(self) -> None:
        raise NotImplementedError("updateable_base cannot be instantiated directly.")

    @property
    def name(self) -> str:
        """The name of this instance."""
        return self.db.name

    def _check_io_reinit(self, new: StorageNode | StorageGroup) -> bool:
        """Do we need to re-initialise our I/O instance?

        Parameters
        ----------
        new : StorageNode or StorageGrou
            The new storage object for this update loop iteration

        Returns
        -------
        do_reinit : bool
            True if re-init needs to happen.
        """

        # db is None if this is a new instance
        if self.db is None:
            return True

        # io is None if the I/O class wasn't found
        if self.io is None:
            return True

        if self.db.id != new.id:
            return True

        if self.db.io_config != new.io_config:
            return True

        if self.db.io_class != new.io_class:
            return True

        return False

    def _parse_io_config(self, config_json: str | None) -> dict:
        """Parse and return the I/O config.

        Parameters
        ----------
        config_json : str or None
            The I/O config JSON string

        Returns
        -------
        io_config : dict
            The parsed I/O config, or an empty dict() if `config_json`
            was `None.  This value is also assigned to `self._io_config`.

        Raises
        ------
        ValueError
            `config_json` did not evaluate to a dict.
        """
        if config_json is None:
            self._io_config = dict()
        else:
            self._io_config = json.loads(config_json)

            if not isinstance(self._io_config, dict):
                raise ValueError(f'Invalid io_config: "{config_json}".')

        return self._io_config

    def _get_io_class(self):
        """Return the I/O class for our Storage object."""

        # If no io_class is specified, the Default I/O classes are used
        io_name = "Default" if self.db.io_class is None else self.db.io_class

        # We assume StorageNode if not StorageGroup
        if self.is_group:
            obj_type = "StorageGroup"
            io_suffix = "GroupIO"
        else:
            obj_type = "StorageNode"
            io_suffix = "NodeIO"

        # Load the module
        module = io_module(io_name)
        if module is None:
            log.error(
                f'No module for I/O class "{io_name}".  Ignoring {obj_type} {self.name}.'
            )
            return None

        io_name += io_suffix

        # Within the module, find the class
        try:
            class_ = getattr(module, io_name)
        except AttributeError as e:
            raise ImportError(
                f'I/O class "{io_name}" not found in module "{module}". '
                f"Required for {obj_type} {self.name}."
            ) from e

        # return the class
        return class_

    def reinit(self, storage: StorageNode | StorageGroup) -> bool:
        """Re-initialise the instance with a new database object.

        Called once per update loop.

        Parameters
        ----------
        storage : StorageNode or StorageGroup
            The newly-fetched database storage instance

        Returns
        -------
        did_reinit : bool
            True if the I/O object was re-initialised
        """
        # Does I/O instance need to be re-instantiated?
        if self._check_io_reinit(storage):
            self.db = storage
            self.io_class = self._get_io_class()

            if self.io_class is None:
                # Error locating I/O module
                self.io = None
                return False

            # Parse I/O config if present
            config = self._parse_io_config(storage.io_config)

            if self.is_group:
                self.io = self.io_class(storage, config)
            else:
                # Nodes also need the queue
                self.io = self.io_class(storage, config, self._queue)

            return True

        # No re-init, update I/O instance's Storage object
        self.db = storage
        self.io.update(storage)
        return False


class UpdateableNode(updateable_base):
    """Updateable storage node

    This is a container class which combines a StorageNode
    and its I/O class, and implements the update logic
    for the node.

    Parameters
    ----------
    queue : FairMultiFIFOQueue
        The task queue/scheduler
    node : StorageNode
        The underlying StorageNode instance
    """

    is_group = False

    def __init__(self, queue: FairMultiFIFOQueue, node: StorageNode) -> None:
        self._queue = queue
        self._updated = False

        # Set in reinit()
        self.db = None
        self.reinit(node)

    @property
    def idle(self) -> bool:
        """Is I/O occurring on this node?

        True whenever the queue FIFO associated with this node is empty;
        False otherwise."""
        return self._queue.fifo_size(self.name) == 0

    def update_active(self) -> bool:
        """Check if we are actually active on the host.

        This is an I/O check, rather than a database check.

        Returns
        -------
        active : bool
            Whether or not the node is active.
        """

        if self.db.active:
            if self.io.check_active():
                return True
            else:
                # Mark the node as inactive in the database
                self.db.active = False
                self.db.save(only=[StorageNode.active])
                log.info(
                    f'Correcting the database: node "{self.name}" is now set to '
                    "inactive."
                )
        else:
            log.warning(f'Attempted to update inactive node "{self.name}"')

        return False

    def update_free_space(self) -> None:
        """Calculate and record free space.

        The free space is found by calling `self.io.bytes_avail()`
        and saved to the database via `self.db.update_avail_gb()`
        """
        # This is always a slow call
        self.db.update_avail_gb(self.io.bytes_avail(fast=False))

    def update_idle(self) -> None:
        """Perform idle updates, if appropriate.

        The idle updates are run if the regular update() ran but
        the node is currently idle.
        """
        if self._updated and self.idle:
            # Do any I/O class idle updates
            self.io.idle_update()

    def update_delete(self) -> None:
        """Process this node for files to delete."""

        # Find all file copies needing deletion on this node
        #
        # If we have less than the minimum available space, we should consider all
        # files not explicitly wanted (i.e. wants_file != 'Y') as candidates for
        # deletion, provided the copy is not on an archive node. If we have more
        # than the minimum space, or we are on archive node then only those
        # explicitly marked (wants_file == 'N') will be removed.
        if self.db.under_min and not self.db.archive:
            log.info(
                f"Hit minimum available space on {self.name} -- "
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
                ArchiveFileCopy.node == self.db,
                ArchiveFileCopy.has_file == "Y",
            )
            .order_by(ArchiveFileCopy.id)
        ):
            # Group a bunch of these together to reduce the number of I/O Tasks
            # created.  TODO: figure out if this actually helps
            if len(del_copies) >= 10:
                self.io.delete(del_copies)
                del_copies = [copy]
            else:
                del_copies.append(copy)

        # Handle the partial group at the end (which may be empty)
        self.io.delete(del_copies)

    def update(self) -> None:
        """Perform I/O updates on this node.

        Sets self._updated to indicate whether the update happened or
        not.
        """

        # Is this node's FIFO empty?  If not, we'll skip this
        # update since we can't know whether we'd duplicate tasks
        # or not
        idle = self.idle

        # Pre-update hook
        do_update = self.io.before_update(idle)

        # Check and update the amount of free space
        # This is always done, even if skipping the update
        self.update_free_space()

        if idle and do_update:
            log.info(f'Updating node "{self.name}".')

            # Check the integrity of any questionable files (has_file=M)
            for copy in ArchiveFileCopy.select().where(
                ArchiveFileCopy.node == self.db, ArchiveFileCopy.has_file == "M"
            ):
                log.info(
                    f'Checking copy "{copy.file.acq.name}/{copy.file.name}" '
                    f"on node {self.name}."
                )

                # Dispatch integrity check to I/O layer
                self.io.check(copy)

            # Delete any unwanted files to cleanup space
            self.update_delete()

            # Process any regular transfers requests onto this node
            # XXX Commented out until future PR.
            # update_node_requests(node)

            self._updated = True
        else:
            log.info(
                f"Skipping update for node {self.name}: "
                f"idle={idle} do_update={do_update}"
            )
            self._updated = False


class UpdateableGroup(updateable_base):
    """Updateable Group

    This is a container class which combines a StorageGroup
    and its I/O class, and implements the update logic
    for the group.

    Parameters
    ----------
    group : StorageGroup
        The underlying StorageGroup instance
    nodes : list of UpdateableNodes
        The nodes active on this host in this group
    idle : bool
        Were all nodes in `nodes` idle at the start of the
        current update loop?
    """

    is_group = True

    def __init__(
        self, *, group: StorageGroup, nodes: list[UpdateableNode], idle: bool
    ) -> None:
        # Set in reinit()
        self.db = None
        self._do_idle_updates = False

        self.reinit(group, nodes, idle)

    def reinit(
        self, group: StorageGroup, nodes: list[UpdateableNode], idle: bool
    ) -> None:
        """Re-initialise the UpdateableGroup.

        Called once per update loop.

        Parameters
        ----------
        group : StorageGroup
            The newly-fetched StorageGroup instance
        nodes : list of UpdateableNodes
            The nodes active on this host in this group
        idle : bool
            Were all nodes in `nodes` idle at the start of the
            current update loop?
        """
        self._init_idle = idle

        # Takes care of I/O re-init
        super().reinit(group)

        try:
            self._nodes = self.io.set_nodes(nodes)
        except ValueError as e:
            # I/O layer didn't like the nodes we gave it
            log.warning(str(e))
            self._nodes = None

    @property
    def idle(self) -> bool:
        """Is this group idle?

        False whenever any consitiuent node is not idle."""

        # A group with no nodes is not idle.
        if self._nodes is None:
            return False

        # If any node is not idle, the group is not idle.
        for node in self._nodes:
            if not node.idle:
                return False

        return True

    def update(self) -> None:
        """Perform I/O updates on the group"""

        self._do_idle_updates = False

        # If the available nodes weren't acceptable to the I/O layer, do nothing
        if self._nodes is None:
            return

        # Call the before update hook
        do_update = self.io.before_update(self._init_idle)

        # Update only happens if the queue is empty and the I/O layer hasn't
        # cancelled the update
        if self._init_idle and do_update:
            log.info(f'Updating group "{self.name}".')

            # TODO: do group I/O here.

            # Check for idleness at the end
            self._do_idle_updates = self.idle
        else:
            log.info(
                f"Skipping update for group {self.name}: "
                f"idle={self._init_idle} do_update={do_update}"
            )

    def update_idle(self) -> None:
        """Perform idle updates, if appropriate.

        The idle updates are run if the regular update() ran but
        the group was idle when it finished.
        """
        if self._do_idle_updates:
            self.io.idle_update()


def update_loop(
    host: str, queue: FairMultiFIFOQueue, pool: WorkerPool | EmptyPool
) -> None:
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
    global done_transport_this_cycle

    # The nodes and groups we're working on.  These will be updated
    # each time through the main loop, whenever the underlying storage objects
    # change.  These are stored as dicts with keys being the name of the node
    # or group for faster look-up
    nodes = dict()
    groups = dict()

    while not global_abort.is_set():
        loop_start = time.time()
        done_transport_this_cycle = False

        # Nodes are re-queried every loop iteration so we can
        # detect changes in available storage media
        try:
            new_nodes = {
                node.name: node
                for node in (
                    StorageNode.select()
                    .where(StorageNode.host == host, StorageNode.active == True)
                    .execute()
                )
            }
        except pw.DoesNotExist:
            new_nodes = dict()

        if len(new_nodes) == 0:
            log.warning(f"No active nodes on host ({host})!")

        # Drop any nodes that have gone away
        for name in nodes:
            if name not in new_nodes:
                log.info(f'Node "{name}" no longer available.')
                del nodes[name]

        # List of groups present this update loop
        new_groups = dict()

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
                nodes[name] = UpdateableNode(queue, new_nodes[name])

            node = nodes[name]

            # Check if we found the I/O class for this node:
            if node.io_class is None:
                del nodes[name]  # Can't do anything with this
                continue

            # Check if the node is actually active
            if not node.update_active():
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
                    new_groups[group_name]["idle"] and node.io.idle
                )

        # Drop groups that are no longer available
        for name in groups:
            if name not in new_groups:
                log.info(f'Group "{name}" no longer available.')
                del groups[name]

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
                groups[name] = UpdateableGroup(**new_groups[name])

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

        # Pool and queue info
        log.info(
            f"Tasks: {queue.qsize} queued, {queue.deferred_size} deferred, "
            f"{queue.inprogress_size} in-progress on {len(pool)} workers"
        )

        # Avoid looping too fast.
        remaining = config.config["service"]["update_interval"] - loop_time
        if remaining > 0:
            global_abort.wait(remaining)  # Stops waiting if a global abort is triggered

    # Warn on exit
    log.warning("Exiting due to global abort")


def serial_io(queue: FairMultiFIFOQueue) -> None:
    """Execute I/O tasks from the queue

    This function is only called when alpenhorn has no worker threads.  It runs
    I/O tasks in the main loop for a limited period of time.
    """
    start_time = time.time()

    # Handle tasks for, say, 15 minutes at most
    while time.time() - start_time < config.config["service"]["serial_io_timeout"]:
        # Get a task
        item = queue.get(timeout=1)

        # Out of things to do
        if item is None:
            break

        # Run the task
        task, key = item

        log.info(f"Beginning task {task}")
        task()
        queue.task_done(key)
        log.info(f"Finished task {task}")


def update_node_requests(node):
    """Process file copy requests onto this node."""
    import shutil
    from .io import ioutil

    global done_transport_this_cycle

    # TODO: Fix up HPSS support
    # Ensure we are not on an HPSS node
    # if is_hpss_node(node):
    #     log.error("Cannot process HPSS node here.")
    #     return

    avail_gb = node.avail_gb

    # Skip if node is too full
    if avail_gb < (node.min_avail_gb + 10):
        log.info("Node %s is nearly full. Skip transfers." % node.name)
        return

    # Calculate the total archive size from the database
    size_query = (
        ac.ArchiveFile.select(fn.Sum(ac.ArchiveFile.size_b))
        .join(ar.ArchiveFileCopy)
        .where(ar.ArchiveFileCopy.node == node, ar.ArchiveFileCopy.has_file == "Y")
    )

    size = size_query.scalar(as_tuple=True)[0]
    current_size_gb = float(0.0 if size is None else size) / 2**30.0

    # Stop if the current archive size is bigger than the maximum (if set, i.e. > 0)
    if current_size_gb > node.max_total_gb and node.max_total_gb > 0.0:
        log.info(
            "Node %s has reached maximum size (current: %.1f GB, limit: %.1f GB)"
            % (node.name, current_size_gb, node.max_total_gb)
        )
        return

    # ... OR if this is a transport node quit if the transport cycle is done.
    if node.storage_type == "T" and done_transport_this_cycle:
        log.info("Ignoring transport node %s" % node.name)
        return

    start_time = time.time()

    # Fetch requests to process from the database
    requests = ar.ArchiveFileCopyRequest.select().where(
        ~ar.ArchiveFileCopyRequest.completed,
        ~ar.ArchiveFileCopyRequest.cancelled,
        ar.ArchiveFileCopyRequest.group_to == node.group,
    )

    # Add in constraint that node_from cannot be an HPSS node
    requests = requests.join(st.StorageNode).where(st.StorageNode.address != "HPSS")

    for req in requests:
        if time.time() - start_time > max_time_per_node_operation:
            break  # Don't hog all the time.

        # By default, if a copy fails, we mark the source file as suspect
        # so it gets re-MD5'd on the source node.
        check_source_on_err = True

        # Only continue if the node is actually active
        if not req.node_from.active:
            continue

        # For transport disks we should only copy onto the transport
        # node if the from_node is local, this should prevent pointlessly
        # rsyncing across the network
        if node.storage_type == "T" and node.host != req.node_from.host:
            log.debug(
                "Skipping request for %s/%s from remote node [%s] onto local "
                "transport disks"
                % (req.file.acq.name, req.file.name, req.node_from.name)
            )
            continue

        # Only proceed if the destination file does not already exist.
        try:
            ar.ArchiveFileCopy.get(
                ar.ArchiveFileCopy.file == req.file,
                ar.ArchiveFileCopy.node == node,
                ar.ArchiveFileCopy.has_file == "Y",
            )
            log.info(
                "Skipping request for %s/%s since it already exists on "
                'this node ("%s"), and updating DB to reflect this.'
                % (req.file.acq.name, req.file.name, node.name)
            )
            ar.ArchiveFileCopyRequest.update(completed=True).where(
                ar.ArchiveFileCopyRequest.file == req.file
            ).where(ar.ArchiveFileCopyRequest.group_to == node.group).execute()
            continue
        except pw.DoesNotExist:
            pass

        # Only proceed if the source file actually exists (and is not corrupted).
        try:
            ar.ArchiveFileCopy.get(
                ar.ArchiveFileCopy.file == req.file,
                ar.ArchiveFileCopy.node == req.node_from,
                ar.ArchiveFileCopy.has_file == "Y",
            )
        except pw.DoesNotExist:
            log.error(
                "Skipping request for %s/%s since it is not available on "
                'node "%s". [file_id=%i]'
                % (req.file.acq.name, req.file.name, req.node_from.name, req.file.id)
            )
            continue

        # Check that there is enough space available.
        if node.avail_gb * 2**30.0 < 2.0 * req.file.size_b:
            log.warning(
                'Node "%s" is full: not adding datafile "%s/%s".'
                % (node.name, req.file.acq.name, req.file.name)
            )
            continue

        # Constuct the origin and destination paths.
        from_path = "%s/%s/%s" % (req.node_from.root, req.file.acq.name, req.file.name)
        if req.node_from.host != node.host:
            if req.node_from.username is None or req.node_from.address is None:
                log.error(
                    "Source node (%s) not properly configured (username=%s, address=%s)",
                    req.node_from.name,
                    req.node_from.username,
                    req.node_from.address,
                )
                continue

            from_path = "%s@%s:%s" % (
                req.node_from.username,
                req.node_from.address,
                from_path,
            )

        to_file = os.path.join(node.root, req.file.acq.name, req.file.name)
        to_dir = os.path.dirname(to_file)
        if not os.path.isdir(to_dir):
            log.info('Creating directory "%s".' % to_dir)
            os.makedirs(to_dir)

        # For the potential error message later
        stderr = None

        # Giddy up!
        log.info('Transferring file "%s/%s".' % (req.file.acq.name, req.file.name))
        start_time = time.time()
        req.transfer_started = dt.datetime.fromtimestamp(start_time)
        req.save(only=req.dirty_fields)

        # Attempt to transfer the file. Each of the methods below needs to set a
        # return code `ret` and give an `md5sum` of the transferred file.

        # First we need to check if we are copying over the network
        if req.node_from.host != node.host:
            # First try bbcp which is a fast multistream transfer tool. bbcp can
            # calculate the md5 hash as it goes, so we'll do that to save doing
            # it at the end.
            if shutil.which("bbcp") is not None:
                ioresult = ioutil.bbcp(from_path, to_dir, req.file.size_b)
            # Next try rsync over ssh.
            elif shutil.which("rsync") is not None:
                ioresult = ioutil.rsync(from_path, to_dir, req.file.size_b, False)
            # If we get here then we have no idea how to transfer the file...
            else:
                log.warn("No commands available to complete this transfer.")
                check_source_on_err = False
                ret = -1

        # Okay, great we're just doing a local transfer.
        else:
            # First try to just hard link the file. This will only work if we
            # are on the same filesystem. As there's no actual copying it's
            # probably unecessary to calculate the md5 check sum, so we'll just
            # fake it.
            ioresult = ioutil.hardlink(from_path, to_dir, req.file.name)

            # If we couldn't just link the file, try copying it with rsync.
            if ioresult is None:
                if shutil.which("rsync") is not None:
                    ioresult = ioutil.rsync(from_path, to_dir, req.file.size_b, True)
                else:
                    log.warn("No commands available to complete this transfer.")
                    check_source_on_err = False
                    ret = -1

        ioutil.copy_request_done(
            req,
            node.io,
            check_src=ioresult.get("check_src", True),
            md5ok=ioresult.get("md5sum", None),
            start_time=start_time,
            stderr=ioresult.get("stderr", None),
            success=(ioresult["ret"] == 0),
        )
