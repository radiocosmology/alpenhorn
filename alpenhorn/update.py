"""Routines for updating the state of a node.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import json
import time
import logging
import peewee as pw
from datetime import datetime

from . import auto_import, config, util
from .archive import ArchiveFileCopy, ArchiveFileCopyRequest
from .extensions import io_module
from .pool import global_abort, WorkerPool, EmptyPool
from .querywalker import QueryWalker
from .storage import StorageNode, StorageGroup

if TYPE_CHECKING:
    from .queue import FairMultiFIFOQueue

log = logging.getLogger(__name__)


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
        self.io.set_storage(storage)
        return False


class RemoteNode(updateable_base):
    """Remote storage node.

    This represents a (potentially) non-local node used
    as the source-side of a pull request.

    Parameters
    ----------
    node : StorageNode
        The underlying StorageNode instance
    """

    is_group = False

    def __init__(self, node: StorageNode) -> None:
        self.db = node
        self.io_class = self._get_io_class()
        config = self._parse_io_config(node.io_config)
        self.io = self.io_class.remote_class(node, config)


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

        # Set to True whenever I/O tasks are started.
        # Set to False whenever idle updates happened.
        #
        # Used to let the idle_update hooks know whether this is the
        # first idle update to happen after some I/O or not
        self._io_happened = True

        # Set in reinit()
        self.db = None
        self.reinit(node)

    def reinit(self, node: StorageNode) -> bool:
        """Re-initialise the instance with a new database object.

        Called once per update loop.

        Parameters
        ----------
        node : StorageNode
            The new StorageNode.

        Returns
        -------
        did_reinit : bool
            True if re-init happened.
        """
        # Most of the work is done in the base class reinit()
        did_reinit = super().reinit(node)

        if did_reinit:
            # QueryWalker for auto-verifcation, if enabled
            self._av_walker = None

        return did_reinit

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
                log.warning(f'Ignoring node "{self.name}": not ready for I/O.')
        else:
            log.warning(f'Ignoring node "{self.name}": deactivated during update.')

        return False

    def update_free_space(self) -> None:
        """Calculate and record free space.

        The free space is found by calling `self.io.bytes_avail()`
        and saved to the database via `self.db.update_avail_gb()`
        """
        # This is always a slow call
        bytes_avail = self.io.bytes_avail(fast=False)

        self.db.update_avail_gb(bytes_avail)

        if bytes_avail is not None:
            log.info(f"Node {self.name}: {util.pretty_bytes(bytes_avail)} available.")

    def run_auto_verify(self) -> None:
        """Run auto-verification on this node.

        This is a single iteration of auto-verification.  The number of
        files which will be auto-verfied in this iteration is equal to
        `self.db.auto_verify`.
        """

        if self._av_walker is None:
            try:
                self._av_walker = QueryWalker(
                    ArchiveFileCopy,
                    ArchiveFileCopy.node == self.db,
                    ArchiveFileCopy.has_file != "N",
                )
            except pw.DoesNotExist:
                return  # No files to verify

        # Get some files to re-verify
        try:
            copies = self._av_walker.get(self.db.auto_verify)
        except pw.DoesNotExist:
            # No files to verify; delete query walker to trigger re-init next time
            self._av_walker = None
            return

        done = set()  # Set of copies being verified already
        for copy in copies:
            # No need to check the same file more than once in a single update
            if copy in done:
                continue

            done.add(copy)

            copy_age_days = (time.time() - copy.last_update.timestamp()) / 86400.0
            if copy_age_days <= config.config["service"]["auto_verify_min_days"]:
                continue  # Too new to re-verify

            log.info(
                f'Auto-verifing copy "{copy.file.acq.name}/{copy.file.name}" on node'
                f" {self.name}."
            )

            self._io_happened = True
            self.io.auto_verify(copy)

    def update_idle(self) -> None:
        """Perform idle updates, if appropriate.

        The idle updates are run if the regular update() ran but
        the node is currently idle.
        """
        if self._updated and self.idle:
            # Do any I/O class idle updates
            self.io.idle_update(self._io_happened)

            self._io_happened = False

            # Run auto-verify, if requested
            if self.db.auto_verify > 0:
                self.run_auto_verify()

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
            # Don't delete file copies which are the source for pending
            # copy requests
            if (
                ArchiveFileCopyRequest.select()
                .where(
                    ArchiveFileCopyRequest.file == copy.file,
                    ArchiveFileCopyRequest.node_from == self.db,
                    ArchiveFileCopyRequest.completed == 0,
                    ArchiveFileCopyRequest.cancelled == 0,
                )
                .count()
            ):
                log.info(
                    f"Skipping delete of {copy.file.path} on node {self.name}: transfer pending"
                )
                continue

            # Group a bunch of these together to reduce the number of I/O Tasks
            # created.  TODO: figure out if this actually helps
            if len(del_copies) >= 10:
                self._io_happened = True
                self.io.delete(del_copies)
                del_copies = [copy]
            else:
                del_copies.append(copy)

        # Handle the partial group at the end (which may be empty)
        if len(del_copies) > 0:
            self._io_happened = True
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

        # Update (start or stop) an auto-import observer for this node if needed
        auto_import.update_observer(self, self._queue)

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
                self._io_happened = True
                self.io.check(copy)

            # Delete any unwanted files to cleanup space
            self.update_delete()

            # Prepare files for pulls out from this node
            for req in ArchiveFileCopyRequest.select().where(
                ArchiveFileCopyRequest.completed == 0,
                ArchiveFileCopyRequest.cancelled == 0,
                ArchiveFileCopyRequest.node_from == self.db,
            ):
                if self.db.filecopy_present(req.file):
                    self._io_happened = True
                    self.io.ready_pull(req)
                else:
                    log.info(
                        "Ignoring ready request for "
                        f"{req.file.acq.name}/{req.file.name} "
                        f"on node {self.name}: not present."
                    )

            self._updated = True
        else:
            log.info(
                f"Skipping update for node {self.name}: "
                + ("busy" if not idle else "cancelled")
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

        self.reinit(group=group, nodes=nodes, idle=idle)

    def reinit(
        self, *, group: StorageGroup, nodes: list[UpdateableNode], idle: bool
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

    def update_pull(self, req: ArchiveFileCopyRequest) -> None:
        """Process pull request `req`.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            The pull request to process.
        """
        # What's the current situation on the destination?
        copy_state = self.db.filecopy_state(req.file)
        if copy_state == "Y":
            # We mark the AFCR cancelled rather than complete becase
            # _this_ AFCR clearly hasn't been responsible for creating
            # the file copy.
            log.info(
                f"Cancelling pull request for "
                f"{req.file.acq.name}/{req.file.name}: "
                f"already present in group {self.name}."
            )
            ArchiveFileCopyRequest.update(cancelled=True).where(
                ArchiveFileCopyRequest.id == req.id
            ).execute()
            return
        elif copy_state == "M":
            log.warning(
                f"Skipping pull request for "
                f"{req.file.acq.name}/{req.file.name}: "
                f"existing copy in group {self.name} needs check."
            )
            return
        elif copy_state == "X":
            # If the file is corrupt, we continue with the
            # pull to overwrite the corrupt file
            pass
        elif copy_state == "N":
            # Check whether an actual file exists on the target.
            node = self.io.exists(req.file.path)
            if node is not None:
                # file on disk: create/update the ArchiveFileCopy
                # to force a check next pass
                log.warning(
                    f"Skipping pull request for "
                    f"{req.file.acq.name}/{req.file.name}: "
                    f"file already on disk in group {self.name}."
                )
                log.info(
                    f"Requesting check of "
                    f"{req.file.acq.name}/{req.file.name} on node "
                    f"{node.name}."
                )

                # Update/create ArchiveFileCopy to force a check.

                # ready == False is the safe option here: copy will be readied
                # during the subsequent check if needed.
                count = (
                    ArchiveFileCopy.update(
                        has_file="M",
                        wants_file="Y",
                        ready=False,
                        last_update=datetime.utcnow(),
                    )
                    .where(
                        ArchiveFileCopy.file == req.file,
                        ArchiveFileCopy.node == node.db,
                    )
                    .execute()
                )
                if count == 0:
                    # Create new copy
                    ArchiveFileCopy.create(
                        file=req.file,
                        node=node.db,
                        has_file="M",
                        wants_file="Y",
                        ready=False,
                        size_b=node.io.filesize(req.file.path, actual=True),
                    )
                return
        else:
            # Shouldn't get here
            log.error(
                f"Unexpected copy state: '{copy_state}' "
                f"for file ID={req.file.id} in group {self.name}."
            )
            return

        # Skip request unless the source node is active
        if not req.node_from.active:
            log.warning(
                f"Skipping request for {req.file.acq.name}/{req.file.name}:"
                f" source node {req.node_from.name} is not active."
            )
            return

        # If the source file doesn't exist, cancel the request.
        if not req.node_from.filecopy_present(req.file):
            log.warning(
                f"Cancelling request for {req.file.acq.name}/{req.file.name}:"
                f" not available on node {req.node_from.name}. "
                f"[file_id={req.file.id}]"
            )
            ArchiveFileCopyRequest.update(cancelled=True).where(
                ArchiveFileCopyRequest.id == req.id
            ).execute()
            return

        # If the source file is not ready, skip the request.
        node_from = RemoteNode(req.node_from)
        if not node_from.io.pull_ready(req.file):
            log.debug(
                f"Skipping request for {req.file.acq.name}/{req.file.name}:"
                f" not ready on node {req.node_from.name}."
            )
            return

        # Early checks passed: dispatch this request to the Group I/O layer
        self.io.pull(req)

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

            # Remember ArchiveFiles that we're pulling, so we don't end up with
            # overlapping pulls (which would try to write to the same file).
            seen_files = set()

            # Process pulls into this group
            for req in ArchiveFileCopyRequest.select().where(
                ArchiveFileCopyRequest.completed == 0,
                ArchiveFileCopyRequest.cancelled == 0,
                ArchiveFileCopyRequest.group_to == self.db,
            ):
                if req.file not in seen_files:
                    seen_files.add(req.file)
                    self.update_pull(req)

            # Check for idleness at the end
            self._do_idle_updates = self.idle
        else:
            log.info(
                f"Skipping update for group {self.name}: "
                + ("busy" if not self._init_idle else "cancelled")
            )

    def update_idle(self) -> None:
        """Perform idle updates, if appropriate.

        The idle updates are run if the regular update() ran but
        the group was idle when it finished.
        """
        if self._do_idle_updates:
            self.io.idle_update()


def update_loop(queue: FairMultiFIFOQueue, pool: WorkerPool | EmptyPool) -> None:
    """Main loop of alepnhornd.

    This is the main update loop for the alpenhorn daemon.

    Parameters
    ----------
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

    # Get the name of this host
    host = util.get_hostname()

    # The nodes and groups we're working on.  These will be updated
    # each time through the main loop, whenever the underlying storage objects
    # change.  These are stored as dicts with keys being the name of the node
    # or group for faster look-up
    nodes = dict()
    groups = dict()

    while not global_abort.is_set():
        loop_start = time.time()

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
                # Stop auto-import, if running
                auto_import.update_observer(nodes[name], queue, force_stop=True)

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
                    new_groups[group_name]["idle"] and node.idle
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
