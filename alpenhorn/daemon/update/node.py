"""UpdateableNode.

This is an active StorageNode which is being managed by alpenhornd.

Also the RemoteNode class, providing a high-level representation of
a StorageNode not locally available to alpenhornd.
"""

from __future__ import annotations

import logging
import pathlib
import time

import click
import peewee as pw

from ...common import config, util
from ...db import (
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    ArchiveFileImportRequest,
    StorageNode,
    utcnow,
)
from ..metrics import Metric
from ..querywalker import QueryWalker
from ..scheduler import FairMultiFIFOQueue, Task
from ._base import UpdateableBase

log = logging.getLogger(__name__)


class RemoteNode(UpdateableBase):
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

        # This ensures updatable_base.stop() does nothing.
        self._fifo = None


class UpdateableNode(UpdateableBase):
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
        self._fifo = None
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

        # These two are for the multiple daemon detection
        self.last_time = None
        self.last_time_failures = 0

        # Metrics that we want to delete when this node goes away
        self._idle_metric = Metric(
            "node_idle", "Node is idle", bound={"name": self.name}
        )

    def __del__(self):
        # Delete metrics
        try:
            self._idle_metric.remove()
        except (KeyError, AttributeError):
            # AttributeError will be raised if this instance wasn't fully initialised
            # KeyError will be raised if the prom metric was never created (because
            #   we never set the metric to anything)
            #   cf. https://github.com/prometheus/client_python/pull/1077
            pass

        super().__del__()

    def stop(self) -> None:
        """Stop updating.

        Stops auto-import, if running, and clears all pending tasks.
        """
        from .. import auto_import

        if self._fifo is not None:
            auto_import.update_observer(self, self._queue, force_stop=True)
        super().stop()

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
        return self._queue.fifo_size(self.io.fifo) == 0

    def check_init(self) -> bool:
        """Check if the node is initialised.

        This is an I/O check, rather than a database check.

        Returns
        -------
        initialised : bool
            Whether or not the node is initialised.
        """

        def _async(
            task: Task, node: UpdateableNode, req: ArchiveFileImportRequest
        ) -> None:
            """Task async to initialise `node`, if necessary.

            Completes `req` only if initialisation succeeds.
            """

            # recheck
            if node.io.check_init():
                log.info(f'Node "{node.name}" already initialised.')
                req.complete("duplicate")
                return

            # Run the init and check result
            if node.io.init() and node.io.check_init():
                log.info(f'Node "{node.name}" initialised.')
                req.complete("success")
                return

            # Otherwise, fail, and don't complete req
            log.warning(f'Initialisation failed for node "{node.name}".')

        if not self.db.active:
            log.warning(f'Ignoring node "{self.name}": deactivated during update.')
            return False

        if self.io.check_init():
            return True

        # We're active but not initialised.  Is there a pending init request?
        try:
            req = ArchiveFileImportRequest.get(
                node=self.db, path="ALPENHORN_NODE", completed=0
            )
            # Create a task to init the node
            Task(
                func=_async,
                queue=self._queue,
                key=self.io.fifo,
                args=(self, req),
                name=f'Init Node "{self.name}"',
            )
            log.info(f'Requesting init of node "{self.name}".')
            # But then we still ignore it for now.
        except pw.DoesNotExist:
            log.warning(f'Ignoring node "{self.name}": not initialised.')

        return False

    def update_free_space(self) -> None:
        """Calculate and record free space.

        The free space is found by calling `self.io.bytes_avail()`
        and saved to the database via `self.db.update_avail_gb()`

        This function is also responsible for detecting other daemons
        managing this node at the same time as us.
        """
        if self.last_time is not None:
            # Check for an unexpected node update
            last_update_check = StorageNode.get_by_id(
                self.db.id
            ).avail_gb_last_checked.timestamp()

            # We allow for a little slop just to hedge against DB storage
            # oddities
            if abs(self.last_time - last_update_check) > 2:
                # Increment the failed check count
                self.last_time_failures += 1

                # By default, we permit an occasional failure, and only decide
                # there's a problem if the check fails multiple times in a row.
                threshold = config.get_int(
                    "daemon.update_skew_threshold", default=4, min=0
                )

                # If the threshold is zero, the check is disabled
                if threshold and self.last_time_failures >= threshold:
                    message = (
                        "FATAL: Multiple simultaneous updates of node "
                        f'"{self.db.name}"!'
                    )
                    log.error(message)
                    raise click.ClickException(message)
            else:
                # If the time is good, reset the failure count
                self.last_time_failures = 0

        # This is always a slow call
        bytes_avail = self.io.bytes_avail(fast=False)

        self.db.update_avail_gb(bytes_avail, update_timestamp=True)

        if self.db.avail_gb is not None:
            log.info(
                f"Node {self.name}: "
                f"{util.pretty_bytes(self.db.avail_gb * 2**30)} available."
            )

        # Record the last update time, so we can check against it next time.
        self.last_time = self.db.avail_gb_last_checked.timestamp()

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

        for copy in copies:
            copy_age_days = (time.time() - copy.last_update.timestamp()) / 86400.0
            if copy_age_days <= config.get_int(
                "daemon.auto_verify_min_days", default=7, min=0
            ):
                continue  # Too new to re-verify

            log.info(
                f'Auto-verifing copy "{copy.file.acq.name}/{copy.file.name}" on node'
                f" {self.name}."
            )

            # Mark file as needing check
            copy.has_file = "M"
            copy.last_update = utcnow()
            copy.save()

    def update_idle(self) -> None:
        """Perform idle updates, if appropriate.

        The idle updates are run if the regular update() ran but
        the node is currently idle.
        """

        self._idle_metric.set(self.idle)
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
        # If we have less than the minimum available space, we need to delete
        # remove some of the files marked for discretionary clearning (i.e.
        # wants_file == 'M').  Figure out how much space we need to free to
        # get back above the minimum.
        if self.db.under_min and not self.db.archive:
            # under_min returns False if avail_gb is None, so this should be safe to do:
            avail_needed = int((self.db.min_avail_gb - self.db.avail_gb) * 2**30)
            log.info(
                f"Hit minimum available space on {self.name} -- "
                f"will attempt to free {util.pretty_bytes(avail_needed)}."
            )
            dfclause = ArchiveFileCopy.wants_file != "Y"
        else:
            dfclause = ArchiveFileCopy.wants_file == "N"
            avail_needed = 0

        # Search db for candidates on this node to delete.  The `dfclause` means
        # this query will only return wants_file == 'M' file copies when there's a
        # chance we'll delete them.
        del_copies = []
        for copy in (
            ArchiveFileCopy.select()
            .where(
                dfclause,
                ArchiveFileCopy.node == self.db,
                ArchiveFileCopy.has_file != "N",
            )
            .order_by(ArchiveFileCopy.id)
        ):
            # Check the database to see if this file can be deleted
            #
            # The value of "discretionary" here means we'll only delete files
            # marked for discretionary cleaning (wants_file == 'M') when we need
            # to get back over min_avail_gb.  Because avail_needed decreases as
            # we add files to the file deletion list, we'll never try to delete
            # more of these than the amount of space we need to clear up.
            if not copy.check_delete(discretionary=(avail_needed > 0)):
                continue

            # If we are trying to get back above min_avail_gb, keep a running total
            # of how much more deletion is needed.
            if avail_needed > 0:
                if copy.size_b:
                    avail_needed -= copy.size_b
                elif copy.file.size_b:
                    avail_needed -= copy.file.size_b

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

    def update_import(self) -> None:
        """Handle ArchiveFileImportRequests for this node."""

        from .. import auto_import

        # Loop over uncompleted requests for this node
        for req in ArchiveFileImportRequest.select().where(
            ArchiveFileImportRequest.node == self.db,
            ArchiveFileImportRequest.completed == 0,
        ):
            # Sanity checks
            path = pathlib.Path(req.path)
            if path.is_absolute():
                log.info(f'Not importing to "{self.name}" absolute path: {path}')
                req.complete("invalid")
                continue

            if req.path == "ALPENHORN_NODE":
                # This part of the update only runs if the node is already initialised,
                # so this request can't be something we need to deal with here
                log.info(
                    f'Ignoring node init request for "{self.name}": '
                    "already initialised."
                )
                req.complete("duplicate")
                continue

            if req.recurse:
                # Ensure the base directory we want to scan is in-tree
                fullpath = pathlib.Path(self.db.root, path)
                try:
                    fullpath = fullpath.resolve(strict=True)
                except OSError as e:
                    log.warning(
                        "Ignoring import request of unresolvable scan path: "
                        f"{fullpath}: {e}"
                    )
                    req.complete("invalid")
                    continue

                # Recompute the relative path after resolution, or skip scan if we're
                # now out-of-tree
                try:
                    path = fullpath.relative_to(self.db.root)
                except ValueError:
                    log.warning(
                        f"Ignoring import request of out-of-tree scan path: {fullpath}"
                    )
                    req.complete("invalid")
                    continue

                # Run scan
                Task(
                    func=auto_import.scan,
                    queue=self._queue,
                    key=self.io.fifo,
                    args=(self, self._queue, path, req.register, req),
                    name=f'Scan "{path}" on {self.name}',
                )
            else:
                # Check that the import path is valid
                rejection_reason = util.invalid_import_path(req.path)
                if rejection_reason:
                    log.warning(
                        f'Ignoring request for import of invalid path "{req.path}": '
                        + rejection_reason
                    )
                    req.complete("invalid")
                    continue

                # Try to directly import the path
                auto_import.import_file(self, self._queue, req.path, req.register, req)

    def update(self) -> None:
        """Perform I/O updates on this node.

        Sets self._updated to indicate whether the update happened or
        not.
        """

        from .. import auto_import

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
            Metric(
                "node_update",
                "Count of updates on a node",
                counter=True,
                bound={"name": self.name},
            ).inc()

            # Check the integrity of any questionable files (has_file=M)
            for copy in ArchiveFileCopy.select().where(
                ArchiveFileCopy.node == self.db,
                ArchiveFileCopy.has_file == "M",
                ArchiveFileCopy.wants_file != "N",
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

            # Process import requests
            self.update_import()

            # Prepare files for pulls out from this node
            remote = RemoteNode(self.db)
            for req in ArchiveFileCopyRequest.select().where(
                ArchiveFileCopyRequest.completed == 0,
                ArchiveFileCopyRequest.cancelled == 0,
                ArchiveFileCopyRequest.node_from == self.db,
            ):
                state = self.db.filecopy_state(req.file)
                if state == "Y":
                    if not remote.io.pull_ready(req.file):
                        self._io_happened = True
                        self.io.ready_pull(req)
                else:
                    reasons = {
                        "N": "not present",
                        "M": "needs check",
                        "X": "corrupt",
                    }
                    log.info(
                        "Ignoring ready request for "
                        f"{req.file.acq.name}/{req.file.name} "
                        f"on node {self.name}: {reasons[state]}."
                    )

            self._updated = True
        else:
            log.info(
                f"Skipping update for node {self.name}: "
                + ("busy" if not idle else "cancelled")
            )
            self._updated = False
