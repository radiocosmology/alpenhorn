"""Lustre Hierarchical Storage Management (HSM) I/O.

These I/O classes provide support for using external storage (typically
a tape system) as a StorageNode via Lustre's Hierarchical Storage
Management (HSM) framework.

This module provides:
* LustreHSMNodeIO and LustreHSMNodeRemote, representing a StorageNode on the
    HSM-managed external storage
* LustreHSMGroupIO, which allows pairing a `LustreHSMNodeIO` HSM StorageNode
    with a secondary StorageNode of a different class meant to store files
    too small for the external system.
"""

from __future__ import annotations

import logging
import pathlib
import time
from typing import IO, TYPE_CHECKING

import peewee as pw

from ..common.util import pretty_bytes, pretty_deltat
from ..daemon.querywalker import QueryWalker
from ..db import ArchiveFileCopy, utcnow
from ..scheduler import Task
from .base import BaseNodeRemote
from .default import DefaultGroupIO
from .lustrequota import LustreQuotaNodeIO

if TYPE_CHECKING:
    import os
    from collections.abc import Generator

    from ..db import ArchiveFile, ArchiveFileCopyRequest
    from ..scheduler import FairMultiFIFOQueue
    from ..service.update import UpdateableGroup, UpdateableNode
del TYPE_CHECKING

log = logging.getLogger(__name__)


class LustreHSMNodeRemote(BaseNodeRemote):
    """LustreHSMNodeRemote: information about a LustreHSM remote node."""

    def pull_ready(self, file: ArchiveFile) -> bool:
        """Is `file` ready for pulling from this remote node?

        Parameters
        ----------
        file : ArchiveFile
            the file being checked

        Returns
        -------
        ready : bool
            True if `file` is ready on the node; False otherwise.
        """
        try:
            copy = ArchiveFileCopy.get(file=file, node=self.node)
        except pw.DoesNotExist:
            return False

        return copy.ready


class LustreHSMNodeIO(LustreQuotaNodeIO):
    """LustreHSM node I/O.

    Required io_config keys:
        * quota_group : string
            the user group to query quota for
        * headroom: float
            the amount of space in kiB to keep empty on the disk.

    Optional io_config keys:
        * lfs : string
            the lfs(1) executable.  Defaults to "lfs"; may be a full path.
        * lfs_timeout: the timeout, in seconds, for an lfs(1) call.  Calls
            that run longer than this will be abandonned.  Defaults to 60
            seconds if not given.
        * fixed_quota : integer
            the quota, in kiB, on the Lustre disk backing the HSM system
        * release_check_count : integer
            The number of files to check at a time when doing idle HSM status
            update (see idle_update()).  Default is 100.
        * restore_wait : integer
            The number of seconds to wait between checking if a restore request
            has completed.  Default is 600 seconds (10 minutes).
    """

    remote_class = LustreHSMNodeRemote

    def __init__(
        self, node: UpdateableNode, config: dict, queue: FairMultiFIFOQueue
    ) -> None:
        super().__init__(node, config, queue)

        self._headroom = config["headroom"] * 2**10  # convert from kiB

        # QueryWalker for the HSM state check
        self._statecheck_qw = None

        # Tracks files we're in the process of retrieving, so we can avoid
        # waiting for the same file twice.  The elements in this set are
        # `ArchiveFile.id`s
        self._restoring = set()

        # For informational purposes.  Keys are elements in self._restoring.
        self._restore_start = {}

        self._restore_wait_time = int(config.get("restore_wait", 600))
        if self._restore_wait_time < 1:
            raise ValueError(
                "io_config key 'restore_wait' non-positive "
                f"(={self._restore_wait_time})"
            )

        # For idle-time HSM state updates
        self._nrelease = int(config.get("release_check_count", 100))
        if self._nrelease < 1:
            raise ValueError(
                f"io_config key 'release_check_count' non-positive (={self._nrelease})"
            )

    def _restore_wait(self, copy: ArchiveFileCopy) -> bool | None:
        """Attempt to restore a file from HSM.

        The caller should call this method repeatedly while it returns `True`,
        indicating the file has not yet been restored, with a suitable wait
        between calls.

        Parameters
        ----------
        copy : ArchiveFileCopy
            The file copy to restore

        Returns
        -------
        result, one of:
            * True: file is not restored.  Caller should wait and try again.
            * False: file is restored.  Caller may now access the restored file.
            * None: an error occurred.  Caller should stop trying to access the file.
        """

        # What's the current situation?
        state = self._lfs.hsm_state(copy.path)

        if state is None:
            log.warning(f"Unable to restore {copy.path}: state check failed.")
            self._restore_start.pop(copy.file.id, None)
            self._restoring.discard(copy.file.id)
            return None

        if state == self._lfs.HSM_MISSING:
            log.warning(f"Unable to restore {copy.path}: missing.")
            self._restore_start.pop(copy.file.id, None)
            self._restoring.discard(copy.file.id)
            return None

        if state == self._lfs.HSM_RESTORING:
            if copy.file.id not in self._restoring:
                self._restoring.add(copy.file.id)
                self._restore_start[copy.file.id] = time.monotonic()

            log.debug(f"Restore in progress: {copy.path}")

            # Tell the caller to wait
            return True
        if state != self._lfs.HSM_RELEASED:
            # i.e. file is restored or unarchived, so we're done.
            if copy.file.id not in self._restoring:
                log.debug(f"Already restored: {copy.path}")
            else:
                deltat = time.monotonic() - self._restore_start[copy.file.id]
                log.info(
                    f"{copy.file.path} restored on node {self.node.name} "
                    f"after {pretty_deltat(deltat)}"
                )
                del self._restore_start[copy.file.id]
                self._restoring.discard(copy.file.id)
            return False

        # If we got here, copy is released.
        if copy.file.id not in self._restoring:
            self._restoring.add(copy.file.id)
            self._restore_start[copy.file.id] = time.monotonic()

        # Try to restore it.
        result = self._lfs.hsm_restore(copy.path)

        if result is False:
            log.warning(f"Restore request failed: {copy.path}")
            # Reqeust failed. Abandon the restore attempt entirely,
            # in case it was deleted from the node.
            del self._restore_start[copy.file.id]
            self._restoring.discard(copy.file.id)

            # Report failure
            return None
        if result is None:
            # Might have worked.  Caller should check again in a bit.
            log.warning(f"Restore request timeout: {copy.path}")

        # Tell the caller to wait
        return True

    def release_files(self) -> None:
        """Release files from the HSM disk to keep the headroom clear.

        This is called at the start of a node update (from
        `before_update`), but only if the update is going
        to happen (i.e. the node is currently idle).
        """

        # Can't do anything if we don't know how much free space we have
        size_gib = self.node.avail_gb
        if size_gib is None:
            log.debug("Skipping release_files: free space unknown.")
            return

        headroom_needed = self._headroom - int(size_gib * 2**30)

        # Nothing to do
        if headroom_needed <= 0:
            return

        def _async(task, node, lfs, headroom_needed):
            total_files = 0
            total_bytes = 0

            # loop through file copies until we've released enough
            # (or we run out of files)
            for copy in (
                ArchiveFileCopy.select()
                .where(
                    ArchiveFileCopy.node == node,
                    ArchiveFileCopy.has_file == "Y",
                    ArchiveFileCopy.ready == True,  # noqa: E712
                )
                .order_by(ArchiveFileCopy.last_update)
            ):
                # The only files we can release are ones that are fully restored
                if lfs.hsm_state(copy.path) != lfs.HSM_RESTORED:
                    continue

                log.debug(
                    f"releasing file copy {copy.path} "
                    f"[={pretty_bytes(copy.file.size_b)}] on node {self.node.name}"
                )
                lfs.hsm_release(copy.path)
                # Update copy record immediately
                ArchiveFileCopy.update(ready=False, last_update=utcnow()).where(
                    ArchiveFileCopy.id == copy.id
                ).execute()
                total_files += 1
                total_bytes += copy.file.size_b
                if total_bytes >= headroom_needed:
                    break
            log.info(
                f"Released {pretty_bytes(total_bytes)} in {total_files} "
                f"{'file' if total_files == 1 else 'files'} "
                f"on node {self.node.name}"
            )
            return

        # Do the rest asynchronously
        Task(
            func=_async,
            queue=self._queue,
            key=self.fifo,
            args=(self.node, self._lfs, headroom_needed),
            name=f"Node {self.node.name}: HSM release {pretty_bytes(headroom_needed)}",
        )

    def before_update(self, idle: bool) -> bool:
        """Pre-update hook.

        If the node is idle (i.e. the update will happen), then
        call `self.release_files to potentially free up space.

        Parameters
        ----------
        idle : bool
            Is the node currently idle?

        Returns
        -------
        True
        """
        # Clear up headroom, if necessary
        if idle:
            self.release_files()

        # Continue with the update
        return True

    def idle_update(self, newly_idle) -> None:
        """Update HSM state of copies when idle.

        If the node is idle after an update, double check the HSM state
        for a few files and update the DB if necessary.

        Any I/O on a HSM file will restore it.  It's important for the
        data index to reflect changes made in this way outside of alpenhorn so that
        the alpenhornd daemon can properly manage free space.
        """

        # Run DefautlIO idle checks
        super().idle_update(newly_idle)

        # Check the query walker.  Initialised if necessary.
        if self._statecheck_qw is None:
            try:
                self._statecheck_qw = QueryWalker(
                    ArchiveFileCopy,
                    ArchiveFileCopy.node == self.node,
                    ArchiveFileCopy.has_file == "Y",
                )
            except pw.DoesNotExist:
                return  # No files to check on the node

        # Try to get a bunch of copies to check
        try:
            copies = self._statecheck_qw.get(self._nrelease)
        except pw.DoesNotExist:
            # Not sure why all the file copies have gone away, but given there's
            # nothing on the node now, can't hurt to re-init the QW in this case
            self._statecheck_qw = None
            return

        def _async(task, node, lfs, copies):
            for copy in copies:
                state = lfs.hsm_state(copy.path)
                if state is None:
                    log.warning(
                        f"Unable to determine state for {copy.file.path} "
                        f"on node {node.name}."
                    )
                elif state == lfs.HSM_MISSING:
                    # File is unexpectedly gone.
                    log.warning(
                        f"File copy {copy.file.path} on node {node.name} is missing!"
                    )
                    ArchiveFileCopy.update(
                        has_file="N", ready=False, last_update=utcnow()
                    ).where(ArchiveFileCopy.id == copy.id).execute()
                elif state == lfs.HSM_RELEASED or state == lfs.HSM_RESTORING:
                    if copy.ready:
                        log.info(f"Updating file copy {copy.file.path}: ready -> False")
                        ArchiveFileCopy.update(ready=False, last_update=utcnow()).where(
                            ArchiveFileCopy.id == copy.id
                        ).execute()
                else:  # i.e. RESTORED or UNARCHIVED
                    if not copy.ready:
                        log.info(f"Updating file copy {copy.file.path}: ready -> True")
                        ArchiveFileCopy.update(ready=True, last_update=utcnow()).where(
                            ArchiveFileCopy.id == copy.id
                        ).execute()

        # Copies get checked in an async
        Task(
            func=_async,
            queue=self._queue,
            key=self.fifo,
            args=(self.node, self._lfs, copies),
            name=f"Node {self.node.name}: HSM state check of {len(copies)} files",
        )

    # I/O METHODS

    def check(self, copy: ArchiveFileCopy) -> None:
        """Check a file in HSM.

        If the file is released, we trigger a restore and wait
        for it to become available.

        Then the file is verified.

        Finally, if the DB indicates the file is not ready, the
        file is released.

        The last step here is to keep auto-verifcation from replacing
        all the newly added data that's just arrived (which is presumably
        more interesting that some random files being auto-verified).

        Parameters
        ----------
        copy : ArchiveFileCopy
            the file copy to auto-verify
        """

        def _async(
            task: Task, node_io: LustreHSMNodeIO, copy: ArchiveFileCopy
        ) -> Generator[int]:
            """Verify a file (with restore and release, if necessary).

            Parameters
            ----------
            task : Task
                The task instance containing this async.
            node_io : LustreHSMNodeIO
                The node we're running on
            copy : ArchiveFileCopy
                The file copy to check
            """

            # The trivial case.
            if not node_io.exists(copy.file.path):
                # Only update if this is surprising (which it probably
                # is.)
                if copy.has_file != "N":
                    log.warning(
                        "File copy missing during check: "
                        f"{copy.path}.  Updating database."
                    )
                    ArchiveFileCopy.update(has_file="N", last_update=utcnow()).where(
                        ArchiveFileCopy.id == copy.id
                    ).execute()
                return

            # Trigger restore, if necessary
            restore_wait = node_io._restore_wait(copy)
            while restore_wait:
                # Wait for a bit
                yield self._restore_wait_time

                # Now check again
                restore_wait = node_io._restore_wait(copy)

            # If the restore failed, bail.
            if restore_wait is None:
                log.debug(f"Aborting check of {copy.path} after failed restore.")
                return

            # Do the check by inlining the Default-I/O function
            from ._default_asyncs import check_async

            check_async(task, node_io, copy)

            # Release the file if the DB says it should be
            if not ArchiveFileCopy.get(id=copy.id).ready:
                node_io._lfs.hsm_release(copy.path)

        # Only do this if another task isn't already restoring this file.
        if copy.file.id not in self._restoring:
            Task(
                func=_async,
                queue=self._queue,
                key=self.fifo,
                args=(self, copy),
                name=f"Check file {copy.file.path} on node {self.node.name}",
            )
        else:
            log.debug(f"Skipping check of {copy.path}: restore in progress.")

    def check_init(self) -> bool:
        """Check that this node is initialised.

        There's no ALPENHORN_NODE file on HSM, so just return True.
        """
        return True

    def exists(self, path: pathlib.PurePath) -> bool:
        """Does `path` exist?

        Checks whether `lfs hsm_state` returns ENOENT.

        Parameters
        ----------
        path : pathlib.PurePath
            path relative to `node.root`
        """
        full_path = pathlib.PurePath(self.node.root).joinpath(path)
        return self._lfs.hsm_state(full_path) != self._lfs.HSM_MISSING

    def filesize(self, path: pathlib.Path, actual: bool = False) -> int:
        """Return size in bytes of the file given by `path`.

        This _always_ returns the apprent size, since the size on tape is
        not known (or important), and the size on disk is usually that of the
        the file stub.

        Parameters
        ----------
        path: path-like
            The filepath to check the size of.  May be absolute or relative
            to `node.root`.
        actual: bool, optional
            Ignored.
        """
        path = pathlib.Path(path)
        if not path.is_absolute():
            path = pathlib.Path(self.node.root, path)
        return path.stat().st_size

    def fits(self, size_b: int) -> bool:
        """Does `size_b` bytes fit on this node?

        Returns True: everything fits in HSM.
        """
        return True

    def init(self) -> bool:
        """Initialise this node.

        This should never be called, because check_init always returns
        True, but in case it does, we implement it anyways.
        """
        # Initialisaiton is a no-op
        return True

    def open(self, path: os.PathLike | str, binary: bool = True) -> IO:
        """Open the file specified by `path` for reading.

        Parameters:
        -----------
        path : pathlike
            Relative to `node.root`
        binary : bool, optional
            If True, open the file in binary mode, otherwise open the file in
            text mode.

        Returns
        -------
        file : file-like
            An open, read-only file.

        Raises
        ------
        ValueError:
            `path` was absolute.
        OSError:
            The file is not restored.
        """
        if pathlib.PurePath(path).is_absolute():
            raise ValueError("path must be relative to node.root")

        # Make abs path
        p = pathlib.Path(self.node.root, path)

        state = self._lfs.hsm_state(p)
        if state is None:
            raise OSError(f"Can't get state for {path}.")
        if state != self._lfs.HSM_RESTORED and state != self._lfs.HSM_UNARCHIVED:
            raise OSError(f"{path} is not restored.")
        return open(p, mode="rb" if binary else "rt")

    def ready_path(self, path: os.PathLike) -> bool:
        """Recall the specified path so it can be read.

        This should only be used on paths not already managed by alpenhorn.

        Parameters
        ----------
        path : path-like
            The path that we want to perform I/O on.

        Returns
        -------
        ready : bool
            True if `path` is ready for I/O.  False otherwise.
        """
        fullpath = pathlib.Path(self.node.root, path)
        state = self._lfs.hsm_state(fullpath)

        # If it's restorable, restore it
        if state == self._lfs.HSM_RELEASED:
            self._lfs.hsm_restore(fullpath)

        # Returns True if file is readable.
        return state == self._lfs.HSM_RESTORED or state == self._lfs.HSM_UNARCHIVED

    def ready_pull(self, req: ArchiveFileCopyRequest) -> None:
        """Ready a file to be pulled as specified by `req`.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the copy request to ready.  We are the source node (i.e.
            `req.node_from == self.node`).
        """

        # A small async to restore-and-wait for the file
        def _async(task: Task, node_io: LustreHSMNodeIO, file_: ArchiveFile):
            """Restore `file_` on `node` and wait for completion."""

            try:
                copy = ArchiveFileCopy.get(node=node_io.node, file=file_)
            except pw.DoesNotExist:
                return

            restore_wait = node_io._restore_wait(copy)
            while restore_wait:
                # Wait for a bit
                yield self._restore_wait_time

                # Now check again
                restore_wait = node_io._restore_wait(copy)

            # Update copy based on result.  restore_wait==False means the
            # file was successfully restored.  On error, we assume the
            # file is not ready (i.e. not restored).
            ready = True if restore_wait is False else False

            if copy.ready != ready:
                copy.ready = ready
                copy.last_update = utcnow()
                copy.save()
                log.info(
                    f"File copy {file_.path} on node {node_io.node.name} now "
                    + ("restored" if ready else "released")
                )

        # No need to do this more than once
        if req.file.id not in self._restoring:
            Task(
                func=_async,
                queue=self._queue,
                key=self.fifo,
                args=(self, req.file),
                name=f"Ready file {req.file.path} on node {self.node.name}",
            )
        else:
            log.debug(
                f"Skipping ready of {req.file.path} "
                f"on node {self.node.name}: restore in progress."
            )

    def release_bytes(self, size: int) -> None:
        """Does nothing."""
        pass

    def reserve_bytes(self, size: int, check_only: bool = False) -> bool:
        """Returns True."""
        return True


class LustreHSMGroupIO(DefaultGroupIO):
    """LustreHSM Group I/O

    The LustreHSM Group contains two nodes:
     - a primary node pointing to the HSM storage itself.
     - a secondary node on a regular disk used for storing files too small for HSM.

    The primary node must have `io_class=="LustreHSM"`.  The secondary node must
    _not_ have `io_class=="LustreHSM`.

    Optional io_config keys:
     - threshold : integer
            The smallfile threshold, in bytes.  Files above this size are sent to
            HSM and put on tape.  The small files are sent to the secondary
            node.  Default is 1000000000 bytes (1 GB).
    """

    # SETUP

    def __init__(
        self, group: UpdateableGroup, config: dict, queue: FairMultiFIFOQueue
    ) -> None:
        super().__init__(group, config, queue)

        self._threshold = self.config.get("threshold", 1000000000)  # bytes

    # HOOKS

    def set_nodes(self, nodes: list[UpdateableNode]) -> list[UpdateableNode]:
        """Set nodes used during update.

        Identify primary and secondary nodes.  If that can't be accomplished,
        `ValueError` is raised.

        Parameters
        ----------
        nodes : list of UpdateableNodes
                The local active nodes in this group.  Will never be
                empty.
        idle : boolean
                True if all the `nodes` were idle when the current
                update loop started.

        Returns
        -------
        nodes : list of UpdateableNodes
                This is always just the `nodes` list passed-in.

        Raises
        ------
        ValueError
            At least one node couldn't be identified, or more than
            two nodes were provided.
        """
        if len(nodes) != 2:
            raise ValueError(
                "need exactly two nodes in StorageGroup "
                f"group {self.group.name} (have {len(nodes)})"
            )

        if nodes[0].db.io_class == "LustreHSM":
            # If both nodes are LustreHSM, we have a problem
            if nodes[1].db.io_class == "LustreHSM":
                raise ValueError(
                    f"Can't use two LustreHSM nodes in StorageGroup {self.group.name}"
                )

            self._hsm = nodes[0]
            self._smallfile = nodes[1]
        elif nodes[1].db.io_class == "LustreHSM":
            self._hsm = nodes[1]
            self._smallfile = nodes[0]
        else:
            raise ValueError(f"no LustreHSM node in StorageGroup {self.group.name}")

        return nodes

    # I/O METHODS

    def exists(self, path: pathlib.PurePath) -> UpdateableNode | None:
        """Check whether the file `path` exists in this group.

        Parameters
        ----------
        path : pathlib.PurePath
            the path, relative to a node `root` of the file to
            search for.

        Returns
        -------
        node : UpdateableNode or None
            If the file exists, the UpdateableNode on which it is.
            If the file doesn't exist in the group, this is None.
        """
        if self._smallfile.io.exists(path):
            return self._smallfile
        if self._hsm.io.exists(path):
            return self._hsm
        return None

    def pull_force(self, req: ArchiveFileCopyRequest) -> None:
        """Handle ArchiveFileCopyRequest `req` by pulling to this group.

        This takes care of directing pulls to the correct node
        based on the threshold.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the request to fulfill.  We are the destination group (i.e.
            `req.group_to == self.group`).
        """
        if req.file.size_b <= self._threshold:
            self._smallfile.io.pull(req)
        else:
            self._hsm.io.pull(req)
