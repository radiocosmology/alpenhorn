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
from typing import TYPE_CHECKING, IO

import logging
import pathlib
import peewee as pw

from ..archive import ArchiveFileCopy
from ..task import Task
from ..util import pretty_bytes
from .base import BaseGroupIO, BaseNodeRemote
from .lustrequota import LustreQuotaNodeIO

if TYPE_CHECKING:
    import os
    from ..acquisition import ArchiveFile
    from ..archive import ArchiveFileCopyRequest
    from ..queue import FairMultiFIFOQueue
    from ..storage import StorageNode, StorageGroup
    from ..update import UpdateableNode

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
        * fixed_quota : integer
            the quota, in kiB, on the Lustre disk backing the HSM system
    """

    remote_class = LustreHSMNodeRemote

    def __init__(
        self, node: StorageNode, config: dict, queue: FairMultiFIFOQueue
    ) -> None:
        super().__init__(node, config, queue)

        self._headroom = config["headroom"] * 2**10  # convert from kiB

    def release_files(self) -> None:
        """Release files from the HSM disk to keep the headroom clear.

        This is called at the start of a node update (from
        `before_update`), but only if the update is going
        to happen (i.e. the node is currently idle).
        """

        headroom_needed = self._headroom - self._lfs.quota_remaining(self.node.root)

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
                    ArchiveFileCopy.ready == True,
                )
                .order_by(ArchiveFileCopy.last_update)
            ):
                # Skip unarchived files
                if not lfs.hsm_archived(copy.path):
                    continue

                log.debug(
                    f"releasing file copy {copy.path} "
                    f"[={pretty_bytes(copy.file.size_b)}] on node {self.node.name}"
                )
                lfs.hsm_release(copy.path)
                # Update copy record immediately
                ArchiveFileCopy.update(ready=False).where(
                    ArchiveFileCopy.id == copy.id
                ).execute()
                total_files += 1
                total_bytes += copy.file.size_b
                if total_bytes >= headroom_needed:
                    break
            log.info(
                f"released {pretty_bytes(total_bytes)} in "
                f"{total_files} files on node {self.node.name}"
            )
            return

        # Do the rest asynchronously
        Task(
            func=_async,
            queue=self._queue,
            key=self.node.name,
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

    # I/O METHODS

    def check(self, copy: ArchiveFileCopy) -> None:
        """Check whether ArchiveFileCopy `copy` is corrupt.

        If the file is not restored, this function calls lfs(1) to
        start restoration and then returns without doing anything else
        (assuming a subsequent call to this function is going to resolve
        the issue).

        Parameters
        ----------
        copy : ArchiveFileCopy
            the file copy to check
        """
        # If the file is released, restore it and do nothing
        # further (assuming the update loop will re-call this
        # method next time through the loop).
        if self._lfs.hsm_released(copy.path):
            self._lfs.hsm_restore(copy.path)
            return

        # Otherwise, use the DefaultIO method to do the check
        super().check(copy)

    def check_active(self) -> bool:
        """Check that this is an active node.

        There's no ALPENHORN_NODE file on HSM,
        so this just returns `self.node.active`.
        """
        return self.node.active

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
            The file is not recalled.
        """
        if pathlib.PurePath(path).is_absolute():
            raise ValueError("path must be relative to node.root")

        # Make abs path
        p = pathlib.Path(self.node.root, path)

        if self._lfs.hsm_released(p):
            raise OSError(f"{path} is not restored.")
        return open(p, mode="rb" if binary else "rt")

    def ready_path(self, path: os.PathLike) -> bool:
        """Recall the specified path so it can be read.

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
        ready = self.ready_path(req.file.path)

        # Update DB based on HSM state
        ArchiveFileCopy.update(ready=ready).where(
            ArchiveFileCopy.file == req.file, ArchiveFileCopy.node == self.node
        ).execute()

    def release_bytes(self, size: int) -> None:
        """Does nothing."""
        pass

    def reserve_bytes(self, size: int, check_only: bool = False) -> bool:
        """Returns True."""
        return True


class LustreHSMGroupIO(BaseGroupIO):
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

    def __init__(self, group: StorageGroup, config: dict) -> None:
        super().__init__(group, config)

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
                f"need exactly two nodes in StorageGroup group {self.group.name} (have {len(nodes)})"
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

    def pull(self, req: ArchiveFileCopyRequest) -> None:
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
