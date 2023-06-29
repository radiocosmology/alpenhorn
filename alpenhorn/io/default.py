"""Alpenhorn Default I/O classes.

The Alpenhorn Default I/O classes largely re-create the legacy I/O behaviour
of previous versions of Alpenhorn.

These I/O classes are used by StorageNodes and StorageGroups which do not
explicitly specify `io_class` (as well as being used explicitly when `io_class`
has the value "Default").
"""
from __future__ import annotations
from typing import TYPE_CHECKING

import os
import logging
import pathlib
import threading

from .base import BaseNodeIO, BaseGroupIO, BaseNodeRemote
from .updownlock import UpDownLock
from .. import util
from ..task import Task

# The asyncs are over here:
from ._default_asyncs import pull_async, check_async, delete_async

if TYPE_CHECKING:
    from ..acquisition import ArchiveFile
    from ..archive import ArchiveFileCopy, ArchiveFileCopyRequest
    from ..queue import FairMultiFIFOQueue
    from ..storage import StorageNode
    from ..update import UpdateableNode

log = logging.getLogger(__name__)

# Reserved byte counts are stored here, indexed by node name and protected
# by the mutex
_mutex = threading.Lock()
_reserved_bytes = dict()


class DefaultNodeRemote(BaseNodeRemote):
    """I/O class for a remote DefaultIO StorageNode."""

    def pull_ready(self, file: ArchiveFile) -> bool:
        """Is `file` ready for pulling from this remote node?

        Parameters
        ----------
        file : ArchiveFile
            the file being checked

        Returns
        -------
        True
            Files on Default nodes are always ready.
        """
        return True


class DefaultNodeIO(BaseNodeIO):
    """A simple StorageNode backed by a regular POSIX filesystem."""

    # SETUP

    remote_class = DefaultNodeRemote

    def __init__(
        self, node: StorageNode, queue: FairMultiFIFOQueue, config: dict
    ) -> None:
        super().__init__(node, queue, config)

        # The directory tree modification lock
        self.tree_lock = UpDownLock()

        # Set up a reservation for ourself if necessary
        with _mutex:
            _reserved_bytes.setdefault(node.name, 0)

    # I/O METHODS

    def bytes_avail(self, fast: bool = False) -> int | None:
        """bytes_avail: Return amount of free space (in bytes) of the node, or
        None if that cannot be determined.

        Does not account for space reserved via reserve_bytes().

        Parameters
        ----------
        fast : bool
            If True, then this is a fast call, and I/O classes for which
            checking available space is expensive may skip it by returning None.

        Returns
        -------
        bytes_avail : int or None
            the total bytes available on the storage system, or None if that
            can't be or wasn't determined.
        """
        x = os.statvfs(self.node.root)
        return x.f_bavail * x.f_bsize

    def check(self, copy: ArchiveFileCopy) -> None:
        """Check whether ArchiveFileCopy `copy` is corrupt.

        Does nothing other than queue a `check_async` task.

        Parameters
        ----------
        copy : ArchiveFileCopy
            the file copy to check
        """

        Task(
            func=check_async,
            queue=self._queue,
            key=self.node.name,
            args=(self, copy),
            name=f"Check copy#{copy.id} on {self.node.name}",
        )

    def check_active(self) -> bool:
        """Check that this is an active node.

        Checks that the file `node.root`/ALPENHORN_NODE exists and
        contains the name of the node.

        Returns
        -------
        active : bool
            True if the `ALPENHORN_NODE` check succeeded;
            False otherwise.
        """

        file_path = pathlib.Path(self.node.root, "ALPENHORN_NODE")
        try:
            with open(file_path, "r") as f:
                first_line = f.readline()
                # Check if the actual node name is in the textfile
                if self.node.name == first_line.rstrip():
                    # Great! Everything is as expected.
                    return True
                log.warning(
                    f"Node name in file {file_path} does not match expected: "
                    f"{self.node.name}."
                )
        except IOError:
            log.warning(f"Node file {file_path} could not be read.")

        return False

    def delete(self, copies: list[ArchiveFileCopy]) -> None:
        """Queue a single asynchronous I/O task to delete the list of file copies."""

        # Nothing to do
        if len(copies) == 0:
            return

        Task(
            func=delete_async,
            queue=self._queue,
            key=self.node.name,
            args=(self.tree_lock, copies),
            name="Delete copies "
            + str([copy.id for copy in copies])
            + f" from {self.node.name}",
        )

    def exists(self, path: pathlib.PurePath) -> bool:
        """Does `path` exist?

        Parameters
        ----------
        path : pathlib.PurePath
            path relative to `node.root`
        """
        return pathlib.Path(self.node.root, path).is_file()

    def filesize(self, path: pathlib.Path, actual: bool = False) -> int:
        """Return size in bytes of the file given by `path`.

        Parameters
        ----------
        path: path-like
            The filepath to check the size of.  May be absolute or relative
            to `node.root`.
        actual: bool, optional
            If True, return the amount of space the file actually takes
            up on the storage system.  Otherwise return apparent size.
        """
        path = pathlib.Path(path)
        if not path.is_absolute():
            path = pathlib.Path(self.node.root, path)

        if actual:
            # Per POSIX, blocksize for st_blocks is always 512 bytes
            return path.stat().st_blocks * 512

        # Apparent size
        return path.stat().st_size

    def md5(self, path: str | pathlib.Path, *segments) -> str:
        """Compute the MD5 hash of the file at the specified path.

        This can take a long time: call it from an async.

        Parameters
        ----------
        path : PathLike
            path (or first part of the, path if other `segments` provided) to
            the file to hash.  Relative to `node.root`.
        *segments : iterable, optional
            other path segments path-concatenated and appended to `path`.

        Returns
        -------
        md5sum : str
            the base64-encoded MD5 hash value
        """
        return util.md5sum_file(pathlib.Path(self.node.root, path, *segments))

    def pull(self, req: ArchiveFileCopyRequest) -> None:
        """Pull file specified by copy request `req` onto `self.node`.

        Most of the work happens in an asynchronous I/O task.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the copy request to fulfill.  We are the destination node (i.e.
            `req.group_to == self.node.group`).
        """

        if self.node.under_min:
            log.info(
                f"Skipping pull for StorageNode {self.node.name}: hit minimum free space: "
                f"({self.node.avail_gb:.2f} GiB < {self.node.min_avail_gb:.2f} GiB)"
            )
            return

        if self.node.check_over_max():
            log.info(
                f"Skipping pull for StorageNode {self.node.name}: node full. "
                f"({self.node.get_total_gb():.2f} GiB >= {self.node.max_total_gb:.2f} GiB)"
            )
            return

        # Check that there is enough space available (and reserve what we need)
        if not self.reserve_bytes(req.file.size_b):
            log.warning(
                f"Skipping request for {req.file.acq.name}/{req.file.name}: "
                f"insufficient space on node {self.node.name}."
            )
            return

        Task(
            func=pull_async,
            queue=self._queue,
            key=self.node.name,
            args=(self, self.tree_lock, req),
            name=f"AFCR#{req.id}: {req.node_from.name} -> {self.node.name}",
        )

    # This is the reservation fudge factor.  XXX Is it correct?
    reserve_factor = 2

    def release_bytes(self, size: int) -> None:
        """Release space previously reserved with `reserve_bytes`.

        Parameters
        ----------
        size : integer
            the number of bytes to release

        Raises
        ------
        ValueError
            `size` was greater than the total amount of reserved
            space.
        """
        size *= self.reserve_factor
        with _mutex:
            if _reserved_bytes[self.node.name] < size:
                raise ValueError(
                    f"attempted to release too many bytes: {_reserved_bytes[self.node.name]} < {size}"
                )
            _reserved_bytes[self.node.name] -= size

    def reserve_bytes(self, size: int, check_only: bool = False) -> bool:
        """Attempt to reserve `size` bytes of space on the filesystem.

        Parameters
        ----------
        size : int
            the number of bytes to reserve
        check_only : bool, optional
            If True, no reservation is made, and the only effect is
            the return value.

        Returns
        -------
        success : bool
            False if there was insufficient space to make the reservation.
            True otherwise.
        """
        size *= self.reserve_factor
        with _mutex:
            bavail = self.bytes_avail()
            if bavail is not None and bavail - _reserved_bytes[self.node.name] < size:
                return False  # Insufficient space

            if not check_only:
                _reserved_bytes[self.node.name] += size

            return True

    def ready_pull(self, req: ArchiveFileCopyRequest) -> None:
        """Ready a file to be pulled as specified by `req`.

        This method does nothing: DefaultIO file copies are
        always ready.
        """
        pass


class DefaultGroupIO(BaseGroupIO):
    """A simple StorageGroup.

    Permits any number of StorageNodes in the group, but only permits at most
    one to be active on a given host at any time.
    """

    # SETUP

    def set_nodes(self, nodes: list[UpdateableNode]) -> list[UpdateableNode]:
        """Set the list of nodes to operate on.

        DefaultGroupIO only accepts a single node to operate on.

        Parameters
        ----------
        nodes : list of UpdateableNodes
            The local active nodes in this group.  Will never be
            empty.

        Returns
        -------
        nodes : list of UpdateableNodes
            `nodes`, if `len(nodes) == 1`

        Raises
        ------
        ValueError
            whenever `len(nodes) != 1`
        """

        if len(nodes) != 1:
            raise ValueError(f"Too many active nodes in group {self.group.name}.")

        self.node = nodes[0]
        return nodes

    # I/O METHODS

    def exists(self, path: pathlib.PurePath) -> UpdateableNode | None:
        """Check whether the file `path` exists in this group.


        Parameters
        ----------
        path : pathlib.PurePath
            the path, relative to node `root` of the file to
            search for.

        Returns
        -------
        node : UpdateableNode or None
            If the file exists, returns the node in the group.
            If the file doesn't exist in the group, this is None.
        """
        if self.node.io.exists(path):
            return self.node

        return None

    def pull(self, req: ArchiveFileCopyRequest) -> None:
        """Handle ArchiveFileCopyRequest `req` by pulling to this group.

        Simply passes the `req` on to the node in the group.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the request to fulfill.  We are the destination group (i.e.
            `req.group_to == self.group`).
        """
        self.node.io.pull(req)
