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

from .. import util
from .base import BaseNodeIO, BaseGroupIO
from ..task import Task

# The asyncs are over here:
from ._default_asyncs import check_async, delete_async

if TYPE_CHECKING:
    from ..archive import ArchiveFileCopy
    from ..update import UpdateableNode

log = logging.getLogger(__name__)


class DefaultNodeIO(BaseNodeIO):
    """A simple StorageNode backed by a regular POSIX filesystem."""

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
            args=(copies,),
            name="Delete copies "
            + str([copy.id for copy in copies])
            + f" from {self.node.name}",
        )

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
