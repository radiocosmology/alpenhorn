"""Alpenhorn Default Node I/O class.

This is the DefaultNodeIO class which implements the I/O framework for
a StorageNode backed by a normal POSIX filesystem.
"""

from __future__ import annotations

import logging
import os
import pathlib
import threading
from collections.abc import Hashable, Iterable
from typing import IO

from watchdog.observers import Observer

from ...common import util
from ...daemon.scheduler import FairMultiFIFOQueue, Task
from ...db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageNode,
)
from ..base import BaseNodeIO
from .check import check_async
from .delete import delete_async, remove_filedir
from .pull import pull_async
from .remote import DefaultNodeRemote
from .updownlock import UpDownLock

log = logging.getLogger(__name__)

# Reserved byte counts are stored here, indexed by node name and protected
# by the mutex
_mutex = threading.Lock()
_reserved_bytes = {}

# This sets how often we run the clean-up idle task.  What
# we're counting here is number of not-idle -> idle transitions
_IDLE_CLEANUP_PERIOD = 400  # (i.e. once every 400 opportunities)


class DefaultNodeIO(BaseNodeIO):
    """A simple StorageNode backed by a regular POSIX filesystem."""

    # SETUP

    remote_class = DefaultNodeRemote

    # Uses the platform-default observer.  On Linux, this will be the InotifyObserver
    # which doesn't work on NFS mounts (for which use alpenhorn.io.polling instead).
    observer = Observer

    def __init__(
        self, node: StorageNode, config: dict, queue: FairMultiFIFOQueue, fifo: Hashable
    ) -> None:
        super().__init__(node, config, queue, fifo)

        # The directory tree modification lock
        self.tree_lock = UpDownLock()

        # When <= 1, the idle clean-up is allowed to run; we initialise
        # to zero to run it as soon as possible after start-up
        self._skip_idle_cleanup = 0

        # Set up a reservation for ourself if necessary
        with _mutex:
            _reserved_bytes.setdefault(node.name, 0)

    # HOOKS

    def idle_update(self, newly_idle: bool) -> None:
        """Idle update hook.

        Called after a regular update that wasn't skipped, but only if,
        after the regular update, there were no tasks pending or in
        progress for this node (i.e. `self.idle` is True).

        This will try to do some tidying-up: look for stale placeholders,
        and attempt to delete empty acqdirs, whenever newly_idle is True.

        Parameters
        ----------
        newly_idle : bool
            True if this is the first time idle_update has been called since
            some I/O happened.
        """

        # Task to do some cleanup
        def _async(task, node, tree_lock):
            # Loop over all acqs
            for acq in ArchiveAcq.select():
                # Only continue if the directory for this acquisition exists
                acqpath = pathlib.Path(node.root, acq.name)
                if acqpath.is_dir():
                    # Look for placeholders
                    for file_ in ArchiveFile.select().where(ArchiveFile.acq == acq):
                        placeholder = acqpath.joinpath(f".{file_.name}.placeholder")
                        if placeholder.exists():
                            log.warning(f"Removing stale placeholder {placeholder!s}")
                            placeholder.unlink()
                    # Attempt to remove acqpath.  If it isn't empty, this does nothing
                    remove_filedir(
                        node,
                        pathlib.Path(node.root, acq.name),
                        tree_lock,
                    )

        # Submit the task only after doing some I/O
        if newly_idle:
            if self._skip_idle_cleanup <= 1:
                self._skip_idle_cleanup = _IDLE_CLEANUP_PERIOD

                # NB: this is an exclusive task: it will remain
                # queued until no other I/O is happening on this
                # node, and then prevent other I/O from happening
                # on the node while it's running.
                Task(
                    func=_async,
                    queue=self._queue,
                    exclusive=True,
                    key=self.fifo,
                    args=(self.node, self.tree_lock),
                    name=f"Tidy up {self.node.name}",
                )
            else:
                self._skip_idle_cleanup = self._skip_idle_cleanup - 1

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
            key=self.fifo,
            args=(self, copy),
            name=f"Check file {copy.file.path} on {self.node.name}",
        )

    def check_init(self) -> bool:
        """Check that this node is initialised.

        Checks that the file `node.root`/ALPENHORN_NODE exists and
        contains the name of the node.

        Returns
        -------
        init : bool
            True if the `ALPENHORN_NODE` check succeeded;
            False otherwise.
        """

        try:
            with self.open("ALPENHORN_NODE", binary=False) as f:
                first_line = f.readline()
                # Check if the actual node name is in the textfile
                if self.node.name == first_line.rstrip():
                    # Great! Everything is as expected.
                    return True
                log.warning(
                    f'Node name in file "{self.node.root}/ALPENHORN_NODE" does not '
                    f"match expected: {self.node.name}."
                )
        except OSError:
            log.warning(
                f'Node file "{self.node.root}/ALPENHORN_NODE" could not be read.'
            )

        return False

    def delete(self, copies: list[ArchiveFileCopy]) -> None:
        """Queue a single asynchronous I/O task to delete the list of file copies."""

        # Nothing to do
        if len(copies) == 0:
            return

        Task(
            func=delete_async,
            queue=self._queue,
            key=self.fifo,
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

    def filesize(self, path: pathlib.Path) -> int:
        """Return size in bytes of the file given by `path`.

        Parameters
        ----------
        path: path-like
            The filepath to check the size of.  May be absolute or relative
            to `node.root`.

        Returns
        -------
        int
            The size, in bytes, of the file
        """
        path = pathlib.Path(path)
        if not path.is_absolute():
            path = pathlib.Path(self.node.root, path)
        return path.stat().st_size

    def storage_used(self, path: pathlib.Path) -> int:
        """Return amount of storage space used by the file given by `path`.

        This is just the number of filesystem blocks used by the file multiplied
        by the block size.

        Parameters
        ----------
        path: path-like
            The filepath to check the size of.  May be absolute or relative
            to `node.root`.

        Returns
        -------
        int
            The amount of space, in bytes, taken up by the file.
        """
        path = pathlib.Path(path)
        if not path.is_absolute():
            path = pathlib.Path(self.node.root, path)

        # Per POSIX, blocksize for st_blocks is always 512 bytes
        return path.stat().st_blocks * 512

    def file_walk(self, path) -> Iterable[pathlib.PurePath]:
        """An iterator over all regular files under `node.root/path`

        pathlib.PurePaths returned by the iterator are absolute
        """

        # path must not be absolute
        if path.is_absolute():
            raise ValueError("path may not be absolute")

        def _walk(path):
            """Recurse through directory path, yielding files"""

            # We use os.scandir instead of os.walk because it gives
            # us DirEntry objects which have useful metadata in them
            for entry in os.scandir(path):
                if entry.is_dir():
                    # Recurse
                    yield from _walk(entry)
                # is_file() on a symlink to a file returns true, so we need both
                elif entry.is_file() and not entry.is_symlink():
                    yield pathlib.PurePath(entry)

        fullpath = pathlib.Path(self.node.root).joinpath(path)

        if not fullpath.exists():
            # If path doesn't exist, just return an empty tuple
            return ()

        if fullpath.is_file() and not fullpath.is_symlink():
            # If path is just a file, just return that
            return (fullpath,)

        # Return an iterator over the directory contents if a directory
        if fullpath.is_dir():
            return _walk(fullpath)

        # Return nothing, if something weird
        return ()

    def fits(self, size_b: int) -> bool:
        """Does `size_b` bytes fit on this node?

        Takes into account reserved space.

        Parameters
        ----------
        size_b : int
            The size of the file we're trying to fit

        Returns
        -------
        fits : bool
            True if `size_b` fits on the node.  False otherwise.
        """
        return self.reserve_bytes(size_b, check_only=True)

    def init(self) -> bool:
        """Initialise this node.

        We do that by creating the ALPENHORN_NODE file, if it
        doesn't already exit.
        """

        # Sanity check
        if self.check_init():
            return True

        # Create file
        try:
            with open(
                pathlib.Path(self.node.root).joinpath("ALPENHORN_NODE"), mode="w"
            ) as f:
                f.write(self.node.name + "\n")
        except OSError as e:
            log.warning(f"Node initialistion failed: {e}")
            return False

        return True

    def locked(self, path: os.PathLike) -> bool:
        """Is file `path` locked?

        A file with path `dir/subdir/name.ext` is locked if a file named
        `dir/subdir/.name.ext.lock` exists.

        Parameters
        ----------
        path : path-like
            The path to check.  May be relative or absolute.

        Returns
        -------
        locked : bool
            True if `path` is locked; False otherwise.
        """

        # Ensure we have an absolute pathlib.Path
        path = pathlib.Path(path)
        if not path.is_absolute():
            path = pathlib.Path(self.node.root, path)

        return path.with_name("." + path.name + ".lock").exists()

    def md5(self, path: str | pathlib.Path, *segments) -> str | None:
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
        md5sum : str | None
            the base64-encoded MD5 hash value or None on error
        """
        path = pathlib.Path(self.node.root, path, *segments)
        try:
            return util.md5sum_file(path)
        except FileNotFoundError:
            log.warning(f"MD5 sum check for {path} failed: file not found.")
        except PermissionError:
            log.warning(f"MD5 sum check for {path} failed: permission error.")
        return None

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
        -------
        ValueError
            `path` was absolute
        """

        if pathlib.PurePath(path).is_absolute():
            raise ValueError("path must be relative to node.root")
        return open(pathlib.Path(self.node.root, path), mode="rb" if binary else "rt")

    def pull(self, req: ArchiveFileCopyRequest, did_search: bool) -> None:
        """Pull file specified by copy request `req` onto `self.node`.

        Most of the work happens in an asynchronous I/O task.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the copy request to fulfill.  We are the destination node (i.e.
            `req.group_to == self.node.group`).
        did_search : boolean
            True if a group-level pre-pull search for an existing file was
            performed.  False otherwise.
        """

        # Run early DB checks.  The group has already run checks on the source node.
        if not self.node.check_pull_dest():
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
            key=self.fifo,
            args=(self, self.tree_lock, req, did_search),
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
                    "attempted to release too many bytes: "
                    f"{_reserved_bytes[self.node.name]} < {size}"
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
            bavail = self.bytes_avail(fast=True)
            if bavail is not None and bavail - _reserved_bytes[self.node.name] < size:
                return False  # Insufficient space

            if not check_only:
                _reserved_bytes[self.node.name] += size

            return True

    def ready_path(self, path):
        """Ready a file at `path` for I/O.

        Parameters
        ----------
        path : path-like
            The path that we want to perform I/O on.

        Returns
        -------
        True
            DefaultIO files are always ready for I/O.
        """
        return True

    def ready_pull(self, req: ArchiveFileCopyRequest) -> None:
        """Ready a file to be pulled as specified by `req`.

        This method does nothing: DefaultIO file copies are
        always ready.
        """
        pass
