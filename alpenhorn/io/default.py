"""Alpenhorn Default I/O classes.

The Alpenhorn Default I/O classes largely re-create the legacy I/O behaviour
of previous versions of Alpenhorn.

These I/O classes are used by StorageNodes and StorageGroups which do not
explicitly specify `io_class` (as well as being used explicitly when `io_class`
has the value "Default").
"""

from __future__ import annotations

import logging
import os
import pathlib
import threading
from collections.abc import Hashable, Iterable
from typing import IO

from watchdog.observers import Observer

from ..common import util
from ..daemon import UpdateableNode
from ..db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageNode,
)
from ..scheduler import FairMultiFIFOQueue, Task
from . import ioutil
from ._default_asyncs import check_async, delete_async, group_search_async, pull_async
from .base import BaseGroupIO, BaseNodeIO, BaseNodeRemote
from .updownlock import UpDownLock

log = logging.getLogger(__name__)

# Reserved byte counts are stored here, indexed by node name and protected
# by the mutex
_mutex = threading.Lock()
_reserved_bytes = {}

# This sets how often we run the clean-up idle task.  What
# we're counting here is number of not-idle -> idle transitions
_IDLE_CLEANUP_PERIOD = 400  # (i.e. once every 400 opportunities)


class DefaultNodeRemote(BaseNodeRemote):
    """I/O class for a remote DefaultIO StorageNode."""

    def pull_ready(self, file: ArchiveFile) -> bool:
        """Return True.

        Files on a Default I/O node are always ready.

        Parameters
        ----------
        file : ArchiveFile
            Unused.

        Returns
        -------
        bool
            ``True``.
        """
        return True


class DefaultNodeIO(BaseNodeIO):
    """Default Node I/O.

    This represents a simple StorageNode backed by a regular POSIX filesystem.
    If you have a StorageNode on a "regular" locally-connected disk, this is
    probably the Node I/O class to use.

    Parameters
    ----------
    node : StorageNode
        The node we're performing I/O on.
    config : dict
        The I/O config.
    queue : FairMultiFIFIOQueue
        The task scheduler.
    fifo : Hashable
        The queue FIFO key to use.
    """

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
                    ioutil.remove_filedir(
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

    def bytes_avail(self, fast: bool = False) -> int:
        """Calculate the amount of free space of the node.

        Does not account for space reserved via reserve_bytes().

        Parameters
        ----------
        fast : bool
            Unused: this method always returns the available size.

        Returns
        -------
        int
            The total bytes available on this nodes' filesystem.
        """
        x = os.statvfs(self.node.root)
        return x.f_bavail * x.f_bsize

    def check(self, copy: ArchiveFileCopy) -> None:
        """Check whether ArchiveFileCopy `copy` is corrupt.

        Does nothing other than queue a `check_async` task.

        Parameters
        ----------
        copy : ArchiveFileCopy
            The file copy to check.
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

        Checks that the file ``<node.root>/ALPENHORN_NODE`` exists and
        contains the name of the node.

        Returns
        -------
        bool
            ``True`` if the `ALPENHORN_NODE` check succeeded;
            ``False`` otherwise.
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
        """Delete some files.

        Queues a single asynchronous I/O task to delete the
        list of file copies given.

        Parameters
        ----------
        copies : list of ArchiveFileCopy
            The list of file copies to delete.
        """

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
        """Check if `path` exists.

        The check is really "exists and is a file".

        Parameters
        ----------
        path : pathlib.PurePath
            The path to check, relative to `node.root`.

        Returns
        -------
        bool
            ``True`` if the path was found.  ``False`` otherwise.
        """
        return pathlib.Path(self.node.root, path).is_file()

    def filesize(self, path: pathlib.Path, actual: bool = False) -> int:
        """Return size in bytes of the file given by `path`.

        Parameters
        ----------
        path : path-like
            The filepath to check the size of.  May be absolute or relative
            to `node.root`.
        actual : bool, optional
            If ``True``, return the amount of space the file actually takes
            up on the storage system.  Otherwise return apparent size.

        Returns
        -------
        int
            The absolute or apparent size, in bytes, of the file.
        """
        path = pathlib.Path(path)
        if not path.is_absolute():
            path = pathlib.Path(self.node.root, path)

        if actual:
            # Per POSIX, blocksize for st_blocks is always 512 bytes
            return path.stat().st_blocks * 512

        # Apparent size
        return path.stat().st_size

    def file_walk(self, path: str | os.PathLike) -> Iterable[pathlib.PurePath]:
        """An iterator over all regular files under `path`.

        Parameters
        ----------
        path : path-like
            The path relative the the node root to walk.

        Returns
        -------
        Iterable
            An iterable containg absolute `pathlib.PurePath` elements
            for all files in the specified path.
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
        """Check for at least `size_b` bytes of free space.

        Takes into account reserved space.

        Parameters
        ----------
        size_b : int
            The amount of data we're trying to fit.

        Returns
        -------
        bool
            ``True`` if `size_b` bytes fits on the node.  ``False`` otherwise.
        """
        return self.reserve_bytes(size_b, check_only=True)

    def init(self) -> bool:
        """Initialise this node.

        We do that by creating the ``ALPENHORN_NODE`` file, if it
        doesn't already exit.  If this node is already initialised,
        this function does nothing and returns ``True``.

        Returns
        -------
        bool
            ``False`` if node initialisation failed; ``True`` otherwise.
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
        """Check if file `path` is locked.

        A file at the path ``dir/subdir/name.ext`` is locked if a file
        named ``dir/subdir/.name.ext.lock`` exists.

        Parameters
        ----------
        path : path-like
            The path to check.  May be relative or absolute.

        Returns
        -------
        bool
            ``True`` if `path` is locked; ``False`` otherwise.
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
            Path (or first part of the, path if other `segments` provided) to
            the file to hash.  Relative to `node.root`.
        *segments : iterable, optional
            Other path segments path-concatenated and appended to `path`.

        Returns
        -------
        str or None
            The base64-encoded MD5 hash value, or ``None`` on error.
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

        Parameters
        ----------
        path : pathlike
            Relative to `node.root`.
        binary : bool, optional
            If ``True`` (the default), open the file in binary mode; otherwise,
            open the file in text mode.

        Returns
        -------
        IO
            An open, read-only file.

        Raises
        ------
        ValueError
            `path` was absolute
        """

        if pathlib.PurePath(path).is_absolute():
            raise ValueError("path must be relative to node.root")
        return open(pathlib.Path(self.node.root, path), mode="rb" if binary else "rt")

    def pull(self, req: ArchiveFileCopyRequest, did_search: bool) -> None:
        """Pull file specified by copy request `req` onto this node.

        Most of the work happens in an asynchronous I/O task.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            The copy request to fulfill.  We are the destination node (i.e.
            `req.group_to == self.node.group`).
        did_search : bool
            ``True`` if a group-level pre-pull search for an existing file was
            performed.  ``False`` otherwise.
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
        size : int
            The number of bytes to release.

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
            The number of bytes to reserve.
        check_only : bool, optional
            If ``True``, no reservation is made, and the only effect is
            the return value.

        Returns
        -------
        bool
            ``False`` if there was insufficient space to make the reservation.
            ``True`` otherwise.
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
        """Do nothing.

        This method does nothing: DefaultIO file copies are always ready.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            Unused.
        """
        pass


class DefaultGroupIO(BaseGroupIO):
    """Default Group I/O.

    The Default I/O group may only have a single active Storage node.

    Parameters
    ----------
    group : StorageGroup
        The group we're performing I/O on.
    config : dict
        The I/O config.
    queue : FairMultiFIFIOQueue
        The task scheduler.
    fifo : Hashable
        The queue FIFO key to use.
    """

    # Because Default groups allow only a single node, they don't need to do
    # group-level pull searches.  So we set this to False.
    #
    # Note, however, that the DefaultGroupIO still fully implements a "normal"
    # pre-pull search to make it easier for subclasses that use this as a parent
    # to enable the group search by simply setting this back to True.
    do_pull_search = False

    # SETUP

    def __init__(
        self,
        group: StorageGroup,
        config: dict,
        queue: FairMultiFIFOQueue,
        fifo: Hashable,
    ) -> None:
        super().__init__(group, config, queue, fifo)
        self._node = None

    @property
    def nodes(self) -> list[UpdateableNode]:  # numpydoc ignore=RT01
        """The list of nodes in this group.

        This is a single element list containing the node assigned to this I/O
        instance, or the empty list if no node has been assigned.
        """
        if self._node:
            return [self._node]
        return []

    @nodes.setter
    def nodes(self, nodes: list[UpdateableNode]) -> None:
        """Set the node in this group.

        DefaultGroupIO only accepts a single node to operate on.

        Parameters
        ----------
        nodes : list of UpdateableNodes
            This will always be a single-element list containing the
            group's `StorageNode`.

        Raises
        ------
        ValueError
            Whenever `len(nodes) != 1`
        """

        if len(nodes) != 1:
            # The nodes list passed in is never empty, so this message is reasonable.
            raise ValueError(f"Too many active nodes in group {self.group.name}.")

        self._node = nodes[0]

    # I/O METHODS

    def exists(self, path: pathlib.PurePath) -> UpdateableNode | None:
        """Check whether the file `path` exists in this group.

        Parameters
        ----------
        path : pathlib.PurePath
            The path, relative to node `root` of the file to
            search for.

        Returns
        -------
        UpdateableNode or None
            If the file exists, returns the node in the group.
            If the file doesn't exist in the group, this is ``None``.
        """
        if self._node.io.exists(path):
            return self._node

        return None

    def pull(self, req: ArchiveFileCopyRequest, did_search: bool) -> None:
        """Handle ArchiveFileCopyRequest `req` by pulling to this group.

        Simply passes the `req` on to the node in the group.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            The request to fulfill.  We are the destination group (i.e.
            ``req.group_to == self.group``).
        did_search : bool
            ``True`` if a group-level pre-pull search for an existing file was
            performed.  ``False`` otherwise.
        """
        self._node.io.pull(req, did_search)

    def pull_search(self, req: ArchiveFileCopyRequest) -> None:
        """Search for an existing copy of a file in a group.

        Before the pull is dispached to the group, we first check
        whether an existing unregistered file exists in the group.

        If there is, the file is schedule for check and the request
        is skipped.  Otherwise, `pull` will be called to actually
        pull the file.

        .. hint::
            The DefaultGroupIO class itself sets `do_pull_search` to False
            because it's not needed by the DefaultIO, but this method is
            implemented to dispatch the search task anyways so that other
            I/O classes which derive from DefaultIO can set `do_pull_search`
            back to True and not have to re-implement this method themselves.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            The request to fulfill.  We are the destination group (i.e.
            ``req.group_to == self.group``).
        """

        # The existing file search needs to happen in a Task.
        Task(
            func=group_search_async,
            queue=self._queue,
            key=self.fifo,
            args=(self, req),
            name=f"Pre-pull search for {req.file.path} in {self.group.name}",
        )
