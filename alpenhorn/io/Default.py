"""Alpenhorn Default I/O classes.

The Alpenhorn Default I/O classes largely re-create the legacy I/O behaviour
of previous versions of Alpenhorn.  These I/O classes are used with StorageNodes
and StorageGroups which do not explicitly specify io_class.
"""


import os
import logging
import pathlib
import threading
from pathlib import PurePath
from watchdog.observers import Observer

from .. import util
from .base import BaseNodeIO, BaseGroupIO, BaseNodeRemote
from ..task import Task

# The asyncs are over here:
from ._default_asyncs import pull_async, check_async, delete_async

log = logging.getLogger(__name__)


# Reserved byte counts are stored here, indexed by node name and protected
# by the mutex
_mutex = threading.Lock()
_reserved_bytes = dict()


class DefaultNodeRemote(BaseNodeRemote):
    """DefaultNodeRemote: information about a DefaultIO remote StorageNode."""

    def pull_ready(self, file):
        """Returns True: DefaultIO file copies are always ready."""
        return True


class DefaultNodeIO(BaseNodeIO):
    """DefaultNodeIO implements a simple StorageNode backed by a regular POSIX filesystem."""

    remote_class = DefaultNodeRemote

    # Uses the platform-default observer.  On Linux, this will be the InotifyObserver
    # which doesn't work on NFS mounts (for which use alpenhorn.io.Polling instead).
    observer = Observer

    def __init__(self, node):
        super().__init__(node)

        # Set up a reservation for ourself if necessary
        with _mutex:
            _reserved_bytes.setdefault(self.node.name, 0)

    def check_active(self):
        """Check that the file <node.root>/ALPENHORN_NODE exists and contains the name of the node.

        Return
        ------

        True if ALPENHORN_NODE is present in `node.root` directory and contains the
        contains node name as its first line, False otherwise.

        .. Note:: The caller needs to ensure the StorageNode has the appropriate
        `active` status.
        """

        file_path = os.path.join(self.node.root, "ALPENHORN_NODE")
        try:
            with open(file_path, "r") as f:
                first_line = f.readline()
                # Check if the actual node name is in the textfile
                if self.node.name == first_line.rstrip():
                    # Great! Everything is as expected.
                    return True
                log.warning(
                    f"Node name in file {file_path} does not match expected: {self.node.name}."
                )
        except IOError:
            log.warning(f"Node file {file_path} could not be read.")

        return False

    def bytes_avail(self, fast=False):
        """Returns the number of bytes available on the filesystem.

        Does not account for space reserved via reserve_bytes()."""

        x = os.statvfs(self.node.root)
        return float(x.f_bavail) * x.f_bsize

    def fits(self, size_b):
        """Returns True if there's enough space for size_b bytes.

        Takes into account reserved space.
        """
        return self.reserve_bytes(size_b, check_only=True)

    def _walk(self, path):
        """Recurse through directory path, yielding files"""

        # We use os.scandir instead of os.walk because it gives
        # us DirEntry objects which have useful metadata in them
        for entry in os.scandir(path):
            if entry.is_dir():
                # Recurse
                for subentry in self._walk(entry):
                    yield PurePath(subentry)
            # is_file() on a symlink to a file returns true, so we need both
            elif entry.is_file() and not entry.is_symlink():
                yield PurePath(entry)

    def file_walk(self):
        """An iterator over all regular files under node.root

        path.PurePaths returned by the iterator are absolute
        """
        return self._walk(self.node.root)

    def exists(self, path):
        """Returns a boolean indicating whether the file path exists or not.

        path is relative to the root."""
        return pathlib.Path(self.node.root, path).is_file()

    def locked(self, acqname, filename):
        """Returns true if "acqname/.filename.lock" exists."""
        path = pathlib.Path(self.node.root, acqname, "." + str(filename) + ".lock")
        return path.is_file()

    def md5sum_file(self, acqname, filename):
        """Return the MD5 sum of file acqname/filename.

        This can take a long time: call it from an async."""
        return util.md5sum_file(pathlib.PurePath(self.node.root, acqname, filename))

    def filesize(self, path, actual=False):
        """Return size in bytes of the file given by path.

        Path may be absolute or relative to node.root.

        If acutal is True, returns the amount of space the file actually takes
        up on the storage system.  Otherwise returns apparent size.
        """
        path = pathlib.Path(path)
        if not path.is_absolute():
            path = pathlib.Path(self.node.root, path)

        if actual:
            # Per POSIX, blocksize for st_blocks is always 512 bytes
            return path.stat().st_blocks * 512

        # Apparent size
        return path.stat().st_size

    # This is the reservation fudge factor.  XXX Is it correct?
    reserve_factor = 2

    def reserve_bytes(self, size, check_only=False):
        """Attempt to reserve <size> bytes of space on the filesystem.

        Returns a boolean indicating whether sufficient free space was available
        to make the reservation.

        If check_only is True, no reservation is made and the only result is the
        return value.
        """
        size *= self.reserve_factor
        with _mutex:
            bavail = self.bytes_avail()
            if bavail is not None and bavail - _reserved_bytes[self.node.name] < size:
                return False  # Insufficient space

            if not check_only:
                _reserved_bytes[self.node.name] += size

            return True

    def release_bytes(self, size):
        """Release space previously reserved with reserve_bytes()."""
        size *= self.reserve_factor
        with _mutex:
            if _reserved_bytes[self.node.name] < size:
                raise ValueError(
                    f"attempted to release too many bytes: {_reserved_bytes[self.node.name]} < {size}"
                )
            _reserved_bytes[self.node.name] -= size

    def pull(self, req):
        """Queue an asynchronous I/O task to pull req.file from req.node onto the local filesystem."""

        if self.node.under_min():
            log.info(
                f"Skipping pull for StorageNode f{self.node.name}: hit minimum free space: "
                f"({self.node.avail_gb:.2f} GiB < {self.node.min_avail_gb}:.2f GiB)"
            )
            return

        if self.node.over_max():
            log.info(
                f"Skipping pull for StorageNode f{self.node.name}: node full. "
                f"({self.node.total_gb():.2f} GiB >= {self.node.max_total_gb}:.2f GiB)"
            )
            return

        # Check that there is enough space available (and reserve what we need)
        if not self.node.io.reserve_bytes(req.file.size_b):
            log.warning(
                f"Skipping request for {req.file.acq.name}/{req.file.name}: "
                f"insufficient space on node {self.node.name}."
            )
            return

        Task(
            func=pull_async,
            queue=self._queue,
            key=self.node.name,
            args=(self.node, req),
            name=f"AFCR#{req.id}: {req.node_from.name} -> {self.node.name}",
        )

    def check(self, copy):
        """Queue an asynchronous I/O task to check the integrity of file copy."""

        Task(
            func=check_async,
            queue=self._queue,
            key=self.node.name,
            args=(self.node, copy),
            name=f"Check copy#{copy.id} on {self.node.name}",
        )

    def delete(self, copies):
        """Queue a single asynchronous I/O task to delete the list of file copies."""

        # Nothing to do
        if len(copies) == 0:
            return

        Task(
            func=delete_async,
            queue=self._queue,
            key=self.node.name,
            args=(self.node, copies),
            name="Delete copies "
            + str([copy.id for copy in copies])
            + f" on {self.node.name}",
        )

    def ready_path(self, path):
        """Returns True: DefaultIO file copies are always ready."""
        return True

    def ready_pull(self, req):
        """Returns True: DefaultIO file copies are always ready."""
        return True


class DefaultGroupIO(BaseGroupIO):
    """DefaultGroupIO implements a simple StorageGroup.

    The DefaultGroupIO permits any number of StorageNodes in the group, but only permits at most
    one to be active on a given host at any time.
    """

    @property
    def idle(self):
        """Returns True if no node I/O is occurring."""
        return self.node.io.idle

    def before_update(self, nodes, idle):
        """DefaultGroupIO only accepts a single node to operate on."""

        if len(nodes) > 1:
            log.warning(f"Too many active nodes in group f{self.group.name}.")
            return False

        self.node = nodes[0]
        return True

    def exists(self, path):
        """Checks whether a file called path exists in this group.

        Returns the StorageNode containing the file, or None if no
        file was found.
        """
        if self.node.io.exists(path):
            return self.node

        return None

    def pull(self, req):
        """Fulfill a copy request pull into this group by passing the request to
        the node."""
        self.node.io.pull(req)
