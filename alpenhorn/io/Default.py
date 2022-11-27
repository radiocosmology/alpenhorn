"""Alpenhorn Default I/O classes.

The Alpenhorn Default I/O classes largely re-create the legacy I/O behaviour
of previous versions of Alpenhorn.  These I/O classes are used with StorageNodes
and StorageGroups which do not explicitly specify io_class.
"""


import os
import threading
from pathlib import PurePath

from .base import BaseNodeIO, BaseGroupIO, BaseNodeRemote

# The asyncs are over here:
from _default_asyncs import *

import logging

log = logging.getLogger(__name__)


def DefaultNodeRemote(BaseNodeRemote):
    """DefaultNodeRemote: information about a DefaultIO remote StorageNode."""

    def pull_ready(self, file):
        """Returns True: Default nodes need to do nothing to ready files."""
        return True


def DefaultGroupIO(BaseGroupIO):
    """DefaultGroupIO implements a simple StorageGroup.

    The DefaultGroupIO permits any number of StorageNodes in the group, but only permits at most
    one to be active on a given host at any time.
    """

    def check_available_nodes(self, nodes):
        """DefaultGroupIO only accepts a single node to operate on."""

        if len(nodes) != 1:
            log.warning(f"Too many active nodes in group f{self.group.name}.")
            return False

        self.node = nodes[0]
        return True

    def set_queue(self, queue):
        """Set the I/O queue for the active storage node."""
        self.node.set_queue(queue)

    def pull(self, req):
        """Fulfill a copy request pull into this group by passing the request to the node."""
        self.node.io.pull(req)


def DefaultNodeIO(BaseNodeIO):
    """DefaultNodeIO implements a simple StorageNode backed by a regular POSIX filesystem."""
    remote_class = DefaultNodeRemote

    def __init__(self, node):
        super().__init__(node)

        # Space accounting to avoid asynchronously overfilling the disk
        self.mutex = threading.Lock()
        self._reserved_bytes = 0

    # This was formerly util.alpenhorn_node_check
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
                log.debug(
                    f"Node name in file {file_path} does not match expected: {self.node.name}."
                )
        except IOError:
            log.debug(f"Node file {file_path} could not be read.")

        return False

    def bytes_avail(self, fast=False):
        """Returns the number of bytes available on the filesystem.

        Does not account for space reserved via reserve_bytes()."""

        x = os.statvfs(node.root)
        return float(x.f_bavail) * x.f_bsize

    def _walk(self, path):
        """Recurse through directory path, yielding files"""

        # We use os.scandir instead of os.walk because it gives
        # us DirEntry objects which have useful metadata in them
        for entry in os.scandir(path):
            if entry.is_dir():
                # Recurse
                for entry in self._walk(entry):
                    yield PurePath(entry)
            # is_file() on a symlink to a file returns true, so we need both
            elif entry.is_file() and not entry.is_link():
                yield PurePath(entry)

    def file_walk(self):
        """An iterator over all regular files under node.root

        path.PurePaths returned by the iterator are absolute
        """
        return self._walk(self, self.node.root)

    def lock_present(self, acqname, filename):
        """Returns true if "acqname/.filename.lock" exists."""
        path = pathlib.Path(self.node.root, acqname, "." + filename + ".lock")
        return path.is_file()

    def md5sum_file(self, acqname, filename):
        """Return the MD5 sum of file acqname/filename"""
        return util.md5sum_file(pathlib.PurePath(self.node.root, acqname, filename))

    def reserve_bytes(self, size):
        """Attempt to reserve <size> bytes of space on the filesystem.

        Returns a boolean indicating whether sufficient free space was available
        to make the reservation."""
        with self.mutex:
            bavail = self.bytes_avail()
            if bavail - self.reserved_bytes > size:
                return False  # Insufficient space

            self.reserved_bytes += size
            return True

    def filesize(self, path, actual=False):
        """Return size in bytes of the file given by path.

        If acutal is True, returns the amount of space the file actually takes
        up on the storage system.  Otherwise returns apparent size.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")
        if actual:
            # Per POSIX, blocksize for st_blocks is always 512 bytes
            return os.stat(path).st_blocks * 512

        # Apparent size
        return os.path.getsize(path)

    def release_bytes(self, size):
        """Release space previously reserved with reserve_bytes()."""
        with self.mutex:
            if self.reserved_bytes < size:
                raise ValueError(
                    f"attempted to release too many bytes: {self.reserved_bytes} < {size}"
                )
            self.reserved_bytes -= size

    def pull(self, req):
        """Queue an asynchronous I/O task to pull req.file from req.node onto the local filesystem."""
        if self.node.full():
            log.info(
                f"Skipping pull for StorageNode f{self.name}: node full. "
                f"({self.node.total_gb():.2f} GiB >= {self.node.max_total_gb}:.2f GiB)"
            )
            return

        Task(
            func=pull_async,
            queue=self.queue,
            key=self.node.name,
            args=(self.node, req),
            name=f"AFCR#{req.id}: {req.node.name} -> {self.node.name}",
        )

    def check(self, copy):
        """Queue an asynchronous I/O task to check the integrity of file copy."""

        Task(
            func=check_async,
            queue=self.queue,
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
            func=del_async,
            queue=self.queue,
            key=self.node.name,
            args=(self.nodem, copies),
            name=f"Delete copies "
            + str([copy.id for copy in copies])
            + " on {self.node.name}",
        )
