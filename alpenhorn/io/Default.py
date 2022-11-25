"""Alpenhorn Default I/O classes.

The Alpenhorn Default I/O classes largely re-create the legacy I/O behaviour
of previous versions of Alpenhorn.  These I/O classes are used with StorageNodes
and StorageGroups which do not explicitly specify io_class.
"""


import os
import threading

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

    def acq_walk(self):
        """An iterator over all directory names in node.root"""
        for entry in os.scandir(node.root):
            if entry.is_dir():
                yield entry.name

    def file_walk(self, acqdir):
        """An iterator over all regular files in node.root/acqdir"""
        for entry in os.scandir(os.path.join(node.root, acqdir)):
            if entry.is_file():
                yield entry.name

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
            key=self.node_name,
            args=(self.node, req),
            name=f"AFCR#{req.id}: {req.node.name} -> {self.node.name}",
        )

    def check(self, copy):
        """Queue an asynchronous I/O task to check the integrity of file copy."""

        Task(
            func=check_async,
            queue=self.queue,
            key=self.node_name,
            args=(self.node, copy),
            name=f"Check copy#{copy.id} on {self.node.name}",
        )

    def import_file(self, acqname, filename):
        """Queue an asynchronous I/O task to import a newly discovered file.

        The file is called "filename" and is in the directory "acqname".

        Generally, this is called by the auto_import watchdog."""

        file_path = os.path.join(self.node.root, acqname, filename)

        # Skip requests to import non-files. These are occasionally sent by
        # the polling observer.  Calling isfile on a symlink to a file
        # returns true, so we need both calls to distinguish.
        if os.path.islink(file_path) or not os.path.isfile(file_path):
            log.debug(f"Skipping import of non-file {file_path}")
            return

        # Skip a file if there is still a lock on it.
        if os.path.isfile(os.path.join(dir_name, ".%s.lock" % base_name)):
            log.debug('Skipping "{file_path}": locked.', file_path)
            return

        # Check if we can handle this acquisition, and skip if we can't
        acq_type_name = ac.AcqType.detect(acqname, self.node)
        if acq_type_name is None:
            log.info(f'Skipping non-acquisition path "{file_path}".')
            return

        # Figure out which acquisition this is; add if necessary.
        acq_type, acq_name = acq_type_name
        with db.proxy.atomic():
            try:
                acq = ac.ArchiveAcq.get(ac.ArchiveAcq.name == acq_name)
                log.debug(f'Acquisition "%s" already in DB. Skipping.', acq_name)
            except pw.DoesNotExist:
                acq = add_acq(acq_type, acq_name, node)
                log.info(f'Acquisition "{acqname}" added to DB.', acq_name)

        Task(
            func=import_async,
            queue=self.queue,
            key=self.node_name,
            args=(self.node, acqname, filename),
            name=f"Import {acqname}/{filename} on {self.node.name}",
            # If the job fails due to DB connection loss, re-start the
            # task because unlike tasks made in the main loop, we're
            # never going to revisit this.
            requeue=True,
        )
