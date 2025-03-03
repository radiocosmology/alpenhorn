import pathlib

import peewee as pw

from ._base import EnumField, base_model
from .acquisition import ArchiveFile
from .storage import StorageGroup, StorageNode


class ArchiveFileCopy(base_model):
    """Information about a copy of a file on a node.

    Attributes
    ----------
    file : foreign key
        Reference to the file of which this is a copy.
    node : foreign key
        The node on which this copy lives (or should live).
    has_file : enum
        Is the file on the node?
        - 'Y': yes, the node has a copy of the file.
        - 'N': no, the node does not have the file.
        - 'M': maybe: the file copy needs to be re-checked.
        - 'X': the file is there, but has been verified to be corrupted.
    wants_file : enum
        Does the node want the file?
        - 'Y': yes, keep the file around
        - 'M': maybe, can delete if we need space
        - 'N': no, should be deleted
        In all cases we try to keep at least two copies of the file around.
    ready : bool
        _Some_ StorageNode I/O classes use this to tell other hosts that
        files are ready for access.  Other I/O classes do _not_ use this
        field and assess readiness in some other way, so never check this
        directly; outside of the I/O-class code itself, use
        StorageNode.io.ready_path() or StorageNode.remote.pull_ready()
        to determine whether a remote file is ready for I/O.
    size_b : integer
        Allocated size of file in bytes (i.e. actual size on the Storage
        medium.)
    last_update : datetime
        The time at which this record was last updated.
    """

    file = pw.ForeignKeyField(ArchiveFile, backref="copies")
    node = pw.ForeignKeyField(StorageNode, backref="copies")
    has_file = EnumField(["N", "Y", "M", "X"], default="N")
    wants_file = EnumField(["Y", "M", "N"], default="Y")
    ready = pw.BooleanField(default=False)
    size_b = pw.BigIntegerField(null=True)
    last_update = pw.DateTimeField(default=pw.utcnow)

    @property
    def path(self) -> pathlib.Path:
        """The absolute path to the file copy.

        For a relative path (one omitting node.root), use copy.file.path
        """

        return pathlib.Path(self.node.root, self.file.path)

    @property
    def state(self) -> str:
        """A human-readable description of the copy state."""

        # key is '{has_file}{wants_file}'
        states = {
            # has_file == 'Y'
            "YY": "Healthy",
            "YM": "Removable",
            "YN": "Released",
            # has_file == 'M'
            "MY": "Suspect",
            "MM": "Suspect",
            "MN": "Released",
            # has_file == 'X'
            "XY": "Corrupt",
            "XM": "Corrupt",
            "XN": "Released",
            # has_file == 'N'
            "NY": "Missing",
            "NM": "Removed",
            "NN": "Removed",
        }

        key = self.has_file + self.wants_file
        return states.get(key, "Corrupt")

    class Meta:
        indexes = ((("file", "node"), True),)  # (file, node) is unique


class ArchiveFileCopyRequest(base_model):
    """Requests for file transfer from node to group.

    Attributes
    ----------
    file : foreign key
        Reference to the file to be copied.
    group_to : foreign key
        The storage group to which the file should be copied.
    node_from : foreign key
        The node from which the file should be copied.
    completed : bool
        Set to true when the copy has succeeded.
    cancelled : bool
        Set to true if the copy is no longer wanted.
    timestamp : datetime
        The UTC time when the request was made.
    transfer_started : datetime
        The UTC time when the transfer was started.
    transfer_completed : datetime
        The UTC time when the transfer was completed.
    """

    file = pw.ForeignKeyField(ArchiveFile, backref="requests")
    group_to = pw.ForeignKeyField(StorageGroup, backref="requests_to")
    node_from = pw.ForeignKeyField(StorageNode, backref="requests_from")
    completed = pw.BooleanField(default=False)
    cancelled = pw.BooleanField(default=False)
    timestamp = pw.DateTimeField(default=pw.utcnow, null=True)
    transfer_started = pw.DateTimeField(null=True)
    transfer_completed = pw.DateTimeField(null=True)

    class Meta:
        indexes = ((("file", "group_to", "node_from"), False),)  # non-unique index


class ArchiveFileImportRequest(base_model):
    """Requests for the import of new files into a node.

    Attributes
    ----------
    node : foreign key
        The StorageNode on which the import should happen
    path : string
        The path to import.  If this is a directory and recurse is True,
        it will be recursively scanned.
    recurse : bool
        If True, recursively scan path for files, if path is a directory.
    register : bool
        If False, only files with existing ArchiveFile records will be
        imported.
    completed : bool
        Set to true when the import request has completed.
    timestamp : datetime
        The UTC time when the request was made.

    If `path` is the special value "ALPENHORN_NODE", then the request is
    a node initialisation request, instead of a normal import request.
    This only happens on active nodes which aren't already initialised:
    an initialisation request on an already initialised node is ignored.

    Note: How node initialisation works is dependent on the I/O class of
    the node.  There is no requirement for initialisation to create an
    ALPENHORN_NODE node file.
    """

    node = pw.ForeignKeyField(StorageNode, backref="import_requests")
    path = pw.CharField(max_length=1024)
    recurse = pw.BooleanField(default=False)
    register = pw.BooleanField(default=False)
    completed = pw.BooleanField(default=False)
    timestamp = pw.DateTimeField(default=pw.utcnow, null=True)
