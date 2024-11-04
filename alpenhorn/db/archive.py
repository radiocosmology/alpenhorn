import pathlib
import datetime
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

    class Meta:
        indexes = ((("file", "node"), True),)  # (file, node) is unique


class ArchiveFileCopyRequest(base_model):
    """Requests for file copies.

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
