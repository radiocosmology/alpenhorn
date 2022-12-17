import pathlib
import datetime
import peewee as pw

from alpenhorn.acquisition import ArchiveFile
from alpenhorn.storage import StorageGroup, StorageNode

from .db import EnumField, base_model


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
        _Some_ StorageNode.io_classes use this to tell other hosts that
        files are ready to be pulled.  Other io_classes do _not_ use this
        field and assess readiness in some other way, so never check this
        directly; use StorageNode.remote.pull_ready() to determine whether
        a remote file is ready for pulling.
    size_b : integer
        Allocated size of file in bytes (i.e. actual size on the Storage
        medium.)
    """

    file = pw.ForeignKeyField(ArchiveFile, backref="copies")
    node = pw.ForeignKeyField(StorageNode, backref="copies")
    has_file = EnumField(["N", "Y", "M", "X"], default="N")
    wants_file = EnumField(["Y", "M", "N"], default="Y")
    ready = pw.BooleanField(default=False)
    size_b = pw.BigIntegerField(null=True)
    last_update = pw.TimestampField(default=datetime.datetime.now())

    @property
    def path(self):
        """The absolute path to the file copy.

        For a relative path (one omitting node.root), use copy.file.path
        """

        # It's a PurePath and not a Path because it's not guaranteed to exist
        # because this property is defined even when has_file!=Y
        return pathlib.PurePath(node.root, file.path)

    class Meta:
        indexes = ((("file", "node"), True),)


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
    nice : integer
        For nicing the copy/rsync process if resource management is needed.
    completed : bool
        Set to true when the copy has succeeded.
    cancelled : bool
        Set to true if the copy is no longer wanted.
    timestamp : datetime
        The time the request was made.
    transfer_started : datetime
        The time the transfer was started.
    transfer_completed : datetime
        The time the transfer was completed.
    """

    file = pw.ForeignKeyField(ArchiveFile, backref="requests")
    group_to = pw.ForeignKeyField(StorageGroup, backref="requests_to")
    node_from = pw.ForeignKeyField(StorageNode, backref="requests_from")
    nice = pw.IntegerField()
    completed = pw.BooleanField()
    cancelled = pw.BooleanField(default=False)
    timestamp = pw.DateTimeField()
    transfer_started = pw.DateTimeField(null=True)
    transfer_completed = pw.DateTimeField(null=True)

    class Meta:
        indexes = ((("file", "group_to", "node_from"), False),)  # non-unique index
