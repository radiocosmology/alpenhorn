import peewee as pw

from alpenhorn.acquisition import ArchiveFile
from alpenhorn.storage import StorageGroup, StorageNode

from .db import EnumField, base_model


class ArchiveFileCopy(base_model):
    """Information about a file.

    Attributes
    ----------
    file : foreign key
        Reference to the file of which this is a copy.
    node : foreign key
        The node on which this copy lives (or should live).
    has_file : string
        Is the node on the file?
        - 'Y': yes, the node has the file.
        - 'N': no, the node does not have the file.
        - 'M': maybe: we've tried to copy/erase it, but haven't yet verified.
        - 'X': the file is there, but has been verified to be corrupted.
    wants_file : enum
        Does the node want the file?
        - 'Y': yes, keep the file around
        - 'M': maybe, can delete if we need space
        - 'N': no, attempt to delete
        In all cases we try to keep at least two copies of the file around.
    size_b : integer
        Allocated size of file in bytes (calculated as a multiple of 512-byte
        blocks).
    """

    file = pw.ForeignKeyField(ArchiveFile, backref="copies")
    node = pw.ForeignKeyField(StorageNode, backref="copies")
    has_file = EnumField(["N", "Y", "M", "X"], default="N")
    wants_file = EnumField(["Y", "M", "N"], default="Y")
    size_b = pw.BigIntegerField()

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
