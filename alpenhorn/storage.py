import peewee as pw

from .db import EnumField, base_model


class StorageGroup(base_model):
    """Storage group for the archive.

    Attributes
    ----------
    name : string
        The group that this node belongs to (Scinet, DRAO hut, . . .).
    notes : string
        Any notes about this storage group.
    """

    name = pw.CharField(max_length=64)
    notes = pw.TextField(null=True)


class StorageNode(base_model):
    """A path on a disc where archives are stored.

    Attributes
    ----------
    name : string
        The name of this node.
    root : string
        The root directory for data in this node.
    host : string
        The hostname that this node lives on.
    address : string
        The internet address for the host (e.g., mistaya.phas.ubc.ca)
    group : foreign key
        The group to which this node belongs.
    active : bool
        Is the node active?
    auto_import : bool
        Should files that appear on this node be automatically added?
    suspect : bool
        Could this node be corrupted?
    storage_type : enum
        What is the type of storage?
        - 'A': archive for the data
        - 'T': for transiting data
        - 'F': for data in the field (i.e acquisition machines)
    max_total_gb : float
        The maximum amout of storage we should use.
    min_avail_gb : float
        What is the minimum amount of free space we should leave on this node?
    avail_gb : float
        How much free space is there on this node?
    avail_gb_last_checked : datetime
        When was the amount of free space last checked?
    min_delete_age_days : float
        What is the minimum amount of time a file must remain on the node before
        we are allowed to delete it?
    notes : string
        Any notes or comments about this node.
    """

    name = pw.CharField(max_length=64)
    root = pw.CharField(max_length=255, null=True)
    host = pw.CharField(max_length=64, null=True)
    username = pw.CharField(max_length=64, null=True)
    address = pw.CharField(max_length=255, null=True)
    group = pw.ForeignKeyField(StorageGroup, backref="nodes")
    active = pw.BooleanField(default=False)
    auto_import = pw.BooleanField(default=False)
    suspect = pw.BooleanField(default=False)
    storage_type = EnumField(["A", "T", "F"], default="A")
    max_total_gb = pw.FloatField(default=-1.0)
    min_avail_gb = pw.FloatField()
    avail_gb = pw.FloatField(null=True)
    avail_gb_last_checked = pw.DateTimeField(null=True)
    min_delete_age_days = pw.FloatField(default=30)
    notes = pw.TextField(null=True)
