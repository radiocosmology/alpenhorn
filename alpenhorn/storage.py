import peewee as pw
import importlib

from .db import EnumField, base_model
from .io.base import BaseNodeIO, BaseGroupIO


def _get_io_instance(obj):
    """Returns an instance of the I/O class for a StorageNode or StorageGroup"""

    # We assume StorageNode if not StorageGroup
    if isinstance(obj, StorageGroup):
        obj_type = "StorageGroup"
        io_suffix = "GroupIO"
        base_class = BaseGroupIO
    else:
        obj_type = "StorageNode"
        io_suffix = "NodeIO"
        base_class = BaseNodeIO

    # If no io_class is specified, the Default I/O classes are used
    io_class = "Default" if obj.io_class is None else obj.io_class

    # If io_class is, say, "Default", then we look for the module alpenhorn.io.Default
    try:
        module = importlib.import_module("alpenhorn.io." + io_class)
    except ImportError as e:
        raise ValueError(
            f'Unable to find I/O class "{io_class}{io_suffix}" for {obj_type} {obj.name}.'
        ) from e

    # Add suffix to create I/O class name, i.e. ("DefaultNodeIO" or "DefaultGroupIO" or whatever)
    io_class.append(io_suffix)

    # Within the module, find the class
    try:
        class_ = getattr(module, io_class)
    except AttributeError as e:
        raise ValueError(
            f'Unable to resolve I/O class "{io_class}" for {obj_type} {obj.name}.'
        ) from e

    # Check class_
    if not issubclass(class_, base_class):
        raise TypeError(
            f'I/O class "{io_class}" for {obj_type} {obj.name} does not descend from {base_class}.'
        )

    # instantiate the class and pass the object to it
    return class_(obj)


class StorageGroup(base_model):
    """Storage group for the archive.

    Attributes
    ----------
    name : string
        The group that this node belongs to (Scinet, DRAO hut, . . .).
    io_class : string
        The I/O class for this group (e.g., Default, Transport, &c.)
        If this is NULL, the value "Default" is used.
    notes : string
        Any notes about this storage group.
    io_config : string
        An optional JSON blob of configuration data interpreted by the I/O class
    """

    name = pw.CharField(max_length=64)
    io_class = pw.CharField(max_length=255, null=True)
    notes = pw.TextField(null=True)
    io_config = pw.TextField(null=True)

    @property
    def io(self):
        """An instance of the I/O class for this group"""

        if self._io is None:
            self._io = _get_io_instance(self)
        return self._io

    def copy_present(self, file):
        """Is a copy of ArchiveFile file present in this group?"""

        try:
            ar.ArchiveFileCopy.join(st.StorageNode).select().where(
                ar.ArchiveFileCopy.file == file,
                ar.ArchiveFileCopy.node.group == self,
                ar.ArchiveFileCopy.has_file == "Y",
            ).get()
        except pw.DoesNotExist:
            return False

        return True


class StorageNode(base_model):
    """A place on a host where files copies are stored.

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
    io_class : string
        The I/O class for this node (e.g., Default, HPSS, Nearline, &c.)
        If this is NULL, the value "Default" is used.
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
        - 'A': archival storage
        - 'T': transiting storage
        - 'F': all other storage (i.e acquisition machines)
    avail_gb : float
        How much free space is there on this node?
    avail_gb_last_checked : datetime
        When was the amount of free space last checked?
    min_delete_age_days : float
        What is the minimum amount of time a file must remain on the node before
        we are allowed to delete it?
    notes : string
        Any notes or comments about this node.
    io_config : string
        An optional JSON blob of configuration data interpreted by the I/O class
    """

    def init(self):
        self._io = None
        self._remote = None

    name = pw.CharField(max_length=64)
    root = pw.CharField(max_length=255, null=True)
    host = pw.CharField(max_length=64, null=True)
    username = pw.CharField(max_length=64, null=True)
    address = pw.CharField(max_length=255, null=True)
    io_class = pw.CharField(max_length=255, null=True)
    group = pw.ForeignKeyField(StorageGroup, backref="nodes")
    active = pw.BooleanField(default=False)
    auto_import = pw.BooleanField(default=False)
    storage_type = EnumField(["A", "T", "F"], default="A")
    max_total_gb = pw.FloatField(default=-1.0, null=True)
    min_avail_gb = pw.FloatField(null=True)
    avail_gb = pw.FloatField(null=True)
    avail_gb_last_checked = pw.DateTimeField(null=True)
    min_delete_age_days = pw.FloatField(default=30, null=True)
    notes = pw.TextField(null=True)
    io_config = pw.TextField(null=True)

    @property
    def io(self):
        """An instance of the I/O class for this node"""

        if self._io is None:
            self._io = _get_io_instance(self)
        return self._io

    @proptery
    def remote(self):
        """An instance of the remote-I/O class for this node"""
        if self._remote is None:
            self._remote = self.io.get_remote()
        return self._remote

    @property
    def archive(self):
        """Is this node an archival node?"""
        return self.storage_type == "A"

    def full(self):
        """Is the total size of file copies on the node greater than the value of max_total_gb?

        If max_total_gb is None, returns False.
        """
        return self.max_total_gb is not None and self.total_gb() >= self.max_total_gb

    def named_copy_present(self, acqname, filename):
        """Is a copy of a file called filename in acqname present
        on this node?"""
        try:
            ar.ArchiveFileCopy.join(ArchiveFile).join(ArchiveAcq).get(
                ar.ArchiveAcq.name == acqname,
                ar.ArchiveFile.name == filename,
                ar.ArchiveFileCopy.node == self,
                ar.ArchiveFileCopy.has_file == "Y",
            )
        except pw.DoesNotExist:
            return False

        return True

    def copy_present(self, file):
        """Is a copy of ArchiveFile file present on this node?"""
        try:
            ar.ArchiveFileCopy.get(
                ar.ArchiveFileCopy.file == req.file,
                ar.ArchiveFileCopy.node == self,
                ar.ArchiveFileCopy.has_file == "Y",
            )
        except pw.DoesNotExist:
            return False

        return True

    def total_gb(self):
        """total_gb: The nominal size (in GiB) of all files on node calculated from the database.

        This is the total of all apparent file sizes from the database.  It may be quite different
        than the amount of actual space the file copies take up on the underlying storage system."""
        size = (
            ac.ArchiveFile.select(fn.Sum(ac.ArchiveFile.size_b))
            .join(ar.ArchiveFileCopy)
            .where(
                ar.ArchiveFileCopy.node == self.node, ar.ArchiveFileCopy.has_file == "Y"
            )
        ).scalar(as_tuple=True)[0]

        return 0.0 if size is None else float(size) / 2**20

    def all_files(self):
        """all_files: a list of all files on node

        Returns paths relative to root for all file copies exsiting on the node.
        """
        return [
            os.path.join(*copy)
            for copy in (
                ar.ArchiveFileCopy.join(ac.ArchiveFile)
                .join(ac.ArchiveAcq)
                .select(ac.ArchiveFile.name, ac.ArchiveAcq.name)
                .where(
                    ar.ArchiveFileCopy.node == node, ar.ArchiveFileCopy.has_file == "Y"
                )
            ).tuples()
        ]

        return files
