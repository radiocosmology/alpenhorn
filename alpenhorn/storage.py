import importlib
import peewee as pw
from peewee import fn

from .db import EnumField, base_model


def _get_io_instance(obj):
    """Returns an instance of the I/O class for a StorageNode or StorageGroup"""

    # We assume StorageNode if not StorageGroup
    if isinstance(obj, StorageGroup):
        obj_type = "StorageGroup"
        io_suffix = "GroupIO"
    else:
        obj_type = "StorageNode"
        io_suffix = "NodeIO"

    # If no io_class is specified, the Default I/O classes are used
    io_class = "Default" if obj.io_class is None else obj.io_class

    # If io_class is, say, "Default", then we look for the module alpenhorn.io.Default
    try:
        module = importlib.import_module("alpenhorn.io." + io_class)
    except ImportError as e:
        raise ValueError(
            f'Unable to find I/O module "alpenhorn.io.{io_class}" for {obj_type} {obj.name}.'
        ) from e

    # Add suffix to create I/O class name, i.e. ("DefaultNodeIO" or "DefaultGroupIO" or whatever)
    io_class += io_suffix

    # Within the module, find the class
    try:
        class_ = getattr(module, io_class)
    except AttributeError as e:
        raise ValueError(
            f'Unable to resolve I/O class "{io_class}" for {obj_type} {obj.name}.'
        ) from e

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

    class Meta:
        indexes = ((("name",), True),)  # name is unique

    @property
    def io(self):
        """An instance of the I/O class for this group"""

        if getattr(self, "_io", None) is None:
            self._io = _get_io_instance(self)
        return self._io

    def copy_state(self, file):
        """Returns the value of has_file for an ArchiveFileCopy for
        the given ArchiveFile file if found in this group, or 'N'
        if no such copy exists.

        Return value
        ------------
        The return value will be one of:
        - 'Y' file copy exists
        - 'X' file copy is corrupt
        - 'M' file copy needs to be checked
        - 'N' file copy does not exist.
        """
        from .archive import ArchiveFileCopy

        try:
            copy = (
                ArchiveFileCopy.select(ArchiveFileCopy.has_file)
                .join(StorageNode)
                .where(
                    ArchiveFileCopy.node.group == self,
                    ArchiveFileCopy.file == file,
                )
            ).get()
            return copy.has_file
        except pw.DoesNotExist:
            pass

        return "N"


class StorageNode(base_model):
    """A place on a host where file copies are stored.

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
    auto_verify : integer
        If greater than zero, automatically re-verify file copies on this
        node during times of no other activity.  The value is the maximum
        number of files that will be re-verified per update loop.
    storage_type : enum
        What is the type of storage?
        - 'A': archival storage
        - 'T': transiting storage
        - 'F': all other storage (i.e acquisition machines)
    max_total_gb : float
        The maximum amout of storage we should use.
    min_avail_gb : float
        What is the minimum amount of free space we should leave on this node?
    avail_gb : float
        How much free space is there on this node?
    avail_gb_last_checked : datetime
        When was the amount of free space last checked?
    notes : string
        Any notes or comments about this node.
    io_config : string
        An optional JSON blob of configuration data interpreted by the I/O class
    """

    name = pw.CharField(max_length=64)
    root = pw.CharField(max_length=255, null=True)
    host = pw.CharField(max_length=64, null=True)
    username = pw.CharField(max_length=64, null=True)
    address = pw.CharField(max_length=255, null=True)
    io_class = pw.CharField(max_length=255, null=True)
    group = pw.ForeignKeyField(StorageGroup, backref="nodes")
    active = pw.BooleanField(default=False)
    auto_import = pw.BooleanField(default=False)
    auto_verify = pw.IntegerField(default=0)
    storage_type = EnumField(["A", "T", "F"], default="A")
    max_total_gb = pw.FloatField(null=True)
    min_avail_gb = pw.FloatField(null=True)
    avail_gb = pw.FloatField(null=True)
    avail_gb_last_checked = pw.DateTimeField(null=True)
    notes = pw.TextField(null=True)
    io_config = pw.TextField(null=True)

    class Meta:
        indexes = ((("name",), True),)  # name is unique

    @property
    def io(self):
        """An instance of the I/O class for this node"""

        if getattr(self, "_io", None) is None:
            self._io = _get_io_instance(self)
        return self._io

    @property
    def remote(self):
        """An instance of the remote-I/O class for this node"""
        if getattr(self, "_remote", None) is None:
            self._remote = self.io.get_remote()
        return self._remote

    @property
    def archive(self):
        """Is this node an archival node?"""
        return self.storage_type == "A"

    def under_min(self):
        """Is the amount of free space below the minimum allowed?

        Returns False if avail_gb is None or if min_avail_gb is zero or None.
        """
        if self.avail_gb is None or not self.min_avail_gb:
            return False
        return self.avail_gb < self.min_avail_gb

    def over_max(self):
        """Is the total size of file copies on the node greater than allowed?

        If max_total_gb is non-positive or None, returns False.
        """
        if self.max_total_gb is None or self.max_total_gb <= 0:
            return False
        return self.total_gb() >= self.max_total_gb

    def named_copy_present(self, acqname, filename):
        """Is a copy of a file called filename in acqname present
        on this node?"""
        from .archive import ArchiveFileCopy
        from .acquisition import ArchiveFile, ArchiveAcq

        try:
            ArchiveFileCopy.select().join(ArchiveFile).join(ArchiveAcq).where(
                ArchiveAcq.name == acqname,
                ArchiveFile.name == filename,
                ArchiveFileCopy.node == self,
                ArchiveFileCopy.has_file == "Y",
            ).get()
        except pw.DoesNotExist:
            return False

        return True

    def copy_present(self, file):
        """Is a copy of ArchiveFile file present on this node?"""
        from .archive import ArchiveFileCopy

        try:
            ArchiveFileCopy.get(
                ArchiveFileCopy.file == file,
                ArchiveFileCopy.node == self,
                ArchiveFileCopy.has_file == "Y",
            )
        except pw.DoesNotExist:
            return False

        return True

    def total_gb(self):
        """total_gb: The nominal size (in GiB) of all files on node calculated from the database.

        This is the total of all apparent file sizes from the database.  It may be quite different
        than the amount of actual space the file copies take up on the underlying storage system."""
        from .acquisition import ArchiveFile
        from .archive import ArchiveFileCopy

        size = (
            ArchiveFile.select(fn.Sum(ArchiveFile.size_b))
            .join(ArchiveFileCopy)
            .where(ArchiveFileCopy.node == self, ArchiveFileCopy.has_file == "Y")
        ).scalar(as_tuple=True)[0]

        return 0.0 if size is None else float(size) / 2**20

    def all_files(self):
        """all_files: a list of all files on node

        Returns paths relative to root for all file copies exsiting on the node.
        """
        from .archive import ArchiveFileCopy

        return [
            copy.file.path
            for copy in (
                ArchiveFileCopy.select().where(
                    ArchiveFileCopy.node == self, ArchiveFileCopy.has_file == "Y"
                )
            )
        ]
