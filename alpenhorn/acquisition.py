import pathlib
import logging
import datetime
import importlib

import peewee as pw

from . import config
from .db import base_model

log = logging.getLogger(__name__)


class type_base(base_model):
    """Base class for AcqType and FileType (q.v.)."""

    name = pw.CharField(max_length=64, unique=True)
    priority = pw.IntegerField(default=0, null=False)
    info_class = pw.CharField(max_length=254, null=True)
    notes = pw.TextField(null=True)
    info_config = pw.TextField(null=True)

    # This dict is an atribute on the class, used to hold the loaded info
    # classes.  Store as a dictionary for easy lookup of handlers by name.
    _info_classes = dict()

    class Meta:
        indexes = ((("priority",), False),)  # index speeds ordering

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get the import_error action for this type
        self._info_import_errors = config.info_import_errors(self.name, is_acq=True)

    def _get_info_class(self):
        """Returns the associated Info table for this type."""

        from .info_base import no_info

        # If the info class has already been loaded, just return it.
        try:
            return self._info_classes[self.name]
        except KeyError:
            pass

        # Not in _info_classes, find the info class
        if self.info_class is None:
            class_ = no_info()
        else:
            # Separate the module from the classname
            dot = self.info_class.rfind(".")
            try:
                if dot < 0:
                    # No module, try getting it from the globals, I guess
                    class_ = globals()[self.info_class]
                else:
                    modname = self.info_class[:dot]
                    classname = self.info_class[dot + 1 :]

                    # Try to import the module
                    module = importlib.import_module(modname)
                    class_ = getattr(module, classname)
            except (KeyError, ImportError, AttributeError) as e:
                if self._info_import_errors == "skip":
                    # In this case we've been told to pretend the
                    # type doesn't exist

                    class_ = no_info()
                    # Force non-match
                    class_.is_type = lambda *args, **kwargs: False

                elif self._info_import_errors == "ignore":
                    # In this case we've been told to pretend that
                    # the type had self.info_class == None.
                    class_ = no_info()
                else:
                    raise ImportError(
                        f'Unable to load info class "{self.info_class}" '
                        f'for {self.__class__.__name__} "{self.name}"'
                    ) from e

        # Initialise the _class_ with the acq_type
        class_.set_config(self)
        self._info_classes[self.name] = class_

        return class_


class AcqType(type_base):
    """The type of data that is being taken in the acquisition.

    Attributes
    ----------
    name : string
        Name of the acqusition type. e.g. `raw`, `vis`
    priority : integer
        Priority of this type.  When performing type-detection, types
        are tried in descending priority order (largest priority first).
        Do not assume the sorting is stable.
    info_class : string
        Import path of the associated Python class implementing this
        type's AcqInfo class.  The class specified must be subclassed
        from alpenhorn.info_base.acq_info_base.
    notes : string
        A human-readable description.
    info_config : string
        An optional JSON blob containing configuration data for the
        associated AcqInfo class.
    """

    def is_type(self, acqname, node):
        """Does this acquisition type understand this directory?

        Parameters
        ----------
        acqname : pathlib.Path
            path relative to node.root of the acquisition we are checking.
        node : StorageNode
            The node we are importing from. Needed so we can inspect the actual
            acquisition.

        Returns
        -------
        is_type : boolean
        """
        return self.info().is_type(acqname, node)

    @classmethod
    def detect(cls, acqname, node):
        """Try to find an acquisition type that understands this directory.

        Parameters
        ----------
        acqname : pathlib.Path
            Name of the acquisition we are trying to find the type of.
        node : StorageNode
            The node we are importing from. Needed so we can inspect the actual
            acquisition.

        Returns
        -------
        (AcqType, str) or None
            A tuple of acquisition type and name or None if one could not be found

        Notes
        -----

        The returned acquisition name is either equal to the parameter
        `acqname` or the closest ancestor directory which had a matching
        `AcqType`.

        """

        # Paths must be relative, otherwise we enter an infinite loop below
        if acqname.is_absolute():
            log.error(f"acq name ({acqname}) cannot be absolute.")
            return None

        # Iterate over all known acquisition types to try and find one that
        # can handle the acqname path. If nothing is found, repeat
        # the process with the parent directory of acqname, until we run out of
        # directory segment
        for name in acqname.parents:
            for acq_type in cls.select().order_by(AcqType.priority.desc()):
                if acq_type.is_type(name, node):
                    return acq_type, name

        # No match
        return None

    def info(self):
        """Return the Info class for this AcqType."""
        return self._get_info_class()

    @property
    def file_types(self):
        """The FileTypes supported by this AcqType, ordered by priority."""
        return (
            FileType.select()
            .join(AcqFileTypes)
            .where(AcqFileTypes.acq_type == self)
            .order_by(FileType.priority.desc())
        )


class ArchiveAcq(base_model):
    """Describe the acquisition.

    Attributes
    ----------
    name : string
        Name of acquisition.
    type : foreign key to AcqType
        The type of this acqusition
    comment : string
        User-specified comment.

    Properties
    ----------
    timed_files
    n_timed_files
    """

    name = pw.CharField(max_length=64, unique=True)
    type = pw.ForeignKeyField(AcqType, backref="acqs")
    comment = pw.TextField(null=True)


class FileType(type_base):
    """A file type.

    Attributes
    ----------
    name : string
        The name of this file type.
    priority : integer
        Priority of this type.  When performing type-detection, types
        are tried in descending priority order (largest priority first).
        Do not assume the sorting is stable.
    info_class : string
        Import path of the associated Python class implementing this
        type's FileInfo class.  The class specified must be subclassed
        from alpenhorn.info_base.file_info_base.
    notes : string
        Any notes or comments about this file type.
    info_config : string
        An optional JSON blob containing configuration data for the
        associated FileInfo class.
    """

    def is_type(self, filename, acq_name, node):
        """Check if this file can be handled by this file type.

        Parameters
        ----------
        filename : string
            Name of the file.
        acq_name : string
            The name of the acquisition the file is in.
        node : StorageNode
            The node we are importing from. Needed so we can inspect the actual
            acquisition.

        Returns
        -------
        is_type : boolean
        """
        return self.info().is_type(filename, acq_name, node)

    @classmethod
    def detect(cls, filename, acqtype, acqname, node):
        """Try to find a file type that understands this file.

        Parameters
        ----------
        filename : string
            Name of the file we are trying to find the type of.
        acqtype : AcqType
            The type of the acquisition hosting the file.
        acqname : string
            The name of the acquisition hosting the file.
        node : StorageNode
            The node we are importing from. Needed so we can inspect the actual
            file.

        Returns the found FileType or None if no FileType understood the file.
        """

        # Iterate over all known acquisition types to try and find one that matches
        # the directory being processed
        for file_type in acqtype.file_types:
            if file_type.is_type(filename, acqname, node):
                return file_type

        return None

    def info(self):
        """Return the Info class for this FileType."""
        return self._get_info_class()


class AcqFileTypes(base_model):
    """FileTypes supported by an AcqType.

    A junction table providing the many-to-many relationship
    indicating which FileTypes are supported by which AcqTypes.

    Attributes
    ----------
    acq_type : foreign key to AcqType
    file_type : foreign key to FileType
    """

    acq_type = pw.ForeignKeyField(AcqType, backref="acq_types")
    file_type = pw.ForeignKeyField(FileType, backref="file_types")

    class Meta:
        primary_key = pw.CompositeKey("acq_type", "file_type")


class ArchiveFile(base_model):
    """A file in an acquisition.

    Attributes
    ----------
    acq : foreign key to ArchiveAcq
        The acqusition containing this file.
    type : foreign key to FileType
        The type of this file.
    name : string
        Name of the file.
    size_b : integer
        Size of file in bytes.
    md5sum : string
        md5 checksum of file. Used for verifying integrity.
    registered : datetime
        The time the file was registered in the database.
    """

    acq = pw.ForeignKeyField(ArchiveAcq, backref="files")
    type = pw.ForeignKeyField(FileType, backref="files")
    name = pw.CharField(max_length=255)
    size_b = pw.BigIntegerField(null=True)
    md5sum = pw.CharField(null=True, max_length=32)
    # Note: default here is the now method itself (i.e. "now", not "now()").
    #       Will be evaulated by peewee at row-creation time.
    registered = pw.DateTimeField(default=datetime.datetime.now)

    class Meta:
        indexes = (
            (
                (
                    "acq",
                    "name",
                ),
                True,
            ),
        )  # (acq,name) is unique

    @property
    def path(self):
        """The relative path to the file copy.

        Simply the path contcatenation of acq.name and name.
        """
        return pathlib.PurePath(self.acq.name, self.name)

    def archive_count(self):
        """Return the total number of archived copies of this file"""
        from .archive import ArchiveFileCopy
        from .storage import StorageNode

        return (
            self.copies.join(StorageNode)
            .select()
            .where(
                StorageNode.storage_type == "A",
                ArchiveFileCopy.has_file == "Y",
            )
            .count()
        )


def import_info_classes():
    """Iterate through AcqType and FileType to load their associated info classes."""

    for acqtype in AcqType.select():
        acqtype.info()

    for filetype in FileType.select():
        filetype.info()
