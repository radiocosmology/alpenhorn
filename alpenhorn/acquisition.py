import pathlib
import logging
import datetime
import importlib

import peewee as pw

from . import config
from .db import base_model

log = logging.getLogger(__name__)


class AcqType(base_model):
    """The type of data that is being taken in the acquisition.

    Attributes
    ----------
    name : string
        Name of the acqusition type. e.g. `raw`, `vis`
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

    name = pw.CharField(max_length=64, unique=True)
    info_class = pw.CharField(max_length=254, null=True)
    notes = pw.TextField(null=True)
    info_config = pw.TextField(null=True)

    # This dict is an atribute on the class, used to hold the set of registered
    # handlers. Store as a dictionary for easy lookup of handlers by name.
    _registered_acq_types = {}

    def is_type(self, acqname, node):
        """Does this acquisition type understand this directory?

        Parameters
        ----------
        acqname : string
            Name of the acquisition we are checking.
        node : StorageNode
            The node we are importing from. Needed so we can inspect the actual
            acquisition.

        Returns
        -------
        is_type : boolean
        """
        return self.acq_info().is_type(acqname, node)

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
            for acq_type in cls.select():
                if acq_type.is_type(name, node):
                    return acq_type, name

        # No match
        return None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get the import_error action for this type
        self._info_import_errors = config.info_import_errors(self.name, is_acq=True)

    def acq_info(self):
        """The AcqInfo table for this AcqType."""

        from .info_base import no_info

        # If the info class has already been loaded, just return it.
        try:
            return self._registered_acq_types[self.name]
        except KeyError:
            pass

        # Not in _registered_acq_types, find the info class
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
                    self._registered_acq_types[self.name] = None
                    return None
                elif self._info_import_errors == "ignore":
                    class_ = no_info()
                else:
                    raise ImportError(
                        f'Unable to load info class "{self.info_class}" '
                        f'for acq type "{self.name}"'
                    ) from e

        # Initialise the _class_ with the acq_type
        class_.set_config(self)
        self._registered_acq_types[self.name] = class_

        return class_

    @property
    def file_types(self):
        """The FileTypes supported by this AcqType."""
        return FileType.select().join(AcqFileTypes).where(AcqFileTypes.acq_type == self)


class ArchiveAcq(base_model):
    """Describe the acquisition.

    Attributes
    ----------
    name : string
        Name of acquisition.
    type : foreign key
        Reference to the data type type.
    comment : string

    Properties
    ----------
    timed_files
    n_timed_files
    """

    name = pw.CharField(max_length=64, unique=True)
    type = pw.ForeignKeyField(AcqType, backref="acqs")
    comment = pw.TextField(null=True)


class FileType(base_model):
    """A file type.

    Attributes
    ----------
    name : string
        The name of this file type.
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

    name = pw.CharField(max_length=64, unique=True)
    info_class = pw.CharField(max_length=254, null=True)
    notes = pw.TextField(null=True)
    info_config = pw.TextField(null=True)

    # This dict is an atribute on the class used to hold the set of registered
    # handlers. Store as a dictionary for easy lookup of handlers by name.
    _registered_file_types = {}

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
        return self.file_info.is_type(filename, acq_name, node)

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get the import_error action for this type
        self._info_import_errors = config.info_import_errors(self.name, is_acq=False)

    def file_info(self):
        """The FileInfo table for this FileType."""

        # If the info class has already been loaded, just return it.
        try:
            return self._registered_acq_types[self.name]
        except KeyError:
            pass

        # Not in _registered_acq_types, find the info class
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
                    self._registered_acq_types[self.name] = None
                    return None
                elif self._info_import_errors == "ignore":
                    class_ = no_info()
                else:
                    raise ImportError(
                        f'Unable to load info class "{self.info_class}" '
                        f'for acq type "{self.name}"'
                    ) from e

        # Initialise the _class_ with the acq_type
        class_.set_config(self)
        self._registered_acq_types[self.name] = class_

        return class_


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
        primary_key = pw.CompositeKey("acq", "file")


class ArchiveFile(base_model):
    """A file in an acquisition.

    Attributes
    ----------
    acq : foreign key
        Reference to the acquisition this file is part of.
    type : foreign key
        Reference to the type of file that this is.
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
        acqtype.acq_info()

    for filetype in FileType.select():
        filetype.file_info()
