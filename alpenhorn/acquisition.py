from os import path

import peewee as pw
from .db import base_model
from .config import ConfigClass

# Setup the logging
from . import logger
log = logger.get_log()


class ArchiveInst(base_model):
    """Instrument that took the data.

    Attributes
    ----------
    name : string
        Name of instrument.
    """
    name = pw.CharField(max_length=64)
    notes = pw.TextField(null=True)


class AcqType(base_model):
    """The type of data that is being taken in the acquisition.

    Attributes
    ----------
    name : string
        Short name of type. e.g. `raw`, `vis`
    """
    name = pw.CharField(max_length=64)
    notes = pw.TextField(null=True)

    # This dict is an atribute on the class, used to hold the set of registered
    # handlers. Store as a dictionary for easy lookup of handlers by name.
    _registered_acq_types = {}

    @classmethod
    def register_type(cls, acq_info):
        """Register a new acquisition type with alpenhorn.

        This creates the entry in the AcqType table if it does not already exist.

        Parameters
        ----------
        acq_info : AcqInfoBase
            AcqInfoBase describing the type of acquisition.
        """

        try:
            cls.get(name=acq_info._acq_type)
        except pw.DoesNotExist:
            log.info("Create AcqType entry for \"%s\"" % acq_info._acq_type)
            cls.create(name=acq_info._acq_type)

        # Add to registry
        cls._registered_acq_types[acq_info._acq_type] = acq_info

    @classmethod
    def check_registration(cls):
        """Check that all AcqTypes known to the database have a
        registered handler.
        """

        # Get the list of types names from the database
        db_acq_types = [row[0] for row in cls.select(cls.name).tuples()]

        # Get the names of all the registered types
        reg_acq_types = cls._registered_acq_types.keys()

        # Find any missing types
        missing_acq_types = set(db_acq_types) - set(reg_acq_types)

        if len(missing_acq_types):
            raise RuntimeError('AcqTypes %s have no registered handler.' %
                               repr(missing_acq_types))

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
        return self.acq_info._is_type(acqname, node.root)

    @classmethod
    def detect(cls, acqname, node):
        """Try to find an acquisition type that understands this directory.

        Parameters
        ----------
        acqname : string
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

        # Iterate over all known acquisition types to try and find one that
        # can handle the acqname path. If nothing is found, repeat
        # the process with the parent directory of acqname, until we run out of
        # directory segments
        while acqname != '':
            for acq_type in cls.select():
                if acq_type.is_type(acqname, node):
                    return acq_type, acqname
            acqname = path.dirname(acqname)
        return None

    @property
    def acq_info(self):
        """The AcqInfo table for this AcqType.
        """
        return self.__class__._registered_acq_types[self.name]

    @property
    def file_types(self):
        """The FileTypes supported by this AcqType.
        """

        def _resolve(x):
            if isinstance(x, str):
                return x
            elif isinstance(x, type) and issubclass(x, FileInfoBase):
                return x.file_type
            else:
                raise RuntimeError("FileType %s not understood." % repr(x))

        # Resolve the file_types list of the AcqInfo to the names
        file_type_names = [_resolve(x) for x in self.acq_info._file_types]

        # Query for the names in the database and return
        return FileType.select().where(FileType.name << file_type_names)


class ArchiveAcq(base_model):
    """Describe the acquisition.

    Attributes
    ----------
    name : string
        Name of acquisition.
    inst : foreign key
        Reference to the instrument that took the acquisition.
    type : foreign key
        Reference to the data type type.
    comment : string

    Properties
    ----------
    timed_files
    n_timed_files
    """
    name = pw.CharField(max_length=64)
    inst = pw.ForeignKeyField(ArchiveInst, related_name='acqs')
    type = pw.ForeignKeyField(AcqType, related_name='acqs')
    comment = pw.TextField(null=True)


class FileType(base_model):
    """A file type.

    Attributes
    ----------
    name : string
        The name of this file type.
    notes: string
        Any notes or comments about this file type.
    """
    name = pw.CharField(max_length=64)
    notes = pw.TextField(null=True)

    # This dict is an atribute on the class used to hold the set of registered
    # handlers. Store as a dictionary for easy lookup of handlers by name.
    _registered_file_types = {}

    @classmethod
    def register_type(cls, file_info):
        """Register a new file type with alpenhorn.

        This creates the entry in the FileType table if it does not already exist.

        Parameters
        ----------
        file_info : FileInfoBase
            FileInfoBase describing the type of acquisition.
        """

        try:
            cls.get(name=file_info._file_type)
        except pw.DoesNotExist:
            log.info("Create FileType entry for \"%s\"" % file_info._file_type)
            cls.create(name=file_info._file_type)

        # Add to registry
        cls._registered_file_types[file_info._file_type] = file_info

    @classmethod
    def check_registration(cls):
        """Check that all FileTypes known to the database have a
        registered handler.
        """

        # Get the list of types names from the database
        db_file_types = [row[0] for row in cls.select(cls.name).tuples()]

        # Get the names of all the registered types
        reg_file_types = cls._registered_file_types.keys()

        # Find any missing types
        missing_file_types = set(db_file_types) - set(reg_file_types)

        if len(missing_file_types):
            raise RuntimeError('FileTypes %s have no registered handler.' %
                               repr(missing_file_types))

    def is_type(self, filename, acq, node):
        """Check if this file can be handled by this file type.

        Parameters
        ----------
        filename : string
            Name of the file.
        acq : ArchiveAcq
            The acquisition the file is in.
        node : StorageNode
            The node we are importing from. Needed so we can inspect the actual
            acquisition.

        Returns
        -------
        is_type : boolean
        """
        return self.file_info._is_type(filename, path.join(node.root, acq.name))

    @classmethod
    def detect(cls, filename, acq, node):
        """Try and find an acquisition type that understands this directory.

        Parameters
        ----------
        filename : string
            Name of the file we are trying to find the type of.
        acq : ArchiveAcq
            The acquisition hosting the file.
        node : StorageNode
            The node we are importing from. Needed so we can inspect the actual
            file.
        """

        # Iterate over all known acquisition types to try and find one that matches
        # the directory being processed
        for file_type in acq.type.file_types:

            if file_type.is_type(filename, acq, node):
                return file_type

        return None

    @property
    def file_info(self):
        """The FileInfo table for this FileType.
        """
        return self.__class__._registered_file_types[self.name]


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
    """
    acq = pw.ForeignKeyField(ArchiveAcq, related_name='files')
    type = pw.ForeignKeyField(FileType, related_name='files')
    name = pw.CharField(max_length=64)
    size_b = pw.BigIntegerField(null=True)
    md5sum = pw.CharField(null=True, max_length=32)


class AcqInfoBase(base_model, ConfigClass):
    """Base class for storing metadata for acquisitions.

    To make a working AcqInfo type you must at a minimum set `_acq_type` to be
    the name of this acquisition type (this is what is used in the `AcqType`
    table), and set `_file_types` to be a list of the file types supported by
    this Acquisition type, as well as provide implementations for `_is_type` and
    `set_info`. Additionally you might want to implement `set_config` to receive
    configuration information.
    """

    _acq_type = None

    _file_types = None

    acq = pw.ForeignKeyField(ArchiveAcq)

    @classmethod
    def new(cls, acq, node):
        """Create a new AcqInfo object.

        Parameters
        ----------
        acq : ArchiveAcq
            The acquisition we are adding metadata for.
        node : StorageNode
            The node we are currently on. Used so we can inspect the actual
            acquisition directory.

        Returns
        -------
        acq_info : AcqInfoBase
            The AcqInfo instance.
        """

        # Create an instance of the metadata class and point it at the
        # acquisition
        acq_info = cls()
        acq_info.acq = acq

        # Call the method on the derived class to set its metadata
        acq_info.set_info(acq.name, node.root)

        # Save the changes and return the AcqInfo object
        acq_info.save()
        return acq_info

    @classmethod
    def _is_type(cls, acq_name, node_root):
        """Check if this acqusition path can be handled by this acquisition type.

        Parameters
        ----------
        acq_name : string
            Path to the acquisition directory.
        node_root : string
            Path to the root of the node containing the acquisition.
        """
        return NotImplementedError()

    @classmethod
    def get_acq_type(cls):
        """Get an instance of the AcqType row corresponding to this AcqInfo.
        """
        return AcqType.get(name=cls._acq_type)

    def set_info(self, acqpath):
        """Set any metadata from the acquisition directory.

        Abstract method, must be implemented in a derived AcqInfo table.

        Parameters
        ----------
        acqpath : string
            Path to the acquisition directory.
        """
        return NotImplementedError()


class FileInfoBase(base_model, ConfigClass):
    """Base class for storing metadata for files.

    To make a working FileInfo type you must at a minimum set `_file_type` to be
    the name of this file type (this is what is used in the `FileType` table),
    as well as provide implementations for `_is_type` and `set_info`.
    Additionally you might want to implement `set_config` to receive
    configuration information.
    """

    _file_type = None

    file = pw.ForeignKeyField(ArchiveFile)

    @classmethod
    def new(cls, file, node):
        """Create a new AcqInfo object.

        Parameters
        ----------
        file : ArchiveFile
            The file we are adding metadata for.
        node : StorageNode
            The node we are currently on. Used so we can inspect the actual
            archive file.

        Returns
        -------
        file_info : FileInfoBase
            The FileInfo instance.
        """

        # Create an instance of the metadata class and point it at the
        # acquisition
        file_info = cls()
        file_info.file = file

        # Call the method on the derived class to set its metadata
        acqpath = path.join(node.root, file.acq.name)
        file_info.set_info(file.name, acqpath)

        # Save the changes and return the FileInfo object
        file_info.save()
        return file_info

    @classmethod
    def get_file_type(cls):
        """Get an instance of the FileType row corresponding to this FileInfo.
        """
        return FileType.get(name=cls._file_type)

    @classmethod
    def _is_type(cls, filename, acq_root):
        """Check if this file can be handled by this file type.

        Parameters
        ----------
        filename : string
            Name of the file.
        acq_root : string
            Path to the root of the the acquisition on the node.
        """
        return NotImplementedError()

    def set_info(self, filename, acq_root):
        """Set any metadata from the file.

        Abstract method, must be implemented in a derived FileInfo table.

        Parameters
        ----------
        filename : string
            Name of the file.
        acq_root : string
            Path to the root of the the acquisition on the node.
        """
        return NotImplementedError()
