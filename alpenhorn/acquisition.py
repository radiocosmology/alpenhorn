from os import path

import peewee as pw
from .db import base_model
from .config import ConfigClass

# Setup the logging
from . import logger
log = logger.get_log()

# Internal lists of registered acq and file types
_registered_acq_types = []
_registered_file_types = []


def register_acq_type(acqinfo):
    """Register a new acquisition type with alpenhorn.

    This creates the entry in the AcqType table if it does not already exist.

    Parameters
    ----------
    acqinfo : AcqInfoBase
        AcqInfoBase describing the type of acquisition.
    """

    try:
        AcqType.get(name=acqinfo.acq_type)
    except pw.DoesNotExist:
        log.info("Create AcqType entry for \"%s\"" % acqinfo.acq_type)
        AcqType.create(name=acqinfo.acq_type)

    _registered_acq_types.append(acqinfo)


def register_file_type(fileinfo):
    """Register a new file type with alpenhorn.

    This creates the entry in the FileType table if it does not already exist.

    Parameters
    ----------
    fileinfo : FileInfoBase
        FileInfoBase describing the type of file.
    """

    try:
        FileType.get(name=fileinfo.file_type)
    except pw.DoesNotExist:
        log.info("Create FileType entry for \"%s\"" % fileinfo.file_type)

        FileType.create(name=fileinfo.file_type)

    _registered_file_types.append(fileinfo)


def check_registration():
    """Check that all AcqTypes and FileTypes known to the database have a
    registered handler.
    """

    # Get the list of types names from the database
    db_acqtypes = [row[0] for row in AcqType.select(AcqType.name).tuples()]
    db_filetypes = [row[0] for row in FileType.select(FileType.name).tuples()]

    # Get the names of all the registered types
    reg_acqtypes = [acqinfo.acq_type for acqinfo in _registered_acq_types]
    reg_filetypes = [fileinfo.file_type for fileinfo in _registered_file_types]

    # Find any missing types
    missing_acqtypes = set(db_acqtypes) - set(reg_acqtypes)
    missing_filetypes = set(db_filetypes) - set(reg_filetypes)

    if len(missing_acqtypes):
        raise RuntimeError('Acq types %s have no registered handler.' %
                           repr(missing_acqtypes))

    if len(missing_filetypes):
        raise RuntimeError('File types %s have no registered handler.' %
                           repr(missing_filetypes))


def resolve_file_types(filetypes):

    """Take a list specifying FileTypes and resolve them all to classes.

    Parameters
    ----------
    filetypes : list
        List of FileTypes specified as either FileInfoBase classes or
        strings giving the name of the FileType.

    Returns
    -------
    filetype_classes : list
        A list of FileInfoBase classes.
    """

    _filetype_lookup = { ft.file_type: ft for ft in _registered_file_types }

    def _resolve(x):

        if isinstance(x, type) and issubclass(x, FileInfoBase):
            return x
        else:
            try:
                return _filetype_lookup[x]
            except KeyError:
                raise RuntimeError('File type \"%s\" not registered in %s' %
                                   (repr(x), _filetype_lookup.keys()))

    return [_resolve(ft) for ft in filetypes]


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


def dispatch_acq_type(acq_name, node):
    """Try and find an acquisition type that understands this directory.

    Parameters
    ----------
    acq_name : string
        Name of the acquisition we are trying to find the type of.
    node : StorageNode
        The node we are importing from. Needed so we can inspect the actual
        acquisition.
    """

    # Iterate over all known acquisition types to try and find one that matches
    # the directory being processed
    for acq_type in _registered_acq_types:

        if acq_type.is_type(acq_name, node.root):
            return acq_type

    return None


def dispatch_file_type(filename, acqinfo, node):
    """Try and find an acquisition type that understands this directory.

    Parameters
    ----------
    filename : string
        Name of the acquisition we are trying to find the type of.
    acqinfo : AcqInfoBase
        The extended acquisition information.
    node : StorageNode
        The node we are importing from. Needed so we can inspect the actual
        acquisition.
    """

    file_types = resolve_file_types(acqinfo.file_types)

    acq_root = path.join(node.root, acqinfo.acq.name)

    # Iterate over all known acquisition types to try and find one that matches
    # the directory being processed
    for file_type in file_types:

        if file_type.is_type(filename, acq_root):
            return file_type

    return None


class AcqInfoBase(base_model, ConfigClass):
    """Base class for storing metadata for acquisitions.

    To make a working AcqInfo type you must at a minimum set `acq_type` to be
    the name of this acquisition type (this is what is used in the `AcqType`
    table), and set `file_types` to be a list of the file types supported by
    this Acquisition type, as well as provide implementations for `is_type` and
    `set_info`. Additionally you might want to implement `set_config` to receive
    configuration information.
    """

    acq_type = None

    file_types = None

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
    def is_type(cls, acq_name, node_root):
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
    def get_acqtype(cls):
        """Get an instance of the AcqType row corresponding to this AcqInfo.
        """
        return AcqType.get(name=cls.acq_type)

    def set_info(self, acqpath):
        """Set any metadata from the acquisition directory.

        Abstract method, must be implemented in a derived AcqInfo table.

        Parameters
        ----------
        acqpath : string
            Path to the acquisition directory.
        """
        return NotImplementedError()


class GenericAcqInfo(AcqInfoBase):
    """A generic acquisition type that can handle acquisitions matching specific
    patterns, but doesn't keep track of any metadata.
    """

    acq_type = 'generic'

    file_types = ['generic']

    patterns = None

    @classmethod
    def set_config(cls, configdict):
        """Set configuration options for this acquisition type.

        There are two supported options: `patterns`, a list of regular
        expressions to match supported acquisitions, and `file_types`, a list of
        the supported file types within this acquisition.
        """
        # Extract patterns to process from a section of the config file

        cls.patterns = configdict['patterns']

        if 'file_types' in configdict:
            cls.file_types = configdict['file_types']

    def set_info(self, acq_name, node_root):
        """Generic acquisition type has no metadata, so just return.
        """
        return

    @classmethod
    def is_type(cls, acq_name, node_root):
        """Check whether the acquisition path matches any patterns we can handle.
        """

        # Loop over patterns and check for matches
        for pattern in cls.patterns:

            import re

            if re.match(pattern, acq_name):
                return True

        return False


class FileInfoBase(base_model, ConfigClass):
    """Base class for storing metadata for acquisitions.

    To make a working AcqInfo type you must at a minimum set `acq_type` to be
    the name of this acquisition type (this is what is used in the `AcqType`
    table), and set `file_types` to be a list of the file types supported by
    this Acquisition type, as well as provide implementations for `is_type` and
    `set_info`. Additionally you might want to implement `set_config` to receive
    configuration information.
    """

    file_type = None

    file = pw.ForeignKeyField(ArchiveFile)

    @classmethod
    def new(cls, file, node):
        """Create a new AcqInfo object.

        Parameters
        ----------
        file : ArchiveFile
            The acquisition we are adding metadata for.
        node : StorageNode
            The node we are currently on. Used so we can inspect the actual
            archive file.

        Returns
        -------
        file_info : AcqInfoBase
            The AcqInfo instance.
        """

        # Create an instance of the metadata class and point it at the
        # acquisition
        file_info = cls()
        file_info.file = file

        # Call the method on the derived class to set its metadata
        acqpath = path.join(node.root, file.acq.name)
        file_info.set_info(file.name, acqpath)

        # Save the changes and return the AcqInfo object
        file_info.save()
        return file_info

    @classmethod
    def get_filetype(cls):
        """Get an instance of the AcqType row corresponding to this AcqInfo.
        """
        return FileType.get(name=cls.file_type)

    @classmethod
    def is_type(cls, filename, acq_root):
        """Check if this acqusition path can be handled by this acquisition type.
        """
        return NotImplementedError()

    def set_info(self, filename, acq_root):
        """Set any metadata from the acquisition directory.

        Abstract method, must be implemented in a derived AcqInfo table.

        Parameters
        ----------
        acqdir : string
            Path to the acquisition directory.
        """
        return NotImplementedError()


class GenericFileInfo(FileInfoBase):
    """A generic file type that cen be configured to match a pattern, but stores no
    metadata.
    """

    file_type = 'generic'

    patterns = None

    @classmethod
    def set_config(cls, configdict):
        """Set the configuration information.

        The only supported entry is `patterns`, a list of regular expressions
        matching supported files.

        Parameters
        ----------
        configdict : dict
            Dictionary of configuration options.
        """
        cls.patterns = configdict['patterns']

    @classmethod
    def is_type(cls, filename, acq_root):
        """Check whether the acquisition path matches any patterns we can handle.
        """

        # Loop over patterns and check for matches
        for pattern in cls.patterns:

            import re

            if re.match(pattern, filename):
                return True

        return False

    def set_info(self, filename, acq_root):
        """This file type has no meta data so this method does nothing.
        """
        pass
