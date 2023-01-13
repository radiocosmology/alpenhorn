"""Acqusition and File Info base classes.

Each AcqType and FileType is associated with an Info class.

All Info classes implement the logic to identify the type
of an acquisition or file from the file's path (see the
is_type() method).  This is used when importing new data
into the data index.

_Most_ Info classes also provide facilities for recording
other metadata to the database, in type-specific Info tables.
Info classes that provide this functionality need to inherit
from the table model base class `alpenhorn.db.base_class`.

Because each entry in the AcqType and FileType tables need
an Info class, and because these types are experiment-specific,
Info classes are, for the most part, not provided with alpenhorn.
All Info classes must subclass from one of the base classes
found here.  There are three levels of base classes implementing
more and more functionality:

    info_base:
        Base-class for all info classes.  Does not implement
        a table model.  Does not distinguish between AcqType and
        FileType.  Types needed a table model should use the
        more appropriate acq_info_base or file_info_base.  Subclasses
        must implement is_type() for type-detection logic.

    acq_info_base and file_info_base:
        Subclassed from both info_base and base_model to connect
        it to a database table to store data.  Subclasses must
        re-implement both is_type(), for type-detection logic, and
        _set_info(), for data gathering.

    GenericAcqInfo and GenericFileInfo:
        Subclassed from acq_info_base or file_info_base.  Adds a
        simple regex or glob matching for is_type().  Subclasses
        must still re-implement _set_info().

This module also provides the inbo_base subclass _NoInfo which has the
same is_type() implementation as GenericAcqInfo and GenericFileInfo but
isn't backed by a table.  This class is used for AcqTypes and FileTypes
whose info_class is None to provide type-detection without info metadata
storage.

All the classes defined in this module, including _NoInfo, must be
subclassed before use.  Putting these base classes directly into the
info_class field of an AcqType or FileType will result in an error.  This
module also provides a class factory no_info() which returns _NoInfo
subclasses.
"""
import json
import logging

import peewee as pw

from .db import base_model
from .acquisition import ArchiveAcq, ArchiveFile

log = logging.getLogger(__name__)


class info_base:
    """Base class for all Info classes.

    Does not implement a table model.  If one is needed, subclass
    from acq_info_base or file_info_base instead.

    Subclasses must re-implement is_type() to define type-detection
    logic."""

    # Class properties
    # The AcqType or FileType instance for this info class
    _type = None
    _config = dict()

    @classmethod
    def has_model(cls):
        """return boolean: is this class backed by a DB table?"""
        return issubclass(cls, base_model)

    @classmethod
    def get_type(cls):
        """Return the associated AcqType or FileType instance."""
        return cls._type

    @classmethod
    def set_config(cls, type_):
        """Set the class config from the type instance.

        Parameters
        ----------
        type : AcqType or FileType
            The AcqType or FileType instance associated with this info class.
        """
        if cls is _NoInfo:
            raise TypeError(
                "cannot call set_config on _NoInfo base class.  Sublass first."
            )

        cls._type = type_

        if type_.info_config is not None:
            cls._config = json.loads(type_.info_config)

    @classmethod
    def is_type(cls, path, *args):
        """Type detection logic.

        Parameters
        ----------

        Subclasses must re-implement this to provide type-detection logic."""
        raise NotImplementedError("must be re-implemented by subclass")

    def __init__(self, *args, **kwargs):
        if self._type is None:
            raise RuntimeError("attempt to instantiate unconfigured info class")

        # Call _set_info, if necessary
        path = kwargs.pop("path_", None)
        if path is not None:
            try:
                node = kwargs.pop("node_")
            except KeyError:
                # Or TypeError?
                raise ValueError("no node_ specified with path_")
            # These are optional
            acqname = kwargs.pop("acqname_", None)
            acqtype = kwargs.pop("acqtype_", None)

            # This is only called if there's a table model backing the
            # info class; i.e. there's something useful to do with the data
            #
            # Info returned is merged into kwargs so the peewee model can
            # ingest it.
            if self.has_model():
                kwargs |= self._set_info(path, node, acqname, acqtype)

        # Continue init
        super().__init__(args, kwargs)

    @property
    def type(self):
        """The name of the associated type"""
        return self._type.name

    def _set_info(self, path, node, acqname, acqtype):
        """generate info metadata for this path.

        An info subclass's _set_info() method is only called if the class
        is backed by a DB table (i.e. has_table() returns True).  Subclasses
        retreive info metadata by inspecting the acqusition or file on disk
        given by `path`.

        Parameters
        ----------
        path : pathlib.Path
            path relative to node.root of the acqusition directory or
            file being imported.
        node : StorageNode
            StorageNode containing the file to be imported.
        acqname : pathlib.Path or None
            For acquisitions, this is None.  For files, this is the
            name of the containing acquisition.
        acqtype : AcqType or None
            For acqusitions, this is None.  For files, this is the
            AcqType of the containing acqusition.

        Any implementation must return a dict containing field data for the
        table.  The dict will be merged into the dict of keywords passed to
        the class constructor, and then passed on to the peewee model
        constructor to populate the new info record being instantiated.

        On error, implementations should raise an appropriate exception.
        """
        raise NotImplementedError("must be re-implemented by subclass")


# See the note below on GenericAcqInfo on the inhertiance ordering
class acq_info_base(info_base, base_model):
    """Base class for storing metadata for acquisitions.

    To make a working AcqInfo type you must at a minimum provide implementations
    for `is_type` and `_set_info`.

    This class defines one model field (others should be added by subclasses):

        - acq : ForeignKey to ArchiveAcq
    """

    acq = pw.ForeignKeyField(ArchiveAcq)

    @classmethod
    def is_type(cls, acq_name, node):
        """Check if this acqusition path can be handled by this acquisition type.

        Parameters
        ----------
        acq_name : pathlib.Path
            Path to the acquisition directory, relative to node.root
        node : StorageNode
            The node containing the acquisition.
        """
        raise NotImplementedError("must be re-implemented by subclass")

    def __init__(self, *args, **kwargs):
        """To instantiate this class from a file on disk (i.e. during import
        by alpenhorn, pass keyword arguments:

        path_ : patlib.Path
            path relative to node_.root for the acq being imported
        node_ : StorageNode
            node containing the acqusition being imported

        Other arguments and keyword arguments are passed to parent
        class initialiser(s).
        """


# See the note below on GenericAcqInfo on the inhertiance ordering
class file_info_base(info_base, base_model):
    """Base class for storing metadata for files.

    To make a working AcqInfo type you must at a minimum provide implementations
    for `is_type` and `_set_info`.

    This class defines one model field (others should be added by subclasses):

        - file : ForeignKey to ArchiveFile
    """

    file = pw.ForeignKeyField(ArchiveFile)

    @classmethod
    def is_type(cls, filename, node, acq_name):
        """Check if this file can be handled by this file type.

        Parameters
        ----------
        filename : string
            Name of the file.
        node : StorageNode
            The node containing the file.
        acq_name : string
            Name of the acquisition.
        """
        raise NotImplementedError("must be re-implemented by subclass")

    def _set_info(self, path, node, acqname, acqpath):
        """Set any metadata from the file.

        Abstract method, must be implemented in a derived FileInfo table.

        Parameters
        ----------
        filename : string
            Name of the file.
        acq_root : string
            Path to the root of the the acquisition on the node.
        """
        raise NotImplementedError("must be re-implemented by subclass")


class _NoInfo(info_base):
    """An info class not backed by a database table.

    This is used as an AcqInfo or FileInfo class stand-in when an
    AcqType or FileType specifies no info_class.

    It is primarily of use with acq/file types which don't store
    additional information in the database (and, as such, need no
    associated info table).  It needs the following keys in the
    info_config JSON object:

    "glob":
        If `True` (default) we should interpret the patterns as globs (use
        the extended syntax of `globre`). Otherwise they are treated as
        regular expressions.

    "patterns":
        A list of regular expressions or globs to match supported paths.

    Never instantiate this class directly.  Use a subclass provided
    by the no_info() class factory.
    """

    _patterns = None
    _glob = None

    @classmethod
    def is_type(cls, path, *args):
        """Check whether the acquisition path matches any patterns we can handle."""
        return cls._check_match(str(path))

    @classmethod
    def set_config(cls, type_):
        """Configure the class from a AcqType or FileType.

        This should be called before creating instances of the class.

        Raises TypeError if called directly on a base class."""

        # Don't pollute base classes
        if cls in [
            info_base,
            _NoInfo,
            acq_info_base,
            file_info_base,
            GenericAcqInfo,
            GenericFileInfo,
        ]:
            raise TypeError(
                f"cannot call set_config on base class `{cls}`.  Sublass first."
            )

        super().set_config(type_)
        cls._glob = cls._config.get("glob", True)
        cls._patterns = cls._config.get("patterns", list())

    @classmethod
    def _check_match(cls, name):
        # Check for initialisation
        if cls._patterns is None:
            raise RuntimeError("_check_match called on unconfigured class")

        # Get the match function to use depending on whether globbing is enabled.
        if cls._glob:
            import globre

            matchfn = globre.match
        else:
            import re

            matchfn = re.match

        # Loop over patterns and check for matches
        for pattern in cls._patterns:

            if matchfn(pattern, name):
                return True

        return False


def no_info():
    """A class factory returning _NoInfo subclasses."""

    class NoInfo(_NoInfo):
        pass

    return NoInfo


# These two classes have a diamond inheritance, e.g.:
#
#    GenericAcqInfo
#     /       \
# _NoInfo  acq_info_base
#     \       /       \
#     info_base    base_model
#                      |
#                   pw.Model
#
# The peewee Model class doesn't support multiple inheritance, so
# it needs to be at the end of the MRO to get the super() chaining to
# work correctly.  (If it were earlier, it's methods would simply
# consume all arguments and not call the method of the next class in
# the MRO.)
class GenericAcqInfo(_NoInfo, acq_info_base):
    """An AcqInfo base class providing a simple detection scheme based
    on matching against the acquisition name.  It needs the following
    keys in the info_config provided by the corresponding AcqType entry:

    "glob":
        If `True` (default) we should interpret the patterns as globs (use
        the extended syntax of `globre`). Otherwise they are treated as
        regular expressions.

    "patterns":
        A list of regular expressions or globs to match supported
        acquisitions."""

    # Without any redefinition, we would end up using _NoInfo.is_type
    # anyways, but it's better to redefine this with the acq_info_base
    # calling signature.
    @classmethod
    def is_type(cls, acq_name, node):
        return _NoInfo.is_type(cls, acq_name, node)

    # Also use the acq_info_base docstring
    is_type.__doc__ = acq_info_base.__doc__


class GenericFileInfo(_NoInfo, file_info_base):
    """A FileInfo base class providing a simple detection scheme based
    on matching against the file path.  It needs the following keys in
    the info_config provided by the corresponding FileType entry:

    "glob":
        If `True` (default) we should interpret the patterns as globs (use
        the extended syntax of `globre`). Otherwise they are treated as
        regular expressions.

    "patterns":
        A list of regular expressions or globs to match supported files."""

    # Use file_info_base signature with _NoInfo body.  See the comment
    # above for GenericAcqInfo.is_type().
    @classmethod
    def is_type(cls, filename, node, acq_name):
        return _NoInfo.is_type(cls, filename, node, acqname)

    # Also use the file_info_base docstring
    is_type.__doc__ = file_info_base.__doc__
