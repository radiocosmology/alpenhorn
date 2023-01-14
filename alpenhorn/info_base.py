"""Acqusition and File Info base classes.

Each AcqType and FileType is associated with an info class.

All info classes implement the logic to identify the type
of an acquisition or file from the file's path (see the
is_type() method).  This is used when importing new data
into the data index.

_Most_ info classes also provide facilities for recording
other metadata to the database, in type-specific info tables.
Info classes that provide this functionality need to inherit
from the table model base class `alpenhorn.db.base_class`.

Because each entry in the AcqType and FileType tables need
an info class, and because these types are experiment-specific,
Info classes are, for the most part, not provided with alpenhorn.
All info classes must subclass from one of the base classes
found here.  There are three levels of base classes implementing
more and more functionality:

    info_base:
        Base-class for all info classes.  Does not implement
        a table model.  Does not distinguish between AcqType and
        FileType.  Types needing a table model should use the
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

This module also provides the info_base subclass _NoInfo which has the
same is_type() implementation as GenericAcqInfo and GenericFileInfo but
isn't backed by a table.  The _NoINfo class is subclassed for use with
AcqTypes and FileTypes whose info_class is None, to provide
type-detection without info metadata storage.

All the classes defined in this module, including _NoInfo, must be
subclassed before use.  Putting these base classes directly into the
info_class field of an AcqType or FileType will result in an error.  This
module also provides a class factory no_info() which returns _NoInfo
subclasses.
"""
import re
import json
import globre
import logging

import peewee as pw

from .db import base_model
from .acquisition import ArchiveAcq, ArchiveFile

log = logging.getLogger(__name__)


class info_base:
    """Base class for all info classes.

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
    def type(cls):
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

        cls._type = type_

        if type_.info_config is not None:
            cls._config = json.loads(type_.info_config)

    @classmethod
    def is_type(cls, path, node, acqtype=None, acqname=None):
        """type detection logic.

        Parameters
        ----------
        path : pathlib.Path
            The path (name) of the acqusition or file.  For acquisitions,
            this is relative to node.root.  For files, it's relative to
            the acqusition directory provided in `acqname`.
        node : StorageNode
            The node containing the file being imported
        acqtype : AcqType or None
            For acquisitions, this is None.  For files, this is the type
            of the acquisition containing this file.
        acqname : pathlib.Path or None
            For acquisitions, this is None.  For files, this is the
            acqusition path relative to node.root.

        Subclasses must re-implement this to provide type-detection logic.

        Implementations should return a True value if the item specified by
        `path` is of this class's associated type or False otherwise.

        Implementations _may_ access data on disk if necessary to perform
        type-detection, but remember that this method is called from the
        alpenhorn main loop, potentially several times per imported object,
        so expensive I/O operations should be avoided.  To access a file
        on the node, don't open() the path directly; use node.io.open().

        For files, implementations should _not_ assume that an ArchiveAcq
        record exists for the acquistion called `acqname`.
        """
        raise NotImplementedError("must be re-implemented by subclass")

    def __init__(self, *args, **kwargs):
        """initialise an instance.

        Initialisation will fail if the class method set_config() has not
        been called to set up the class beforehand.

        In subclasses that implement a table model (ie. when self.has_model()
        returns True), the values of the keyword parameters: "path_", "node_",
        "acqname_", and "acqtype_" will be passed to the method _set_info().
        The dict returned by _set_info() will be merged with the list of
        keyword parameters (after removing the four keywords listed above).
        These merged keyword parameters will be passed to the next initialiser
        in the method resolition order and will, eventually, be consumed as
        field values by the peewee.Model initialiser.

        This _set_info() call is only performed if a "path_" keyword is
        provided.  The trailing underscore on these keywords prevent the
        potential for clashes with field names of the underlying table.

        In subclasses _not_ implementing a table model, the four keywords
        are still removed from the parameter list before passing the parameters
        to the next method in the resolution order, but in this case a call
        to _set_info() is not performed."""

        if self._type is None:
            raise RuntimeError("attempt to instantiate unconfigured info class")

        # Remove keywords we consume
        path = kwargs.pop("path_", None)
        node = kwargs.pop("node_", None)
        acqname = kwargs.pop("acqname_", None)
        acqtype = kwargs.pop("acqtype_", None)
        # Call _set_info, if necessary
        # This is only called if there's a table model backing the
        # info class; i.e. there's something useful to do with the data
        #
        # Info returned is merged into kwargs so the peewee model can
        # ingest it.
        if path is not None and self.has_model():
            if node is None:
                raise ValueError("no node_ specified with path_")

            kwargs |= self._set_info(path, node, acqtype, acqname)

        # Continue init
        super().__init__(*args, **kwargs)

    @property
    def type_name(self):
        """The name of the associated type.

        Equivalent to type().name
        """
        return self._type.name

    def _set_info(self, path, node, acqtype, acqname):
        """generate info table field data for this path.

        This method is only called on subclasses which are backed by
        a DB table (i.e. has_model() returns True).  Calls to this
        method occur as part of object initialisation during an
        __init__ call.

        Parameters
        ----------
        path : pathlib.Path
            path relative to node.root of the acqusition directory or
            file being imported.
        node : StorageNode
            StorageNode containing the file to be imported.
        acqtype : AcqType or None
            For acqusitions, this is None.  For files, this is the
            AcqType of the containing acqusition.
        acqname : pathlib.Path or None
            For acquisitions, this is None.  For files, this is the
            name of the containing acquisition.

        Subclasses with a table model must re-implement this method
        to generate metadata by inspecting the acqusition or file on disk
        given by `path`.  To access a file on the node, don't open() the
        path directly; use `node.io.open(path)`.

        Any implementation must return a dict containing field data for the
        table.  The dict will passed to the base_model and used to populate
        the fields of the new info record being created.

        For acquisitions, implementations should _not_ provide a value for
        the `acq` field.  For files, implementations should _not_ provide
        a value for the `file` field.  Appropriate values for these fields
        will be inserted by alpenhorn after those records are created.

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


# See the note below on GenericAcqInfo on the inhertiance ordering
class file_info_base(info_base, base_model):
    """Base class for storing metadata for files.

    To make a working AcqInfo type you must at a minimum provide implementations
    for `is_type` and `_set_info`.

    This class defines one model field (others should be added by subclasses):

        - file : ForeignKey to ArchiveFile
    """

    file = pw.ForeignKeyField(ArchiveFile)


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
    def is_type(cls, path, node, acqtype=None, acqname=None):
        """type-detection logic.

        Performs pattern matching on "path" per values of the
        config parameters "glob" and "patterns".  Returns
        a boolean indicating whether pattern matching was
        successful.

        Parameters `node`, `acqtype`, and `acqname` are ignored.
        """
        return cls._check_match(str(path))

    @classmethod
    def set_config(cls, type_):
        """Configure the class from a AcqType or FileType.

        This should be called before creating instances of the class.

        Raises TypeError if called directly on a base class."""

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
            matchfn = globre.match
        else:
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
#     \       /     \
#     info_base  base_model
#                    |
#                 pw.Model
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
