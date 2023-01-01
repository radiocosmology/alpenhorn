"""Acqusition and File info base classes."""
import json
import logging

import peewee as pw

from .db import base_model
from .acquisition import ArchiveAcq, ArchiveFile

log = logging.getLogger(__name__)


class info_base:
    """Base class for all Info classes.

    Does not implement a table model."""

    # Class properties
    # The AcqType or FileType instance for this info class
    _type = None
    _config = dict()

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
        cls._type = type_

        if type_.info_config is not None:
            cls._config = json.loads(type_.info_config)

    @property
    def type(self):
        """The name of the associated type"""
        return self._type.name


class acq_info_base(base_model, info_base):
    """Base class for storing metadata for acquisitions.

    To make a working AcqInfo type you must at a minimum set `_acq_type` to be
    the name of this acquisition type (this is what is used in the `AcqType`
    table), and set `_file_types` to be a list of the file types supported by
    this Acquisition type, as well as provide implementations for `is_type` and
    `set_info`. Additionally you might want to implement `set_config` to receive
    configuration information.
    """

    acq = pw.ForeignKeyField(ArchiveAcq)

    @classmethod
    def is_type(cls, acq_name, node):
        """Check if this acqusition path can be handled by this acquisition type.

        Parameters
        ----------
        acq_name : string
            Path to the acquisition directory.
        node : StorageNode
            The node containing the acquisition.
        """
        raise NotImplementedError()

    def set_info(self, acqpath, node):
        """Set any metadata from the acquisition directory.

        Abstract method, must be implemented in a derived AcqInfo table.

        Parameters
        ----------
        acqpath : string
            Path to the acquisition directory.
        node_root : string
            Path to the root directory for data in the node we are currently on.
        """
        raise NotImplementedError()


class file_info_base(base_model, info_base):
    """Base class for storing metadata for files.

    To make a working FileInfo type you must at a minimum set `_file_type` to be
    the name of this file type (this is what is used in the `FileType` table),
    as well as provide implementations for `is_type` and `set_info`.
    Additionally you might want to implement `set_config` to receive
    configuration information.
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
        raise NotImplementedError()

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
        raise NotImplementedError()


class NoInfo(info_base):
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
        A list of regular expressions or globs to match supported
        acquisitions.

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
        """Configure the class from a act_type or file_type."""

        # Don't pollute the base class
        if cls is NoInfo:
            raise TypeError(
                "cannot call set_config on NoInfo base class.  Sublass first."
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

    def set_info(self, *args):
        """Does nothing."""


def no_info():
    """A class factory returning NoInfo subclasses."""

    class _NoInfo(NoInfo):
        pass

    return _NoInfo
