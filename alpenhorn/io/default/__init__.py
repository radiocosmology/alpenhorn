"""Default I/O class.

This module implements Alpenhorn's "Default" I/O class,
which is the I/O class used for StorageNodes and
StorageGroups which don't specify an I/O class.

The Default Node I/O class provides support for a
StorageNode backed by a "normal" POSIX filesystem.
The Default Group I/O class provides support for a
StorageGroup containing a single StorageNode.
"""

# These must be imported here to make this a valid Alpenhorn I/O module.
from .group import DefaultGroupIO
from .node import DefaultNodeIO
from .remote import DefaultNodeRemote

# The rest of these import simplify other I/O classes re-using
# Default I/O methods
from .check import check_async
from .delete import delete_async, remove_filedir
from .pull import pull_async
from .updownlock import UpDownLock
