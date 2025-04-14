"""Database connection and models

This module abstracts the database connection, providing a minimally
functional fallback if no external database module has been provided.

More capable database connectors may be provided by a database extension
module.  The dict returned by the register_extension() call to a database
extension module must contain a "database" key whose value is a second dict
with keys providing the database extensions capabilities.

The following keys are allowed in the "database" dict, all of which are
optional:
    - "reentrant" : boolean
            If True, the database extension is re-entrant (threadsafe), and
            simultaneous independant connections to the database will be
            made to it.  False is assumed if not given.
    - "connect" : callable
            Invoked to create a database connection.  Will be passed a dict
            containing the contents of the "database" section of the
            alpenhorn config as the keyword parameter "config".  Must
            return a `pw.Database`.  Should raise `pw.OperationalError` if
            a connection could not be established.  If not given, the
            `_connect()` function in this module will be called instead.
    - "close" : callable
            Invoked when closing the database connection.

Before accessing the attributes of this module, `connect()` must be called
once, from the MainThread, to set up the database.  After that function is
called, the following attributes are available:

- threadsafe: a boolean indicating whether the database can be concurrently
            accessed from multiple threads.

After `connect()` has been called, database access is possible, from any
thread, typically via the peewee table models provided by this module.
"""

# Table models
from .acquisition import ArchiveAcq, ArchiveFile
from .data_index import DataIndexVersion, current_version, schema_version
from .archive import ArchiveFileCopy, ArchiveFileCopyRequest, ArchiveFileImportRequest
from .storage import StorageGroup, StorageNode, StorageTransferAction

# Basic functionality
from ._base import connect, close, database_proxy, threadsafe

# Prototypes
from ._base import EnumField, base_model

# Naive-UTC stuff courtesy peewee.  These were originally in datetime
# but were deprecated in 3.12 as too confusing.
from peewee import utcnow, utcfromtimestamp

# This contains all tables in the Data Index
gamut = (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    ArchiveFileImportRequest,
    DataIndexVersion,
    StorageGroup,
    StorageNode,
    StorageTransferAction,
)
