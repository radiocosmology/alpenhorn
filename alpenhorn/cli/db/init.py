"""alpenhorn db init command"""

import click
import peewee as pw

from ...db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
    StorageTransferAction,
    database_proxy,
)


@click.command()
def init():
    """Initialise the Data Index.

    This command will create all the database table required
    by the Alpenhorn Data Index.  Pre-existing tables will not
    be overwritten.

    The tables created here are required for most of alpenhorn's
    functionality.
    """

    # Create any alpenhorn core tables
    core_tables = [
        ArchiveAcq,
        ArchiveFile,
        ArchiveFileCopy,
        ArchiveFileCopyRequest,
        StorageGroup,
        StorageNode,
        StorageTransferAction,
    ]

    database_proxy.create_tables(core_tables, safe=True)
