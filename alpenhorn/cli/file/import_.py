"""alpenhorn file import command."""

import pathlib

import click

from ...db import ArchiveFileImportRequest, database_proxy
from ..cli import echo
from ..options import resolve_node


@click.command()
@click.argument("path", metavar="PATH")
@click.argument("node_name", metavar="NODE")
@click.option(
    "--register-new",
    "-n",
    help="If path points to a previously-unknown file, add it to the Data Index "
    "instead of ignoring it.",
    is_flag=True,
)
def import_(path, node_name, register_new):
    """Import a File on a Node.

    Requests the daemon import the file at PATH onto NODE.  The PATH may not
    be absolute.  It is assumed to be relative to the Node's root path.

    By default, only files already known to alpenhorn are imported, but you can
    tell the daemon to create a new file record for an previously-unknown file
    at PATH by using the "--register-new" flag.
    """

    # Check path
    if pathlib.PurePath(path).is_absolute():
        raise click.UsageError("PATH may not be absolute")

    with database_proxy.atomic():
        node = resolve_node(node_name)

        ArchiveFileImportRequest.create(
            node=node, path=path, recurse=False, register=register_new, complete=False
        )

    echo("Added new import request.")
