"""alpenhorn file verify command."""

import click
import peewee as pw

from ...db import ArchiveFileCopy, database_proxy
from ..cli import echo
from ..options import file_from_path, resolve_node


@click.command()
@click.argument("path", metavar="FILE")
@click.argument("node_name", metavar="NODE")
@click.pass_context
def verify(ctx, path, node_name):
    """Request verification of a File.

    This command requests that the copy of FILE on NODE be re-verified to
    check it for corruption.  FILE should be specified as
    "<acq_name>/<file_name>".

    If there is no copy of FILE on NODE, an error is returned.
    """
    with database_proxy.atomic():
        file = file_from_path(path)
        node = resolve_node(node_name)

        # Find the existing record, if any
        try:
            copy = ArchiveFileCopy.get(file=file, node=node)
        except pw.DoesNotExist:
            raise click.ClickException("File not present on node.")

        # We allow verify requests on missing files
        if copy.has_file == "N" and copy.wants_file != "Y":
            raise click.ClickException("File not present on node.")

        if copy.has_file == "M":
            # Nothing to do.
            echo("File already scheduled for verification.")
            ctx.exit()

        # Verbiage
        if copy.has_file == "N":
            descriptor = "missing "
        elif copy.has_file == "X":
            descriptor = "corrupt "
        else:
            descriptor = ""

        # Update
        ArchiveFileCopy.update(has_file="M").where(
            ArchiveFileCopy.id == copy.id
        ).execute()

    echo(f'Requesting verification of {descriptor}file "{path}".')
