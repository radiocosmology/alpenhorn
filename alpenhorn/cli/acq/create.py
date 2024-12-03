"""alpenhorn acq create command"""

import click
import peewee as pw

from ...common.util import invalid_import_path
from ...db import ArchiveAcq, database_proxy
from ..cli import echo


@click.command()
@click.argument("name", metavar="NAME")
def create(name):
    """Create a new Acquisitiop.

    The Acquisition will be called NAME, which must not be the name of
    another existing Acquisition.
    """

    # Validate
    rejection_reason = invalid_import_path(name)
    if rejection_reason:
        raise click.ClickException(f"invalid name: {rejection_reason}")

    with database_proxy.atomic():
        try:
            ArchiveAcq.get(name=name)
            raise click.ClickException(f'Acquisition "{name}" already exists.')
        except pw.DoesNotExist:
            pass

        ArchiveAcq.create(name=name)
        echo(f'Created acquisition "{name}".')
