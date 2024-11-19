"""alpenhorn acq create command"""

import click
import peewee as pw

from ...db import database_proxy, ArchiveAcq
from ..options import cli_option
from ..cli import echo


@click.command()
@click.argument("name", metavar="NAME")
def create(name):
    """Create a new Acquisitiop.

    The Acquisition will be called NAME, which must not be the name of
    another existing Acquisition.
    """

    with database_proxy.atomic():
        try:
            ArchiveAcq.get(name=name)
            raise click.ClickException(f'Acquisition "{name}" already exists.')
        except pw.DoesNotExist:
            pass

        ArchiveAcq.create(name=name)
        echo(f'Created acquisition "{name}".')
