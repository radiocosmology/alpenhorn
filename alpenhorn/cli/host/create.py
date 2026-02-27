"""alpenhorn host create command"""

import click
import peewee as pw

from ...db import StorageHost, database_proxy
from ..cli import echo
from ..options import (
    cli_option,
)


@click.command()
@click.argument("hostname", metavar="NAME")
@cli_option("address")
@cli_option("notes")
@cli_option("username")
def create(hostname, address, notes, username):
    """Create a new Storage Host.

    The Storage Host will be called NAME, which must not already exist.  This
    is a _logical_ value used to match against a daemon's "host" value.  It
    need not be related to the hosts actual hostname.
    """

    with database_proxy.atomic():
        # Check name
        try:
            StorageHost.get(name=hostname)
            raise click.ClickException(f'host "{hostname}" already exists.')
        except pw.DoesNotExist:
            pass

        StorageHost.create(
            name=hostname, address=address, notes=notes, username=username
        )

    echo(f'Created storage host "{hostname}".')
