"""alpenhorn host rename command"""

import click
import peewee as pw

from ...db import StorageHost, database_proxy
from ..cli import echo


@click.command()
@click.argument("hostname", metavar="HOST")
@click.argument("new_name", metavar="NEW_NAME")
def rename(hostname, new_name):
    """Rename a storage host.

    The existing storage host named HOST will be renamed to NEW_NAME.
    NEW_NAME must not already be the name of another host.
    """

    if hostname == new_name:
        # The easy case
        echo("No change.")
        return

    with database_proxy.atomic():
        try:
            StorageHost.get(name=new_name)
            raise click.ClickException(f'Storage host "{hostname}" already exists.')
        except pw.DoesNotExist:
            pass

        count = (
            StorageHost.update(name=new_name)
            .where(StorageHost.name == hostname)
            .execute()
        )

    if not count:
        raise click.ClickException("no such host: " + hostname)

    echo(f'Storage host "{hostname}" renamed to "{new_name}".')
