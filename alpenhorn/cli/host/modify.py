"""alpenhorn host modify command"""

import click

from ...db import StorageHost, database_proxy
from ..cli import echo, update_or_remove
from ..options import (
    cli_option,
    resolve_host,
)


@click.command()
@click.argument("hostname", metavar="HOST")
@cli_option("address")
@cli_option("notes")
@cli_option("username")
def modify(hostname, address, notes, username):
    """Modify a Storage Host.

    This modifies metadata for the Storage Host named HOST, updating field
    specified in the options.

    NB: to rename a host, use the "host rename" command.
    """

    with database_proxy.atomic():
        host = resolve_host(hostname)

        updates = {}

        # Find updated fields
        updates |= update_or_remove("address", address, host.address)
        updates |= update_or_remove("notes", notes, host.notes)
        updates |= update_or_remove("username", username, host.username)

        if updates:
            StorageHost.update(**updates).where(StorageHost.id == host.id).execute()
            echo("Host updated.")
        else:
            echo("No change.")
