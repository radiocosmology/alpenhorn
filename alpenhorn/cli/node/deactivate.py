"""alpenhorn node activate command"""

import click
import peewee as pw

from ...db import database_proxy, StorageNode
from ..cli import echo


@click.command()
@click.argument("name", metavar="NODE")
def deactivate(name):
    """Deactivate a node.

    This deactivates the node named NODE, removing it from the list of nodes
    being actively updated.
    """

    with database_proxy.atomic():
        # Check name
        try:
            node = StorageNode.get(name=name)
        except pw.DoesNotExist:
            raise click.ClickException("no such node: " + name)

        if not node.active:
            echo(f'No change: node "{name}" already inactive.')
            return

        # Update
        StorageNode.update(active=False).where(StorageNode.id == node.id).execute()
        echo(f'Storage node "{name}" deactivated.')