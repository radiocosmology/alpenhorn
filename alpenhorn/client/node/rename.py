"""alpenhorn node rename command"""

import click
import peewee as pw

from ...db import StorageNode, database_proxy
from ..cli import echo


@click.command()
@click.argument("node_name", metavar="NODE")
@click.argument("new_name", metavar="NEW_NAME")
def rename(node_name, new_name):
    """Rename a storage node.

    The existing storage node named NODE will be renamed to NEW_NAME.
    NEW_NAME must not already be the name of another node.
    """

    if node_name == new_name:
        # The easy case
        echo("No change.")
        return

    with database_proxy.atomic():
        try:
            StorageNode.get(name=new_name)
            raise click.ClickException(f'Storage node "{node_name}" already exists.')
        except pw.DoesNotExist:
            pass

        count = (
            StorageNode.update(name=new_name)
            .where(StorageNode.name == node_name)
            .execute()
        )

    if not count:
        raise click.ClickException("no such node: " + node_name)

    echo(f'Storage node "{node_name}" renamed to "new_name".')
