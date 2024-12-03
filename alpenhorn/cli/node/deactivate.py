"""alpenhorn node activate command"""

import click

from ...db import StorageNode, database_proxy
from ..cli import echo
from ..options import resolve_node


@click.command()
@click.argument("name", metavar="NODE")
def deactivate(name):
    """Deactivate a node.

    This deactivates the node named NODE, removing it from the list of nodes
    being actively updated.
    """

    with database_proxy.atomic():
        node = resolve_node(name)

        if not node.active:
            echo(f'No change: node "{name}" already inactive.')
            return

        # Update
        StorageNode.update(active=False).where(StorageNode.id == node.id).execute()
        echo(f'Storage node "{name}" deactivated.')
