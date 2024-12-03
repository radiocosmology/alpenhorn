"""alpenhorn node activate command"""

import click

from ...db import StorageNode, database_proxy
from ..cli import echo, update_or_remove
from ..options import cli_option, resolve_node


@click.command()
@click.argument("name", metavar="NODE")
@cli_option("address", help="Set the node address to ADDR before activation.")
@cli_option("host", help="Set the node host to HOST before activation.")
@cli_option("root", help="Set the node root to ROOT before activation.")
@cli_option("username", help="Set the node username to USER before activation.")
def activate(name, address, host, root, username):
    """Activate a node.

    This activates the node named NODE, marking it for update by an alpenhorn
    server running on the node's host.

    The node's host, address, root, and username may be changed before activation
    by using the appropriate option, if necessary.
    """

    with database_proxy.atomic():
        node = resolve_node(name)

        # If the node is already active, we don't do an update, even if different
        # metadata was specified
        if node.active:
            echo(f'No change: node "{name}" already active.')
            return

        # collect the updated parameters
        updates = {"active": True}
        updates |= update_or_remove("address", address, node.address)
        updates |= update_or_remove("host", host, node.host)
        updates |= update_or_remove("root", root, node.root)
        updates |= update_or_remove("username", username, node.username)

        # Update
        StorageNode.update(**updates).where(StorageNode.id == node.id).execute()
        echo(f'Storage node "{name}" activated.')
