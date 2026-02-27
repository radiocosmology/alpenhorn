"""alpenhorn host list command"""

import click
from tabulate import tabulate

from ...db import StorageHost, StorageNode
from ..cli import echo


@click.command()
def list_():
    """List all storage hosts."""

    data = []
    for host in StorageHost.select():
        # Count nodes on host
        node_count = (
            StorageNode.select(StorageNode.id).where(StorageNode.host == host).count()
        )

        # Create table row
        data.append((host.name, host.username, host.address, node_count, host.notes))

    if data:
        echo(tabulate(data, headers=["Name", "Username", "Address", "Nodes", "Notes"]))
    else:
        echo("No storage hosts found.")
