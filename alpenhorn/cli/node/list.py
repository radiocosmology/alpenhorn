"""alpenhorn node list command"""

import click
import peewee as pw
from tabulate import tabulate

from ...db import StorageGroup, StorageNode
from ..options import cli_option
from ..cli import echo


@click.command()
@click.option(
    "--active/--inactive",
    help="list only active/inactive nodes.",
    is_flag=True,
    default=None,
)
@cli_option("group", help="List only nodes in Storage Group GROUP.")
@cli_option("host", help="List only nodes on HOST.", metavar="HOST")
def list_(active, group, host):
    """List Storage Nodes."""

    roles = {"A": "archive", "F": "-", "T": "transport"}

    data = []
    nodes = StorageNode.select().join(StorageGroup)

    # Apply limits
    if active is not None:
        nodes = nodes.where(StorageNode.active == active)
    if group:
        try:
            group = StorageGroup.get(name=group)
        except pw.DoesNotExist:
            raise click.ClickException("no such group: " + group)
        nodes = nodes.where(StorageNode.group == group)
    if host:
        nodes = nodes.where(StorageNode.host == host)

    # Format rows
    for node in nodes:
        data.append(
            (
                node.name,
                node.group.name,
                roles.get(node.storage_type, "???"),
                node.io_class,
                node.host,
                "Yes" if node.active else "No",
                node.root,
                node.notes,
            )
        )

    if data:
        headers = [
            "Name",
            "Group",
            "Role",
            "I/O Class",
            "Host",
            "Active",
            "Root",
            "Notes",
        ]

        echo(tabulate(data, headers=headers))
    else:
        echo("no nodes")