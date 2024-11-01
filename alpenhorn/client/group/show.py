"""alpenhorn group show command"""

import json
import click
import peewee as pw
from tabulate import tabulate

from ...db import StorageGroup, StorageNode
from ..cli import echo


@click.command()
@click.argument("group_name", metavar="GROUP")
@click.option("--node-details", is_flag=True, help="Show details of listed nodes.")
@click.option("--node-stats", is_flag=True, help="Show usage stats of listed nodes.")
def show(group_name, node_details, node_stats):
    """Show details of a storage group.

    Shows details of the storage group named GROUP.
    """

    try:
        group = StorageGroup.get(name=group_name)
    except pw.DoesNotExist:
        raise click.ClickException(f"no such group: {group_name}")

    # Print a report
    echo("Storage Group: " + group.name)
    echo("        Notes: " + (group.notes if group.notes else ""))
    echo("    I/O Class: " + (group.io_class if group.io_class else "Default"))

    echo("\nI/O Config:\n")
    if group.io_config:
        try:
            io_config = json.loads(group.io_config)
            if io_config:
                # Find length of longest key (but not too long)
                keylen = min(max([len(key) for key in io_config]), 30)
                for key, value in io_config.items():
                    echo("  " + key.rjust(keylen) + ": " + str(value))
            else:
                echo("  empty")
        except json.JSONDecodeError:
            echo("INVALID (JSON decode error)")
    else:
        echo("  none")

    # List nodes, if any
    echo("\nNodes:\n")
    nodes = list(StorageNode.select().where(StorageNode.group == group))
    if nodes:
        if node_details or node_stats:
            if node_details:
                data = [
                    (
                        node.name,
                        node.host,
                        "Yes" if node.active else "No",
                        node.io_class if node.io_class else "Default",
                    )
                    for node in nodes
                ]
                headers = ["Name", "Host", "Active", "I/O Class"]
            if node_stats:
                # TODO: add --node-stats support when "alpenhorn node stats" is implemented
                raise NotImplementedError()
            echo(tabulate(data, headers=headers))
        else:
            # simple list
            for node in nodes:
                echo("  " + node.name)
    else:
        echo("  none")
