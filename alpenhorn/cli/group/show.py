"""alpenhorn group show command"""

import json

import click
import peewee as pw
from tabulate import tabulate

from ...common.util import pretty_bytes
from ...db import (
    ArchiveFile,
    ArchiveFileCopyRequest,
    StorageNode,
    StorageTransferAction,
)
from ..cli import echo
from ..node.stats import get_stats
from ..options import cli_option, resolve_group


@click.command()
@click.argument("group_name", metavar="GROUP")
@click.option(
    "--actions",
    is_flag=True,
    help="Show post-transfer auto-actions affecting this group.",
)
@cli_option("all_", help="Show all additional data.")
@click.option("--node-details", is_flag=True, help="Show details of listed nodes.")
@click.option("--node-stats", is_flag=True, help="Show usage stats of listed nodes.")
@click.option("--transfers", is_flag=True, help="Show pending inbound transfers.")
def show(group_name, actions, all_, node_details, node_stats, transfers):
    """Show details of a storage group.

    Shows details of the storage group named GROUP.
    """

    if all_:
        node_details = True
        node_stats = True
        transfers = True
        actions = True

    group = resolve_group(group_name)

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
                details = {
                    node.id: (
                        node.name,
                        node.host,
                        "Yes" if node.active else "No",
                        node.io_class if node.io_class else "Default",
                    )
                    for node in nodes
                }
            if node_stats:
                stats = get_stats(nodes, False)

            # Make table
            data = []
            if node_stats and node_details:
                headers = [
                    "Name",
                    "Host",
                    "Active",
                    "I/O Class",
                    "File Count",
                    "Total Size",
                    "% Full",
                ]
                for node in nodes:
                    data.append(
                        (
                            *details[node.id],
                            stats[node.id]["count"],
                            stats[node.id]["size"],
                            stats[node.id]["percent"],
                        )
                    )
            elif node_details:
                headers = ["Name", "Host", "Active", "I/O Class"]
                for node in nodes:
                    data.append(details[node.id])
            else:
                headers = ["Name", "File Count", "Total Size", "% Full"]
                for node in nodes:
                    data.append(
                        (
                            node.name,
                            stats[node.id]["count"],
                            stats[node.id]["size"],
                            stats[node.id]["percent"],
                        )
                    )

            echo(tabulate(data, headers=headers))
        else:
            # simple list
            for node in nodes:
                echo("  " + node.name)
    else:
        echo("  none")

    # List transfers, if requested
    if transfers:
        echo("\nPending inbound transfers:\n")

        query = (
            ArchiveFileCopyRequest.select(
                StorageNode.name,
                pw.fn.COUNT(ArchiveFileCopyRequest.id).alias("count"),
                pw.fn.Sum(ArchiveFile.size_b).alias("size"),
            )
            .join(ArchiveFile)
            .switch(ArchiveFileCopyRequest)
            .join(StorageNode)
            .where(
                ArchiveFileCopyRequest.group_to == group,
                ArchiveFileCopyRequest.completed == 0,
                ArchiveFileCopyRequest.cancelled == 0,
            )
            .group_by(ArchiveFileCopyRequest.node_from_id)
            .order_by(StorageNode.name)
        )

        data = []
        for node in query.tuples():
            data.append((node[0], node[1], pretty_bytes(node[2])))

        echo(tabulate(data, headers=["Source Node", "Request Count", "Total Size"]))

    # List auto-actions, if requested
    if actions:
        echo("\nAuto-actions:\n")

        # It's possible for there to be entries in the table with nothing
        # activated.  So filter those out
        query = (
            StorageTransferAction.select()
            .join(StorageNode)
            .where(
                StorageTransferAction.group_to == group,
                (StorageTransferAction.autosync == 1)
                | (StorageTransferAction.autoclean == 1),
            )
            .order_by(StorageTransferAction.node_from_id)
        )

        data = []
        for action in query.execute():
            if action.autoclean:
                data.append(
                    (action.node_from.name, "Auto-clean", "File added to this group")
                )
            if action.autosync:
                data.append(
                    (action.node_from.name, "Auto-sync", "File added to that node")
                )

        if data:
            echo(tabulate(data, headers=["Node", "Action", "Trigger"]))
        else:
            echo("  none")
