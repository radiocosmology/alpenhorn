"""alpenhorn node show command"""

import json

import click
import peewee as pw
from tabulate import tabulate

from ...common.util import pretty_bytes
from ...db import (
    ArchiveFile,
    ArchiveFileCopyRequest,
    ArchiveFileImportRequest,
    StorageGroup,
    StorageTransferAction,
)
from ..cli import echo, pretty_time
from ..options import cli_option, resolve_node
from .stats import get_stats


@click.command()
@click.argument("name", metavar="NAME")
@click.option(
    "--actions",
    is_flag=True,
    help="Show post-transfer auto-actions affecting this group.",
)
@cli_option("all_", help="Show all additional data.")
@click.option(
    "--imports", is_flag=True, help="Show pending import requests for the node."
)
@click.option("--stats", is_flag=True, help="Show usage stats of the node.")
@click.option(
    "--transfers", is_flag=True, help="Show pending transfers out from the node."
)
def show(name, actions, all_, imports, stats, transfers):
    """Show details of a Storage Node.

    Shows details of the Storage Node named NODE.
    """

    if all_:
        actions = True
        imports = True
        stats = True
        transfers = True

    node = resolve_node(name)

    if node.storage_type == "A":
        type_name = "Archive"
    elif node.storage_type == "T":
        type_name = "Transport"
    else:
        type_name = "-"

    if node.max_total_gb and node.max_total_gb > 0:
        max_total = pretty_bytes(node.max_total_gb * 2**30)
    else:
        max_total = "-"

    if node.min_avail_gb and node.min_avail_gb > 0:
        min_avail = pretty_bytes(node.min_avail_gb * 2**30)
    else:
        min_avail = "-"

    if node.avail_gb and node.avail_gb > 0:
        avail = pretty_bytes(node.avail_gb * 2**30)
    else:
        avail = "-"

    # Print a report
    echo("   Storage Node: " + node.name)
    echo("  Storage Group: " + node.group.name)
    echo("         Active: " + ("Yes" if node.active else "No"))
    echo("           Type: " + type_name)
    echo("          Notes: " + (node.notes if node.notes else ""))
    echo("      I/O Class: " + (node.io_class if node.io_class else "Default"))
    echo()
    echo("    Daemon Host: " + (node.host if node.host else ""))
    echo(" Log-in Address: " + (node.address if node.address else ""))
    echo("Log-in Username: " + (node.username if node.username else ""))
    echo()
    echo("    Auto-Import: " + ("On" if node.auto_import else "Off"))
    echo(
        "    Auto-Verify: "
        + (f"On (Size: {node.auto_verify})" if node.auto_verify else "Off")
    )
    echo("      Max Total: " + max_total)
    echo("      Available: " + avail)
    echo("  Min Available: " + min_avail)
    echo("   Last Checked: " + pretty_time(node.avail_gb_last_checked))

    echo("\nI/O Config:\n")
    if node.io_config:
        try:
            io_config = json.loads(node.io_config)
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

    if stats:
        stats = get_stats([node], False)[node.id]

        echo("\nStats:\n")
        echo("    Total Files: " + str(stats["count"]))
        echo("     Total Size: " + stats["size"])
        echo("          Usage: " + stats["percent"].lstrip() + "%")

    # List imports, if requested
    if imports:
        echo("\nPending import requests:\n")

        query = (
            ArchiveFileImportRequest.select()
            .where(
                ArchiveFileImportRequest.node == node,
                ArchiveFileImportRequest.completed == 0,
            )
            .order_by(ArchiveFileImportRequest.timestamp)
        )

        inits = []
        reqs = []
        for req in query.execute():
            if req.path == "ALPENHORN_NODE":
                # Handle node init requests separately
                inits.append(("[Node Init]", "-", "-", req.timestamp))
            else:
                reqs.append(
                    (
                        req.path,
                        "Yes" if req.recurse else "No",
                        "Yes" if req.register else "No",
                        req.timestamp,
                    )
                )

        # The node init requests are always put at the top of the table
        echo(
            tabulate(
                inits + reqs, headers=["Path", "Scan", "Register New", "Request Time"]
            )
        )

    # List transfers, if requested
    if transfers:
        echo("\nPending outbound transfers:\n")

        query = (
            ArchiveFileCopyRequest.select(
                StorageGroup.name,
                pw.fn.COUNT(ArchiveFileCopyRequest.id).alias("count"),
                pw.fn.Sum(ArchiveFile.size_b).alias("size"),
            )
            .join(ArchiveFile)
            .switch(ArchiveFileCopyRequest)
            .join(StorageGroup)
            .where(
                ArchiveFileCopyRequest.node_from == node,
                ArchiveFileCopyRequest.completed == 0,
                ArchiveFileCopyRequest.cancelled == 0,
            )
            .group_by(ArchiveFileCopyRequest.group_to_id)
            .order_by(StorageGroup.name)
        )

        data = []
        for group in query.tuples():
            data.append((group[0], group[1], pretty_bytes(group[2])))

        echo(tabulate(data, headers=["Dest. Group", "Request Count", "Total Size"]))

    # List auto-actions, if requested
    if actions:
        echo("\nAuto-actions:\n")

        # It's possible for there to be entries in the table with nothing
        # activated.  So filter those out
        query = (
            StorageTransferAction.select()
            .join(StorageGroup)
            .where(
                StorageTransferAction.node_from == node,
                (StorageTransferAction.autosync == 1)
                | (StorageTransferAction.autoclean == 1),
            )
            .order_by(StorageTransferAction.group_to_id)
        )

        data = []
        for action in query.execute():
            if action.autoclean:
                data.append(
                    (action.group_to.name, "Auto-clean", "File added to that group")
                )
            if action.autosync:
                data.append(
                    (action.group_to.name, "Auto-sync", "File added to this node")
                )

        if data:
            echo(tabulate(data, headers=["Group", "Action", "Trigger"]))
        else:
            echo("  none")
