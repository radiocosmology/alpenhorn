"""alpenhorn node show command"""

import json
import click
import peewee as pw
from tabulate import tabulate

from ...common.util import pretty_bytes
from ...db import StorageGroup, StorageNode, StorageTransferAction
from ..cli import echo
from .stats import get_stats


@click.command()
@click.argument("name", metavar="NAME")
@click.option(
    "--actions",
    is_flag=True,
    help="Show post-transfer auto-actions affecting this group.",
)
@click.option("all_", "--all", "-a", is_flag=True, help="Show all additional data.")
@click.option("--stats", is_flag=True, help="Show usage stats of the node.")
def show(name, actions, all_, stats):
    """Show details of a Storage Node.

    Shows details of the Storage Node named NODE.
    """

    if all_:
        actions = True
        stats = True

    try:
        node = StorageNode.get(name=name)
    except pw.DoesNotExist:
        raise click.ClickException(f"no such node: {name}")

    if node.storage_type == "A":
        type_name = "Archive"
    elif node.storage_type == "T":
        type_name = "Transport"
    else:
        type_name = "-"

    if node.max_total_gb:
        max_total = pretty_bytes(node.max_total_gb * 2**30)
    else:
        max_total = "-"

    if node.min_avail_gb:
        min_avail = pretty_bytes(node.min_avail_gb * 2**30)
    else:
        min_avail = "-"

    if node.avail_gb:
        avail = pretty_bytes(node.avail_gb * 2**30)
    else:
        avail = "-"

    if node.avail_gb_last_checked:
        last_checked = node.avail_gb_last_checked.ctime() + " UTC"
    else:
        last_checked = "???"

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
    echo("   Last Checked: " + last_checked)

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

    # List transfer actions, if requested
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