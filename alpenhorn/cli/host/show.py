"""alpenhorn host show command"""

import click
from tabulate import tabulate

from ...db import StorageNode
from ..cli import echo
from ..node.stats import get_stats
from ..options import resolve_host


@click.command()
@click.argument("hostname", metavar="HOST")
@click.option("--node-details", is_flag=True, help="Show details of listed nodes.")
@click.option("--node-stats", is_flag=True, help="Show usage stats of listed nodes.")
def show(hostname, node_details, node_stats):
    """Show details of a storage host.

    Shows details of the storage host named HOST.
    """

    host = resolve_host(hostname)

    # Print a report
    echo("Storage Host: " + host.name)
    echo("    Username: " + (host.username if host.username else ""))
    echo("     Address: " + (host.address if host.address else ""))
    echo("       Notes: " + (host.notes if host.notes else ""))

    # List nodes, if any
    echo("\nNodes:\n")
    nodes = list(StorageNode.select().where(StorageNode.host == host))
    if nodes:
        if node_details or node_stats:
            if node_details:
                details = {
                    node.id: (
                        node.name,
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
                headers = ["Name", "Active", "I/O Class"]
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
