"""alpenhorn node stats command"""

import click
import peewee as pw
from tabulate import tabulate

from ...db import StorageGroup, StorageNode, ArchiveFile, ArchiveFileCopy
from ...common.util import pretty_bytes
from ..options import cli_option
from ..cli import echo


@click.command()
@click.option(
    "--active/--inactive",
    help="List only active/inactive nodes.",
    is_flag=True,
    default=None,
)
@cli_option("group", help="List only nodes in Storage Group GROUP.")
@cli_option("host", help="List only nodes on HOST.", metavar="HOST")
@click.option("--extra-stats", help="Show extra stats.", is_flag=True)
def stats(active, group, host, extra_stats):
    """Show Storage Node stats.

    Options can be used to restrict which nodes are selected for
    display.

    For each selected node, shows total number of files, total size,
    and percentage full (if the node has a well defined total size).

    If --extra-stats are requested, also shows counts of corrupt,
    missing, and suspect files."""

    # Frist find all the nodes.  This takes care of the user
    # selection limit
    query = StorageNode.select()

    # Apply limits
    if active is not None:
        query = query.where(StorageNode.active == active)
    if group:
        try:
            group = StorageGroup.get(name=group)
        except pw.DoesNotExist:
            raise click.ClickException("no such group: " + group)
        query = query.where(StorageNode.group == group)
    if host:
        query = query.where(StorageNode.host == host)

    # Run the node query
    nodes = list(query.execute())

    # Now fetch stats
    stats = {
        row["id"]: row
        for row in StorageNode.select(
            StorageNode.id,
            pw.fn.COUNT(ArchiveFileCopy.id).alias("count"),
            pw.fn.Sum(ArchiveFile.size_b).alias("size"),
        )
        .join(ArchiveFileCopy, pw.JOIN.LEFT_OUTER)
        .join(ArchiveFile, pw.JOIN.LEFT_OUTER)
        .where(
            ArchiveFileCopy.has_file == "Y",
            ArchiveFileCopy.wants_file == "Y",
            StorageNode.id << nodes,
        )
        .group_by(StorageNode.id)
        .dicts()
    }

    # Compose table
    headers = ["Name", "File Count", "Total Size", "% Full"]
    colalign = ["left", "right", "right", "right"]
    data = []
    for node in nodes:
        if node.id not in stats:
            data.append((node.name, 0, "-", "-"))
            continue

        node_stats = stats[node.id]
        if node_stats["size"]:
            size = pretty_bytes(node_stats["size"])
            if node.max_total_gb:
                percent = 100.0 * node_stats["size"] / node.max_total_gb / 2**30
                percent = f"{percent:5.2f}"
            else:
                percent = "-"
        else:
            size = "-"
            percent = "-"
        data.append((node.name, node_stats["count"], size, percent))

    # Add the extra stats, if requested
    if extra_stats:
        headers += ["Corrupt Files", "Suspect Files", "Missing Files"]
        colalign += ["right", "right", "right"]

        # We could make this a huge, nasty SQL query
        # by employing multiple subqueries, but I think it's
        # probably more readable if we do it one-by-one, even
        # though that's going to be a bit more work for the CLI
        # itself

        # Corrupt counts
        corrupt = {
            row[0]: row[1]
            for row in (
                StorageNode.select(
                    StorageNode.name,
                    pw.fn.COUNT(ArchiveFileCopy.id),
                )
                .join(ArchiveFileCopy, pw.JOIN.LEFT_OUTER)
                .where(
                    ArchiveFileCopy.has_file == "X",
                    ArchiveFileCopy.wants_file == "Y",
                    StorageNode.id << nodes,
                )
                .group_by(StorageNode.id)
            ).tuples()
        }
        suspect = {
            row[0]: row[1]
            for row in (
                StorageNode.select(
                    StorageNode.name,
                    pw.fn.COUNT(ArchiveFileCopy.id),
                )
                .join(ArchiveFileCopy, pw.JOIN.LEFT_OUTER)
                .where(
                    ArchiveFileCopy.has_file == "M",
                    ArchiveFileCopy.wants_file == "Y",
                    StorageNode.id << nodes,
                )
                .group_by(StorageNode.id)
            ).tuples()
        }
        missing = {
            row[0]: row[1]
            for row in (
                StorageNode.select(
                    StorageNode.name,
                    pw.fn.COUNT(ArchiveFileCopy.id),
                )
                .join(ArchiveFileCopy, pw.JOIN.LEFT_OUTER)
                .where(
                    ArchiveFileCopy.has_file == "N",
                    ArchiveFileCopy.wants_file == "Y",
                    StorageNode.id << nodes,
                )
                .group_by(StorageNode.id)
            ).tuples()
        }

        old_data = data
        data = []
        echo(f"C: {corrupt}")
        for row in old_data:
            data.append(
                (
                    *row,
                    corrupt.get(row[0], "-"),
                    suspect.get(row[0], "-"),
                    missing.get(row[0], "-"),
                )
            )

    if data:
        echo(tabulate(data, headers=headers, colalign=colalign, floatfmt=".2f"))
    else:
        echo("no nodes")
