"""alpenhorn node stats command"""

from __future__ import annotations

from collections import defaultdict

import click
import peewee as pw
from tabulate import tabulate

from ...common.util import pretty_bytes
from ...db import ArchiveFile, ArchiveFileCopy, StorageNode
from ..cli import echo
from ..options import cli_option, resolve_group


def get_stats(nodes: list[StorageNode], extra_stats: bool) -> dict[int, dict]:
    """Generate usage stats for nodes.

    Parameters
    ----------
    nodes:
        a list of StorageNodes to generate stats for
    extra_stats:
        If True also return corrupt/suspect/missing counts

    Returns
    -------
    stats:
        a dict of dicts of stats keyed by node id
    """

    stats = defaultdict(dict)
    for row in (
        StorageNode.select(
            StorageNode.id.alias("id"),
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
    ):
        stats[row["id"]] = row

    # Add the extra stats, if requested
    if extra_stats:
        # We could make this a huge, nasty SQL query
        # by employing multiple subqueries, but I think it's
        # probably more readable if we do it one-by-one, even
        # though that's going to be a bit more work for the client
        # itself

        # Corrupt counts
        for row in (
            StorageNode.select(
                StorageNode.id,
                pw.fn.COUNT(ArchiveFileCopy.id),
            )
            .join(ArchiveFileCopy, pw.JOIN.LEFT_OUTER)
            .where(
                ArchiveFileCopy.has_file == "X",
                ArchiveFileCopy.wants_file == "Y",
                StorageNode.id << nodes,
            )
            .group_by(StorageNode.id)
            .tuples()
        ):
            stats[row[0]]["corrupt"] = row[1]

        # Suspect counts
        for row in (
            StorageNode.select(
                StorageNode.id,
                pw.fn.COUNT(ArchiveFileCopy.id),
            )
            .join(ArchiveFileCopy, pw.JOIN.LEFT_OUTER)
            .where(
                ArchiveFileCopy.has_file == "M",
                ArchiveFileCopy.wants_file == "Y",
                StorageNode.id << nodes,
            )
            .group_by(StorageNode.id)
            .tuples()
        ):
            stats[row[0]]["suspect"] = row[1]

        # Missing counts
        for row in (
            StorageNode.select(
                StorageNode.id,
                pw.fn.COUNT(ArchiveFileCopy.id),
            )
            .join(ArchiveFileCopy, pw.JOIN.LEFT_OUTER)
            .where(
                ArchiveFileCopy.has_file == "N",
                ArchiveFileCopy.wants_file == "Y",
                StorageNode.id << nodes,
            )
            .group_by(StorageNode.id)
            .tuples()
        ):
            stats[row[0]]["missing"] = row[1]

    # Some post-processing
    for node in nodes:
        node_stats = stats[node.id]
        if "count" not in node_stats or not node_stats["count"]:
            stats[node.id]["count"] = 0

        if node_stats.get("size"):
            if node.max_total_gb and node.max_total_gb > 0:
                percent = float(100 * node_stats["size"] / 2**30) / node.max_total_gb
                stats[node.id]["percent"] = f"{percent:5.2f}"
            else:
                stats[node.id]["percent"] = "-"
            stats[node.id]["size"] = pretty_bytes(node_stats["size"])
        else:
            stats[node.id]["size"] = "-"
            stats[node.id]["percent"] = "-"

        if extra_stats:
            if "corrupt" not in node_stats or not node_stats["corrupt"]:
                stats[node.id]["corrupt"] = "-"
            if "suspect" not in node_stats or not node_stats["suspect"]:
                stats[node.id]["suspect"] = "-"
            if "missing" not in node_stats or not node_stats["missing"]:
                stats[node.id]["missing"] = "-"

    return stats


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
        query = query.where(StorageNode.group == resolve_group(group))
    if host:
        query = query.where(StorageNode.host == host)

    # Run the node query
    nodes = list(query.execute())

    # Now fetch stats
    stats = get_stats(nodes, extra_stats)

    # Compose table
    headers = ["Name", "File Count", "Total Size", "% Full"]
    colalign = ["left", "right", "right", "right"]
    if extra_stats:
        headers += ["Corrupt Files", "Suspect Files", "Missing Files"]
        colalign += ["right", "right", "right"]

    # Create table rows
    data = []
    for node in nodes:
        if node.id not in stats:
            if extra_stats:
                data.append((node.name, 0, "-", "-", "-", "-", "-"))
            else:
                data.append((node.name, 0, "-", "-"))
            continue

        node_stats = stats[node.id]

        if extra_stats:
            data.append(
                (
                    node.name,
                    node_stats["count"],
                    node_stats["size"],
                    node_stats["percent"],
                    node_stats["corrupt"],
                    node_stats["suspect"],
                    node_stats["missing"],
                )
            )
        else:
            data.append(
                (
                    node.name,
                    node_stats["count"],
                    node_stats["size"],
                    node_stats["percent"],
                )
            )

    if data:
        echo(tabulate(data, headers=headers, colalign=colalign, floatfmt=".2f"))
    else:
        echo("no nodes")
