"""alpenhorn acq show command."""

import click
import peewee as pw
from tabulate import tabulate

from ...common.util import pretty_bytes
from ...db import ArchiveAcq, ArchiveFile, ArchiveFileCopy, StorageGroup, StorageNode
from ..cli import echo


@click.command()
@click.argument("acq")
@click.option("--show-groups", is_flag=True, help="Show Storage Groups containing ACQ.")
@click.option("--show-nodes", is_flag=True, help="Show Storage Nodes containing ACQ.")
def show(acq, show_groups, show_nodes):
    """Show details of an Acquisition.

    Shows details of the Acquisition named ACQ.
    """

    try:
        acq = ArchiveAcq.get(name=acq)
    except pw.DoesNotExist:
        raise click.ClickException("No such Acquisition: " + acq)

    # File count and size
    totals = (
        ArchiveFile.select(
            pw.fn.COUNT(ArchiveFile.id).alias("count"),
            pw.fn.Sum(ArchiveFile.size_b).alias("size"),
        )
        .where(ArchiveFile.acq == acq)
        .group_by(ArchiveFile.acq)
        .dicts()
    )[0]

    echo("Acquisition: " + acq.name)
    echo(" File Count: " + str(totals["count"]))
    echo(" Total Size: " + pretty_bytes(totals["size"]))

    if show_nodes:
        # If show_groups and show_nodes are True, keys are group names
        # and value are sub-dicts.  If only show_nodes is Ture, keys are
        # node names.  Not set if show_nodes is False.
        node_totals = {}

        query = (
            ArchiveFile.select(
                StorageNode.name.alias("node"),
                StorageNode.group.alias("group"),
                pw.fn.COUNT(ArchiveFile.id).alias("count"),
                pw.fn.Sum(ArchiveFile.size_b).alias("size"),
            )
            .join(ArchiveFileCopy)
            .join(StorageNode)
            .where(ArchiveFile.acq == acq, ArchiveFileCopy.has_file == "Y")
            .group_by(StorageNode.id)
        )
        for row in query.dicts():
            if show_groups:
                group_dict = node_totals.setdefault(row["group"], {})
                group_dict[row["node"]] = (row["count"], pretty_bytes(row["size"]))
            else:
                node_totals[row["node"]] = (row["count"], pretty_bytes(row["size"]))

    if show_groups:
        group_totals = (
            ArchiveFile.select(
                StorageGroup.id.alias("gid"),
                StorageGroup.name.alias("name"),
                pw.fn.COUNT(ArchiveFile.id).alias("count"),
                pw.fn.Sum(ArchiveFile.size_b).alias("size"),
            )
            .join(ArchiveFileCopy)
            .join(StorageNode)
            .join(StorageGroup)
            .where(ArchiveFile.acq == acq, ArchiveFileCopy.has_file == "Y")
            .group_by(StorageGroup.id)
            .order_by(StorageGroup.name)
        )
        if show_nodes:
            name_header = "Group/Node"
        else:
            name_header = "Group"
        echo(f"\n{name_header} breakdown:\n")
        data = []
        for group in group_totals.dicts():
            if not group["count"]:
                continue
            data.append(
                (
                    group["name"],
                    group["count"],
                    pretty_bytes(group["size"]),
                )
            )
            if show_nodes and group["gid"] in node_totals:
                add_blank = False
                for node_name, node_data in node_totals[group["gid"]].items():
                    if node_data[0]:
                        add_blank = True
                        data.append(("-- " + node_name, *node_data))
                if add_blank:
                    data.append(("", "", ""))
        if data:
            echo(tabulate(data, headers=[name_header, "Count", "Size"]))
        else:
            echo("No nodes with data")
    elif show_nodes:
        echo("\nNode breakdown:\n")
        if node_totals:
            node_data = [(node, *node_totals[node]) for node in sorted(node_totals)]
            echo(tabulate(node_data, headers=["Node", "Count", "Size"]))
        else:
            echo("No nodes with data")
