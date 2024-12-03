"""alpenhorn file show command."""

import click
import peewee as pw
from tabulate import tabulate

from ...common.util import pretty_bytes
from ...db import ArchiveFileCopy, ArchiveFileCopyRequest, StorageGroup, StorageNode
from ..cli import echo, pretty_time
from ..options import cli_option, file_from_path


@click.command()
@click.argument("path", metavar="FILE")
@cli_option("all_", help="Show all additional data.")
@click.option("--groups", is_flag=True, help="show file status in Storage Groups")
@click.option("--nodes", is_flag=True, help="show file status on Storage Nodes")
@click.option("--transfers", is_flag=True, help="show file transfer requests")
def show(path, all_, groups, nodes, transfers):
    """Show details of a File.

    Show details of the Archive File FILE, which should
    be specified as "<acq_name>/<file_name>".
    """

    if all_:
        groups = True
        nodes = True
        transfers = True

    # Find the file given the pathspec; doesn't return if file couldn't be found
    file_ = file_from_path(path)

    echo("       Name: " + file_.name)
    echo("Acquisition: " + file_.acq.name)
    echo("       Path: " + file_.acq.name + "/" + file_.name)
    echo()
    echo(
        "       Size: "
        + ("unknown" if file_.size_b is None else pretty_bytes(file_.size_b))
    )
    echo("   MD5 Hash: " + ("-" if file_.md5sum is None else file_.md5sum.lower()))
    echo(" Registered: " + pretty_time(file_.registered))

    if groups:
        echo("\nGroup status\n")

        # "Absent" could be "missing" or "removed".  Groups don't tell us.
        states = {"Y": "Present", "M": "Suspect", "X": "Corrupt", "N": "Absent"}

        # Find groups containing file
        data = []
        for group in (
            StorageGroup.select()
            .join(StorageNode, pw.JOIN.LEFT_OUTER)
            .join(ArchiveFileCopy, pw.JOIN.LEFT_OUTER)
            .where(ArchiveFileCopy.file == file_)
            .distinct()
            .execute()
        ):
            state, node = group.state_on_node(file_)
            data.append(
                (group.name, states.get(state, "Unknown"), node.name if node else "-")
            )

        if data:
            echo(tabulate(data, headers=["Group", "State", "Node"]))
        else:
            echo("No extant copies.")

    if nodes:
        echo("\nNode status:\n")

        data = []
        for copy in (
            ArchiveFileCopy.select()
            .join(StorageNode)
            .where(ArchiveFileCopy.file == file_)
            .execute()
        ):
            # Only print size if it makes sense
            if copy.has_file in ["Y", "M"]:
                size = pretty_bytes(copy.size_b)
            else:
                size = "-"
            data.append((copy.node.name, copy.state, size))

        if data:
            echo(tabulate(data, headers=["Node", "State", "Size on Node"]))
        else:
            echo("No extant copies.")

    if transfers:
        echo("\nTransfer requests:\n")
        data = []
        for req in (
            ArchiveFileCopyRequest.select()
            .join(StorageGroup)
            .switch(ArchiveFileCopyRequest)
            .join(StorageNode)
            .where(ArchiveFileCopyRequest.file == file_)
            .execute()
        ):
            if req.completed:
                status = "Complete"
            elif req.cancelled:
                status = "Cancelled"
            else:
                status = "Pending"

            data.append(
                (
                    req.node_from.name,
                    req.group_to.name,
                    status,
                    pretty_time(req.timestamp),
                    pretty_time(req.transfer_started),
                    pretty_time(req.transfer_completed),
                )
            )

        if data:
            echo(
                tabulate(
                    data,
                    headers=[
                        "Source Node",
                        "Dest. Group",
                        "Status",
                        "Request Time",
                        "Start Time",
                        "Completion Time",
                    ],
                )
            )
        else:
            echo("No transfers")
