"""alpenhorn acq files command."""

import click
import peewee as pw
from tabulate import tabulate

from ...common.util import pretty_bytes
from ...db import ArchiveAcq, ArchiveFile, ArchiveFileCopy, StorageGroup, StorageNode
from ..cli import echo
from ..options import cli_option, not_both


@click.command()
@click.argument("acq", metavar="ACQ")
@cli_option("group")
@cli_option("node")
@click.option(
    "--show-removed",
    is_flag=True,
    help=(
        "If used with --node or --group, also list files "
        "that have been removed from the node/group."
    ),
)
def files(acq, group, node, show_removed):
    """List files in an Acqusition.

    Lists files in the Acqusition named ACQ."""

    not_both(node, "node", group, "group")

    try:
        acq = ArchiveAcq.get(name=acq)
    except pw.DoesNotExist:
        raise click.ClickException("No such Acquisition: " + acq)

    data = []
    if node:
        headers = ["Name", "Size", "Node State", "Size on Node", "MD5"]

        try:
            node = StorageNode.get(name=node)
        except pw.DoesNotExist:
            raise click.ClickException("No such Storage Node: " + node)
        query = (
            ArchiveFileCopy.select()
            .join(ArchiveFile)
            .where(ArchiveFile.acq == acq, ArchiveFileCopy.node == node)
        )

        for copy in query:
            state = copy.state
            if show_removed or state != "Removed":
                data.append(
                    (
                        copy.file.name,
                        pretty_bytes(copy.file.size_b),
                        state,
                        pretty_bytes(copy.size_b),
                        copy.file.md5sum,
                    )
                )
    elif group:
        headers = ["Name", "Size", "Group State", "Node", "MD5"]

        try:
            group = StorageGroup.get(name=group)
        except pw.DoesNotExist:
            raise click.ClickException("No such Storage Group: " + group)
        query = (
            ArchiveFileCopy.select()
            .join(ArchiveFile)
            .switch(ArchiveFileCopy)
            .join(StorageNode)
            .where(ArchiveFile.acq == acq, StorageNode.group == group)
            .group_by(ArchiveFile.id)
        )

        for copy in query:
            state, node = group.state_on_node(copy.file)

            if show_removed or state != "N":
                # Convert state to words
                if state == "Y":
                    state = "Present"
                elif state == "M":
                    state = "Needs Check"
                elif state == "N":
                    state = "Removed"
                else:
                    state = "Corrupt"

                data.append(
                    (
                        copy.file.name,
                        pretty_bytes(copy.file.size_b),
                        state,
                        "-" if node is None else node.name,
                        copy.file.md5sum,
                    )
                )
    else:
        headers = ["Name", "Size", "MD5"]

        query = ArchiveFile.select().where(ArchiveFile.acq == acq)

        for file in query:
            data.append((file.name, pretty_bytes(file.size_b), file.md5sum))

    if data:
        echo(tabulate(data, headers=headers))
    else:
        echo("No files.")
