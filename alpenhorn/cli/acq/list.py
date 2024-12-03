"""alpenhorn acq list command."""

import click
import peewee as pw
from tabulate import tabulate

from ...db import ArchiveAcq, ArchiveFile, ArchiveFileCopy, StorageGroup, StorageNode
from ..cli import echo
from ..options import cli_option, not_both


@click.command()
@cli_option(
    "group",
    help="Limit to acquisitions with files existing on Storage Group named "
    "GROUP.  In this case, only files in GROUP are counted.",
)
@cli_option(
    "node",
    help="Limit to acquisitions with files existing on Storage Node named "
    "NODE.  In this case, only files on NODE are counted.",
)
def list_(group, node):
    """List acquisitions."""

    not_both(node, "node", group, "group")

    query = (
        ArchiveAcq.select(ArchiveAcq.name, pw.fn.COUNT(ArchiveFile.id))
        .join(ArchiveFile, pw.JOIN.LEFT_OUTER)
        .group_by(ArchiveAcq.id)
    )

    if group:
        try:
            group = StorageGroup.get(name=group)
        except pw.DoesNotExist:
            raise click.ClickException("No such group: " + group)

        query = (
            query.join(ArchiveFileCopy)
            .join(StorageNode)
            .where(StorageNode.group == group, ArchiveFileCopy.has_file == "Y")
        )

    elif node:
        try:
            node = StorageNode.get(name=node)
        except pw.DoesNotExist:
            raise click.ClickException("No such node: " + node)

        query = query.join(ArchiveFileCopy).where(
            ArchiveFileCopy.node == node, ArchiveFileCopy.has_file == "Y"
        )

    data = query.tuples()

    if data:
        echo(tabulate(data, headers=["Name", "Files"]))
    else:
        echo("No acquisitions")
