"""Alpenhorn client interface for operations on `ArchiveAcq`s."""

import re
import sys

import click
import peewee as pw

from ..db import ArchiveAcq, ArchiveFile, ArchiveFileCopy, StorageGroup, StorageNode

RE_LOCK_FILE = re.compile(r"^\..*\.lock$")


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Commands operating on archival data products. Use to list acquisitions, their contents, and locations of copies."""


@cli.command(name="list")
@click.argument("node_name", required=False)
def acq_list(node_name):
    """List known acquisitions. With NODE specified, list acquisitions with files on NODE."""
    config_connect()

    import tabulate

    if node_name:
        try:
            node = StorageNode.get(name=node_name)
        except pw.DoesNotExist:
            print("No such storage node:", node_name)
            sys.exit(1)

        query = (
            ArchiveFileCopy.select(ArchiveAcq.name, pw.fn.count(ArchiveFileCopy.id))
            .join(ArchiveFile)
            .join(ArchiveAcq)
            .where(ArchiveFileCopy.node == node)
            .group_by(ArchiveAcq.id)
        )
    else:
        query = (
            ArchiveAcq.select(ArchiveAcq.name, pw.fn.COUNT(ArchiveFile.id))
            .join(ArchiveFile, pw.JOIN.LEFT_OUTER)
            .group_by(ArchiveAcq.name)
        )

    data = query.tuples()

    if data:
        print(tabulate.tabulate(data, headers=["Name", "Files"]))
    else:
        print("No matching acquisitions")


@cli.command()
@click.argument("acquisition")
@click.argument("node_name", required=False)
def files(acquisition, node_name):
    """List files that are in the ACQUISITION. With NODE specified, list acquisitions with files on NODE."""
    config_connect()

    import tabulate

    try:
        acq = ArchiveAcq.get(name=acquisition)
    except pw.DoesNotExist:
        print("No such acquisition:", acquisition)
        sys.exit(1)

    if node_name:
        try:
            node = StorageNode.get(name=node_name)
        except pw.DoesNotExist:
            print("No such storage node:", node_name)
            sys.exit(1)

        query = (
            ArchiveFile.select(
                ArchiveFile.name,
                ArchiveFileCopy.size_b,
                ArchiveFileCopy.has_file,
                ArchiveFileCopy.wants_file,
            )
            .join(ArchiveFileCopy)
            .where(
                ArchiveFile.acq == acq,
                ArchiveFileCopy.node == node,
            )
        )
        headers = ["Name", "Size", "Has", "Wants"]
    else:
        query = ArchiveFile.select(
            ArchiveFile.name, ArchiveFile.size_b, ArchiveFile.md5sum
        ).where(ArchiveFile.acq_id == acq.id)
        headers = ["Name", "Size", "MD5"]

    data = query.tuples()

    if data:
        print(tabulate.tabulate(data, headers=headers))
    else:
        print("No registered archive files.")


@cli.command()
@click.argument("acquisition")
def where(acquisition):
    """List locations of files that are in the ACQUISITION."""
    config_connect()

    import tabulate

    try:
        acq = ArchiveAcq.get(name=acquisition)
    except pw.DoesNotExist:
        print("No such acquisition:", acquisition)
        sys.exit(1)

    nodes = (
        StorageNode.select()
        .join(ArchiveFileCopy)
        .join(ArchiveFile)
        .where(ArchiveFile.acq == acq)
        .distinct()
    ).execute()
    if not nodes:
        print("No registered archive files.")
        return

    for node in nodes:
        print("Storage node:", node.name)
        query = (
            ArchiveFile.select(
                ArchiveFile.name,
                ArchiveFileCopy.size_b,
                ArchiveFileCopy.has_file,
                ArchiveFileCopy.wants_file,
            )
            .join(ArchiveFileCopy)
            .where(
                ArchiveFile.acq == acq,
                ArchiveFileCopy.node == node,
            )
        )
        headers = ["Name", "Size", "Has", "Wants"]
        data = query.tuples()
        print(tabulate.tabulate(data, headers=headers))
        print()


@cli.command()
@click.argument("acquisition")
@click.argument("source_node")
@click.argument("destination_group")
def syncable(acquisition, source_node, destination_group):
    """List all files that are in the ACQUISITION that still need to be moved to DESTINATION_GROUP and are available on SOURCE_NODE."""
    config_connect()

    import tabulate

    try:
        acq = ArchiveAcq.get(name=acquisition)
    except pw.DoesNotExist:
        print("No such acquisition:", acquisition)

    try:
        src = StorageNode.get(name=source_node)
    except pw.DoesNotExist:
        print("No such storage node:", source_node)
        sys.exit(1)

    try:
        dest = StorageGroup.get(name=destination_group)
    except pw.DoesNotExist:
        print("No such storage group:", destination_group)
        sys.exit(1)

    # First get the nodes at the destination...
    nodes_at_dest = StorageNode.select().where(StorageNode.group == dest)

    # Then use this to get a list of all files at the destination...
    files_at_dest = (
        ArchiveFile.select()
        .join(ArchiveFileCopy)
        .where(
            ArchiveFile.acq == acq,
            ArchiveFileCopy.node << nodes_at_dest,
            ArchiveFileCopy.has_file == "Y",
        )
    )

    # Then combine to get all file(copies) that are available at the source but
    # not at the destination...
    query = (
        ArchiveFile.select(
            ArchiveFile.name,
            ArchiveFile.size_b,
        )
        .where(ArchiveFile.acq == acq)
        .join(ArchiveFileCopy)
        .where(
            ArchiveFileCopy.node == src,
            ArchiveFileCopy.has_file == "Y",
            ~(ArchiveFile.id << files_at_dest),
        )
    )

    data = query.tuples()

    if data:
        print(tabulate.tabulate(data, headers=["Name", "Size"]))
    else:
        print(
            "No files to copy from node '",
            source_node,
            "' to group '",
            destination_group,
            "'.",
            sep="",
        )
