"""Alpenhorn client interface for operations on `ArchiveAcq`s."""

import click
from collections import defaultdict
import os
import peewee as pw
import re
import sys

from alpenhorn import db

import alpenhorn.acquisition as ac
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.util as util

from .connect_db import config_connect

RE_LOCK_FILE = re.compile(r'^\..*\.lock$')


@click.group(context_settings={'help_option_names': ['-h', '--help']})
def cli():
    """Commands operating on archival data products. Use to list acquisitions, their contents, and locations of copies."""
    pass


@cli.command(name='list')
@click.argument('node_name', required=False)
def acq_list(node_name):
    """List known acquisitions. With NODE specified, list acquisitions with files on NODE.
    """
    config_connect()

    import tabulate

    if node_name:
        try:
            node = st.StorageNode.get(name=node_name)
        except pw.DoesNotExist:
            print("No such storage node:", node_name)
            sys.exit(1)

        query = (
            ar.ArchiveFileCopy.select(
                ac.ArchiveAcq.name,
                pw.fn.count(ar.ArchiveFileCopy.id))
            .join(ac.ArchiveFile)
            .join(ac.ArchiveAcq)
            .where(ar.ArchiveFileCopy.node == node)
            .group_by(ac.ArchiveAcq.id)
        )
    else:
        query = (
            ac.ArchiveAcq.select(
                ac.ArchiveAcq.name,
                pw.fn.COUNT(ac.ArchiveFile.id))
            .join(ac.ArchiveFile, pw.JOIN.LEFT_OUTER)
            .group_by(ac.ArchiveAcq.name)
        )

    data = query.tuples()

    if data:
        print(tabulate.tabulate(data, headers=['Name', 'Files']))
    else:
        print("No matching acquisitions")

@cli.command()
@click.argument('acquisition')
@click.argument('node_name', required=False)
def files(acquisition, node_name):
    """List files that are in the ACQUISITION. With NODE specified, list acquisitions with files on NODE.
    """
    config_connect()

    import tabulate

    try:
        acq = ac.ArchiveAcq.get(name=acquisition)
    except pw.DoesNotExist:
        print("No such acquisition:", acquisition)
        sys.exit(1)

    if node_name:
        try:
            node = st.StorageNode.get(name=node_name)
        except pw.DoesNotExist:
            print("No such storage node:", node_name)
            sys.exit(1)

        query = (
            ac.ArchiveFile.select(
                ac.ArchiveFile.name,
                ar.ArchiveFileCopy.size_b,
                ar.ArchiveFileCopy.has_file,
                ar.ArchiveFileCopy.wants_file,
            )
            .join(ar.ArchiveFileCopy)
            .where(
                ac.ArchiveFile.acq == acq,
                ar.ArchiveFileCopy.node == node,
            )
        )
        headers = ['Name', 'Size', 'Has', 'Wants']
    else:
        query = (
            ac.ArchiveFile.select(
                ac.ArchiveFile.name,
                ac.ArchiveFile.size_b,
                ac.ArchiveFile.md5sum)
            .where(ac.ArchiveFile.acq_id == acq.id)
        )
        headers = ['Name', 'Size', 'MD5']

    data = query.tuples()

    if data:
        print(tabulate.tabulate(data, headers=headers))
    else:
        print("No registered archive files.")


@cli.command()
@click.argument('acquisition')
def where(acquisition):
    """List locations of files that are in the ACQUISITION.
    """
    config_connect()

    import tabulate

    try:
        acq = ac.ArchiveAcq.get(name=acquisition)
    except pw.DoesNotExist:
        print("No such acquisition:", acquisition)
        sys.exit(1)

    nodes = (
        st.StorageNode.select()
        .join(ar.ArchiveFileCopy)
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.acq == acq)
        .distinct()
    ).execute()
    if not nodes:
        print("No registered archive files.")
        return

    for node in nodes:
        print("Storage node:", node.name)
        query = (
            ac.ArchiveFile.select(
                ac.ArchiveFile.name,
                ar.ArchiveFileCopy.size_b,
                ar.ArchiveFileCopy.has_file,
                ar.ArchiveFileCopy.wants_file,
            )
            .join(ar.ArchiveFileCopy)
            .where(
                ac.ArchiveFile.acq == acq,
                ar.ArchiveFileCopy.node == node,
            )
        )
        headers = ["Name", "Size", "Has", "Wants"]
        data = query.tuples()
        print(tabulate.tabulate(data, headers=headers))
        print()


@cli.command()
@click.argument('acquisition')
@click.argument('source_node')
@click.argument('destination_group')
def syncable(acquisition, source_node, destination_group):
    """List all files that are in the ACQUISITION that still need to be moved to DESTINATION_GROUP and are available on SOURCE_NODE.
    """
    config_connect()

    import tabulate

    try:
        acq = ac.ArchiveAcq.get(name=acquisition)
    except pw.DoesNotExist:
        print("No such acquisition:", acquisition)

    try:
        src = st.StorageNode.get(name=source_node)
    except pw.DoesNotExist:
        print("No such storage node:", source_node)
        sys.exit(1)

    try:
        dest = st.StorageGroup.get(name=destination_group)
    except pw.DoesNotExist:
        print("No such storage group:", destination_group)
        sys.exit(1)

    # First get the nodes at the destination...
    nodes_at_dest = st.StorageNode.select().where(st.StorageNode.group == dest)

    # Then use this to get a list of all files at the destination...
    files_at_dest = ac.ArchiveFile.select().join(ar.ArchiveFileCopy).where(
        ac.ArchiveFile.acq == acq,
        ar.ArchiveFileCopy.node << nodes_at_dest,
        ar.ArchiveFileCopy.has_file == 'Y',
    )

    # Then combine to get all file(copies) that are available at the source but
    # not at the destination...
    query = (
        ac.ArchiveFile.select(
            ac.ArchiveFile.name,
            ac.ArchiveFile.size_b,
        )
        .where(ac.ArchiveFile.acq == acq)
        .join(ar.ArchiveFileCopy)
        .where(
            ar.ArchiveFileCopy.node == src,
            ar.ArchiveFileCopy.has_file == 'Y',
            ~(ar.ArchiveFile.id << files_at_dest),
        )
    )

    data = query.tuples()

    if data:
        print(tabulate.tabulate(data, headers=['Name', 'Size']))
    else:
        print("No files to copy from node '", source_node,
              "' to group '", destination_group, "'.", sep="")

