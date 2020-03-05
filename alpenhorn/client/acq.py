"""Alpenhorn client interface for operations on `ArchiveAcq`s."""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

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
def files(acquisition):
    """List files that are in the ACQUISITION.
    """
    config_connect()

    import tabulate

    try:
        acq = ac.ArchiveAcq.get(name=acquisition)
    except pw.DoesNotExist:
        print("No such acquisition:", acquisition)
        sys.exit(1)

    query = (
        ac.ArchiveFile.select(
            ac.ArchiveFile.name,
            ac.ArchiveFile.size_b)
        .where(ac.ArchiveFile.acq_id == acq.id)
    )

    data = query.tuples()

    if data:
        print(tabulate.tabulate(data, headers=['Name', 'Size']))
    else:
        print("No registered archive files.")
