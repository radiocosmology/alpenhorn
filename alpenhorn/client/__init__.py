"""Alpenhorn client interface."""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import datetime
import logging

import click
import peewee as pw

from alpenhorn import config, extensions, db, util
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.acquisition as ac

from .connect_db import config_connect
from . import group
from . import node
from . import transport


log = logging.getLogger(__name__)


@click.group(context_settings={'help_option_names': ['-h', '--help']})
def cli():
    """Client interface for alpenhorn. Use to request transfers, mount drives,
    check status etc."""
    pass


@cli.command()
def init():
    """Initialise an alpenhorn database.

    Creates the database tables required for alpenhorn and any extensions
    specified in its configuration.
    """

    # Load the configuration and initialise the database connection
    config.load_config()
    extensions.load_extensions()
    db.config_connect()

    # Create any alpenhorn core tables
    core_tables = [
        ac.AcqType, ac.ArchiveAcq, ac.FileType,
        ac.ArchiveFile, st.StorageGroup, st.StorageNode,
        ar.ArchiveFileCopy, ar.ArchiveFileCopyRequest
    ]

    db.database_proxy.create_tables(core_tables, safe=True)

    # Register the acq/file type extensions
    extensions.register_type_extensions()

    # Create any tables registered by extensions
    ext_tables = (list(ac.AcqType._registered_acq_types.values()) +
                  list(ac.FileType._registered_file_types.values()))

    db.database_proxy.create_tables(ext_tables, safe=True)


@cli.command()
@click.argument('node_name', metavar='NODE')
@click.argument('group_name', metavar='GROUP')
@click.option('--acq', help='Sync only this acquisition.', metavar='ACQ', type=str, default=None)
@click.option('--force', '-f', help='proceed without confirmation', is_flag=True)
@click.option('--nice', '-n', help='nice level for transfer', default=0)
@click.option('--target', metavar='TARGET_GROUP', default=None, type=str,
              help='Only transfer files not available on this group.')
@click.option("--transport", "-t", is_flag=True,
              help="[DEPRECATED] transport mode: only copy if fewer than two archived copies exist.")
@click.option('--show_acq', help='Summarise acquisitions to be copied.', is_flag=True)
@click.option('--show_files', help='Show files to be copied.', is_flag=True)
def sync(node_name, group_name, acq, force, nice, target, transport, show_acq, show_files):
    """Copy all files from NODE to GROUP that are not already present.

    We can also use the --target option to only transfer files that are not
    available on both the destination group, and the TARGET_GROUP. This is
    useful for transferring data to a staging location before going to a final
    archive (e.g. HPSS, transport disks).
    """

    config_connect()

    try:
        from_node = st.StorageNode.get(name=node_name)
    except pw.DoesNotExist:
        click.echo("Node \"%s\" does not exist in the DB." % node_name)
        exit(1)
    try:
        to_group = st.StorageGroup.get(name=group_name)
    except pw.DoesNotExist:
        click.echo("Group \"%s\" does not exist in the DB." % group_name)
        exit(1)

    # Construct list of file copies that are available on the source node, and
    # not available on any nodes at the destination. This query is quite complex
    # so I've broken it up...

    # First get the nodes at the destination...
    nodes_at_dest = st.StorageNode.select().where(st.StorageNode.group == to_group)

    # Then use this to get a list of all files at the destination...
    files_at_dest = ac.ArchiveFile.select().join(ar.ArchiveFileCopy).where(
        ar.ArchiveFileCopy.node << nodes_at_dest,
        ar.ArchiveFileCopy.has_file == 'Y'
    )

    # Then combine to get all file(copies) that are available at the source but
    # not at the destination...
    copy = ar.ArchiveFileCopy.select().where(
        ar.ArchiveFileCopy.node == from_node,
        ar.ArchiveFileCopy.has_file == 'Y',
        ~(ar.ArchiveFileCopy.file << files_at_dest))

    # If the target option has been specified, only copy nodes also not
    # available there...
    if target is not None:

        # Fetch a reference to the target group
        try:
            target_group = st.StorageGroup.get(name=target)
        except pw.DoesNotExist:
            click.echo("Target group \"%s\" does not exist in the DB." % target)
            exit(1)

        # First get the nodes at the destination...
        nodes_at_target = st.StorageNode.select().where(st.StorageNode.group == target_group)

        # Then use this to get a list of all files at the destination...
        files_at_target = ac.ArchiveFile.select().join(ar.ArchiveFileCopy).where(
            ar.ArchiveFileCopy.node << nodes_at_target,
            ar.ArchiveFileCopy.has_file == 'Y'
        )

        # Only match files that are also not available at the target
        copy = copy.where(~(ar.ArchiveFileCopy.file << files_at_target))

    # In transport mode (DEPRECATED) we only move files that don't have an
    # archive copy elsewhere...
    if transport:
        import warnings
        warnings.warn('Transport mode is deprecated. Try to use --target instead.')

        # Get list of other archive nodes
        other_archive_nodes = st.StorageNode.select().where(
            st.StorageNode.storage_type == "A",
            st.StorageNode.id != from_node
        )

        files_in_archive = ac.ArchiveFile.select().join(ar.ArchiveFileCopy).where(
            ar.ArchiveFileCopy.node << other_archive_nodes,
            ar.ArchiveFileCopy.has_file == "Y"
        )

        copy = copy.where(~(ar.ArchiveFileCopy.file << files_in_archive))

    # Join onto ArchiveFile for later query parts
    copy = copy.join(ac.ArchiveFile)

    # If requested, limit query to a specific acquisition...
    if acq is not None:

        # Fetch acq if specified
        try:
            acq = ac.ArchiveAcq.get(name=acq)
        except pw.DoesNotExist:
            raise Exception("Acquisition \"%s\" does not exist in the DB." % acq)

        # Restrict files to be in the acquisition
        copy = copy.where(ac.ArchiveFile.acq == acq)

    if not copy.count():
        print("No files to copy from node %s." % (node_name))
        return

    # Show acquisitions based summary of files to be copied
    if show_acq:
        acqs = [c.file.acq.name for c in copy]

        import collections
        for acq, count in collections.Counter(acqs).items():
            print("%s [%i files]" % (acq, count))

    # Show all files to be copied
    if show_files:
        for c in copy:
            print("%s/%s" % (c.file.acq.name, c.file.name))

    size_bytes = copy.select(pw.fn.Sum(ac.ArchiveFile.size_b)).scalar()
    size_gb = int(size_bytes) / 1073741824.0

    print ('Will request that %d files (%.1f GB) be copied from node %s to group %s.' %
           (copy.count(), size_gb, node_name, group_name))

    if not (force or click.confirm("Do you want to proceed?")):
        print("Aborted.")
        return

    dtnow = datetime.datetime.now()

    # Perform update in a transaction to avoid any clobbering from concurrent updates
    with ar.ArchiveFileCopyRequest._meta.database.atomic():

        # Get a list of all the file ids for the copies we should perform
        files_ids = [c.file_id for c in copy]

        # Get a list of all the file ids for exisiting requests
        requests = ar.ArchiveFileCopyRequest.select().where(
            ar.ArchiveFileCopyRequest.group_to == to_group,
            ar.ArchiveFileCopyRequest.node_from == from_node,
            ~ar.ArchiveFileCopyRequest.completed,
            ~ar.ArchiveFileCopyRequest.cancelled,
        )
        req_file_ids = [req.file_id for req in requests]

        # Separate the files into ones that already have requests and ones that don't
        files_in = [x for x in files_ids if x in req_file_ids]
        files_out = [x for x in files_ids if x not in req_file_ids]

        click.echo("Adding {} new requests{}.\n"
                   .format(len(files_out) or 'no',
                           ', keeping {} already existing'
                           .format(len(files_in)) if len(files_in) else ''))

        # Insert any new requests
        if len(files_out) > 0:

            # Construct a list of all the rows to insert
            insert = [{'file': fid, 'node_from': from_node, 'nice': 0,
                       'group_to': to_group, 'completed': False,
                       'timestamp': dtnow} for fid in files_out]

            # Do a bulk insert of these new rows
            ar.ArchiveFileCopyRequest.insert_many(insert).execute()


@cli.command()
@click.option('--all', help='Show the status of all nodes, not just active ones.', is_flag=True)
def status(all):
    """Summarise the status of alpenhorn storage nodes.
    """

    import tabulate

    config_connect()

    # Data to fetch from the database (node name, total files, total size)
    query_info = (
        st.StorageNode.name, pw.fn.Count(ar.ArchiveFileCopy.id).alias('count'),
        pw.fn.Sum(ac.ArchiveFile.size_b).alias('total_size'), st.StorageNode.host, st.StorageNode.root
    )

    # Per node totals
    nodes = st.StorageNode.select(*query_info)\
        .join(ar.ArchiveFileCopy, pw.JOIN.LEFT_OUTER, on=((st.StorageNode.id == ar.ArchiveFileCopy.node_id) & (ar.ArchiveFileCopy.has_file == 'Y')))\
        .join(ac.ArchiveFile, pw.JOIN.LEFT_OUTER, on=(ac.ArchiveFile.id == ar.ArchiveFileCopy.file_id)).group_by(st.StorageNode).order_by(st.StorageNode.name)

    log.info("Nodes: %s (all=%s)" % (nodes.count(), all))
    if not all:
        nodes = nodes.where(st.StorageNode.active)

    log.info("Nodes: %s" % nodes.count())

    # Totals for the whole archive
    total_count, total_size = ac.ArchiveFile.select(
        pw.fn.Count(ac.ArchiveFile.id).alias('count'),
        pw.fn.Sum(ac.ArchiveFile.size_b).alias('total_size')).scalar(as_tuple=True)

    # Create table of node stats to present to the user
    data = []
    for node in nodes.tuples():
        node_name, file_count, file_size, node_host, node_root = node
        pct_count = (100.0 * file_count / total_count) if total_count else None
        pct_size = (100.0 * float(file_size / total_size)) if total_count and file_size else None
        file_size_tb = (float(file_size) / 2**40.0) if file_count else None
        node_path = '%s:%s' % (node_host, node_root)
        data.append([node_name, file_count, file_size_tb, pct_count, pct_size, node_path])

    headers = ['Node', 'Files', 'Size [TB]', 'Files [%]', 'Size [%]', 'Path']

    print(tabulate.tabulate(data, headers=headers, floatfmt=".1f"))


@cli.command()
@click.option('--host', '-H', help='use specified host rather than local machine', type=str, default=None)
def active(host):
    """List the nodes active on this, or another specified, machine"""

    config_connect()

    if host is None:
        host = util.get_short_hostname()
    zero = True
    for node in (st.StorageNode.select()
                 .where(st.StorageNode.host == host, st.StorageNode.active)):
        n_file = ar.ArchiveFileCopy \
                   .select() \
                   .where((ar.ArchiveFileCopy.node == node) & (ar.ArchiveFileCopy.has_file == 'Y')) \
                   .count()
        print("%-25s %-30s %5d files" % (node.name, node.root, n_file))
        zero = False
    if zero:
        print("No nodes are active on host %s." % host)


cli.add_command(group.cli, "group")
cli.add_command(node.cli, "node")
cli.add_command(transport.cli, "transport")
