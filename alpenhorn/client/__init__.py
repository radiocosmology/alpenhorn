"""Alpenhorn client interface."""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from collections import defaultdict
import sys
import os
import datetime
import logging
import re

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

RE_LOCK_FILE = re.compile(r'^\..*\.lock$')


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


@cli.command()
@click.argument('node_name', metavar='NODE')
@click.option('-v', '--verbose', count=True)
@click.option('--acq', help='Limit import to specified acquisition directories.', multiple=True, default=None)
@click.option('--register-new', help='Register new files instead of ignoring them.', is_flag=True)
@click.option('--dry', '-d', help='Dry run. Do not modify database.', is_flag=True)
def import_files(node_name, verbose, acq, register_new, dry):
    """Scan the current directory for known acquisition files and add them into the database for NODE.

    This command is useful for manually maintaining an archive where we cannot
    run alpenhornd in the usual manner.
    """
    config_connect()

    # Keep track of state as we process the files
    added_files = []  # Files we have added to the database
    corrupt_files = []  # Known files which are corrupt
    registered_files = []  # Files already registered in the database
    unknown_files = []  # Files not known in the database

    known_acqs = []      # Directories which are known acquisitions
    new_acqs = []        # Directories which were newly registered acquisitions
    not_acqs = []        # Directories which were not known acquisitions

    # Fetch a reference to the node
    try:
        node = st.StorageNode.select().where(st.StorageNode.name == node_name).get()
    except pw.DoesNotExist:
        print("Unknown node.")
        return

    cwd = os.getcwd()
    # Construct a dictionary of directories that might be acquisitions and the of
    # list files that they contain
    db_acqs = ac.ArchiveAcq.select(ac.ArchiveAcq.name)
    acq_files = defaultdict(list)
    if len(acq) == 0:
        tops = [cwd]
    else:
        db_acqs = db_acqs.where(ac.ArchiveAcq.name >> acq)
        tops = []
        for acq_name in acq:
            acq_dir = os.path.join(node.root, acq_name)
            if not os.path.isdir(acq_dir):
                print('Aquisition "%s" does not exist in this node. Ignoring.' % acq_name,
                      file=sys.stderr)
                continue
            if acq_dir == cwd:
                # the current directory is one of the limiting acquisitions, so
                # we can ignore all others in the `--acq` list
                tops = [acq_dir]
                break
            elif cwd.startswith(acq_dir):
                # the current directory is inside one of the limiting
                # acquisitions, so we can just walk its subtree
                tops = [cwd]
                break
            elif acq_dir.startswith(cwd):
                # the acquisition is inside the current directory, so we can
                # just walk its subtree
                tops.append(acq_dir)
            else:
                print('Acquisition "%s" is outside the current directory and will be ignored.' % acq_name,
                      file=sys.stderr)

    for top in tops:
        for d, ds, fs in os.walk(top):
            d = os.path.relpath(d, node.root)
            if d == '.':            # skip the node root directory
                continue
            acq_type_name = ac.AcqType.detect(d, node)
            if acq_type_name:
                _, acq_name = acq_type_name
                if d == acq_name:
                    # the directory is the acquisition
                    acq_files[acq_name] += [f for f in fs if not RE_LOCK_FILE.match(f) and not os.path.isfile(os.path.join(d, '.{}.lock'.format(f)))]
                if d.startswith(acq_name + "/"):
                    # the directory is inside an acquisition
                    acq_dirname = os.path.relpath(d, acq_name)
                    acq_files[acq_name] += [(acq_dirname + '/' + f) for f in fs if not RE_LOCK_FILE.match(f) and not os.path.isfile(os.path.join(d, '.{}.lock'.format(f)))]
            else:
                not_acqs.append(d)

    with click.progressbar(acq_files, label='Scanning acquisitions') as acq_iter:

        for acq_name in acq_iter:
            try:
                acq = ac.ArchiveAcq.select().where(ac.ArchiveAcq.name == acq_name).get()
                known_acqs.append(acq_name)

                # Fetch lists of all files in this acquisition, and all
                # files in this acq with local copies
                file_names = [f.name for f in acq.files]
                local_file_names = [f.name for f in acq.files.join(ar.ArchiveFileCopy).where(ar.ArchiveFileCopy.node == node)]
            except pw.DoesNotExist:
                if register_new:
                    acq_type, _ = ac.AcqType.detect(acq_name, node)
                    acq = ac.ArchiveAcq(name=acq_name, type=acq_type)
                    if not dry:
                        # TODO: refactor duplication with auto_import.add_acq
                        with db.database_proxy.atomic():
                            # insert the archive record
                            acq.save()
                            # and generate the metadata table
                            acq_type.acq_info.new(acq, node)

                    new_acqs.append(acq_name)

                    # Because it's a newly imported acquisition, all files within it are new also
                    file_names = []
                    local_file_names = []
                else:
                    not_acqs.append(acq_name)
                    continue

            for f_name in acq_files[acq_name]:
                file_path = os.path.join(acq_name, f_name)

                # Check if file exists in database
                if not register_new and f_name not in file_names:
                    unknown_files.append(file_path)
                    continue

                # Check if file is already registered on this node
                if f_name in local_file_names:
                    registered_files.append(file_path)
                else:
                    abs_path = os.path.join(node.root, file_path)
                    if f_name in file_names:
                        # it is a known file
                        archive_file = ac.ArchiveFile.select().where(ac.ArchiveFile.name == f_name, ac.ArchiveFile.acq == acq).get()

                        # TODO: decide if, when the file is corrupted, we still
                        # register the file as `has_file="X"` or just _continue_
                        if (os.path.getsize(abs_path) != archive_file.size_b):
                            corrupt_files.append(file_path)
                            continue
                        else:
                            if verbose > 2:
                                print('Computing md5sum of "{}"'.format(f_name))
                            md5sum = util.md5sum_file(abs_path, cmd_line=False)
                            if md5sum != archive_file.md5sum:
                                corrupt_files.append(file_path)
                                continue
                    else:
                        # not a known file, register the new ArchiveFile instance
                        file_type = ac.FileType.detect(f_name, acq, node)
                        if not file_type:
                            unknown_files.append(file_path)
                            continue

                        if verbose > 2:
                            print('Computing md5sum of "{}"'.format(f_name))
                        md5sum = util.md5sum_file(abs_path, cmd_line=False)
                        size_b = os.path.getsize(abs_path)
                        archive_file = ac.ArchiveFile(name=f_name, acq=acq, type=file_type,
                                                      size_b=size_b, md5sum=md5sum)
                        if not dry:
                            archive_file.save()

                    added_files.append(file_path)
                    if not dry:
                        copy_size_b = os.stat(abs_path).st_blocks * 512
                        ar.ArchiveFileCopy.create(file=archive_file, node=node, has_file='Y', wants_file='Y',
                                                  size_b=copy_size_b)

    # now find the minimum unknown acqs paths that we can report
    not_acqs_roots = []
    last_acq_root = ''
    for d in sorted(not_acqs):
        common = os.path.commonprefix([last_acq_root, d])
        if common == '':
            for acq_name in known_acqs:
                if acq_name.startswith(d):
                    break
            else:
                for acq_name in new_acqs:
                    if acq_name.startswith(d):
                        break
                else:
                    not_acqs_roots.append(d)
            last_acq_root = d

    print("\n==== Summary ====")
    print()
    if register_new:
        print("Registered %i new acquisitions" % len(new_acqs))
    print("Added %i files" % len(added_files))
    print()
    print("%i corrupt files." % len(corrupt_files))
    print("%i files already registered." % len(registered_files))
    print("%i files not known" % len(unknown_files))
    print("%i directories were not acquisitions." % len(not_acqs_roots))

    if verbose > 0:
        print()
        if register_new:
            print("New acquisitions:")
            for an in sorted(new_acqs):
                print(an)
            print()

        print("Added files:")
        for fn in sorted(added_files):
            print(fn)

        print()

    if verbose > 1:
        print("Corrupt:")
        for fn in sorted(corrupt_files):
            print(fn)
        print()

        print("Unknown files:")
        for fn in sorted(unknown_files):
            print(fn)
        print()

        print("Unknown acquisitions:")
        for fn in sorted(not_acqs_roots):
            print(fn)
        print()


cli.add_command(group.cli, "group")
cli.add_command(node.cli, "node")
cli.add_command(transport.cli, "transport")
