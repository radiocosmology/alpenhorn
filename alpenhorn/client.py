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
import subprocess
import time

import click
import peewee as pw

from alpenhorn import config, extensions, db, util
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.acquisition as ac
import alpenhorn.auto_import as ai


log = logging.getLogger(__name__)

RE_LOCK_FILE = re.compile('^\..*\.lock$')


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

    _init_config_db()

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

    _init_config_db()

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
@click.argument('node_name', metavar='NODE')
@click.option('--md5', help='perform full check against md5sum', is_flag=True)
@click.option('--fixdb', help='fix up the database to be consistent with reality', is_flag=True)
@click.option('--acq', metavar='ACQ', multiple=True,
              help='Limit verification to specified acquisitions. Use repeated --acq flags to specify multiple acquisitions.')
def verify(node_name, md5, fixdb, acq):
    """Verify the archive on NODE against the database.

    If there are no issues with the archive returns with exit status of zero,
    non-zero if there are issues. Specifically:

    `0`
        No problems.
    `1`
        Corrupt files found.
    `2`
        Files missing from archive.
    `3`
        Both corrupt and missing files.
    """

    import os

    _init_config_db()

    try:
        this_node = st.StorageNode.get(name=node_name)
    except pw.DoesNotExist:
        click.echo('Storage node "{}" does not exist.'.format(node_name))
        exit(1)

    if not this_node.active:
        click.echo('Node "{}" is not active.'.format(node_name))
        exit(1)
    if not util.alpenhorn_node_check(this_node):
        click.echo('Node "{}" does not match ALPENHORN_NODE: {}'
                   .format(node_name, this_node.root))
        exit(1)

    # Use a complicated query with a tuples construct to fetch everything we
    # need in a single query. This massively speeds up the whole process versus
    # fetching all the FileCopy's then querying for Files and Acqs.
    lfiles = ac.ArchiveFile\
               .select(ac.ArchiveFile.name, ac.ArchiveAcq.name,
                       ac.ArchiveFile.size_b, ac.ArchiveFile.md5sum,
                       ar.ArchiveFileCopy.id)\
               .join(ac.ArchiveAcq)\
               .switch(ac.ArchiveFile)\
               .join(ar.ArchiveFileCopy)\
               .where(ar.ArchiveFileCopy.node == this_node,
                      ar.ArchiveFileCopy.has_file == 'Y')\
               .tuples()

    missing_files = []
    corrupt_files = []

    missing_ids = []
    corrupt_ids = []

    nfiles = 0

    with click.progressbar(lfiles, label='Scanning files') as lfiles_iter:
        for filename, acqname, filesize, md5sum, fc_id in lfiles_iter:

            # Skip if not in specified acquisitions
            if len(acq) > 0 and acqname not in acq:
                continue

            nfiles += 1

            filepath = this_node.root + '/' + acqname + '/' + filename

            # Check if file is plain missing
            if not os.path.exists(filepath):
                missing_files.append(filepath)
                missing_ids.append(fc_id)
                continue

            if md5:
                file_md5 = util.md5sum_file(filepath)
                corrupt = (file_md5 != md5sum)
            else:
                corrupt = (os.path.getsize(filepath) != filesize)

            if corrupt:
                corrupt_files.append(filepath)
                corrupt_ids.append(fc_id)
                continue

    if len(missing_files) > 0:
        click.echo()
        click.echo("=== Missing files ===")
        for fname in missing_files:
            click.echo(fname)

    if len(corrupt_files) > 0:
        print()
        click.echo("=== Corrupt files ===")
        for fname in corrupt_files:
            click.echo(fname)

    click.echo()
    click.echo("=== Summary ===")
    click.echo("  %i total files" % nfiles)
    click.echo("  %i missing files" % len(missing_files))
    click.echo("  %i corrupt files" % len(corrupt_files))
    click.echo()

    # Fix up the database by marking files as missing, and marking
    # corrupt files for verification by alpenhornd.
    if fixdb:

        # TODO: ensure write access to the database
        # # We need to write to the database.
        # di.connect_database(read_write=True)

        if (len(missing_files) > 0) and click.confirm('Fix missing files'):
            missing_count = ar.ArchiveFileCopy\
                              .update(has_file='N')\
                              .where(ar.ArchiveFileCopy.id << missing_ids)\
                              .execute()
            click.echo("  %i marked as missing" % missing_count)

        if (len(corrupt_files) > 0) and click.confirm('Fix corrupt files'):
            corrupt_count = ar.ArchiveFileCopy\
                              .update(has_file='M')\
                              .where(ar.ArchiveFileCopy.id << corrupt_ids)\
                              .execute()
            click.echo("  %i corrupt files marked for verification" % corrupt_count)
    else:
        # Set the exit status
        status = 1 if corrupt_files else 0
        status += 2 if missing_files else 0

        sys.exit(status)


@cli.command()
@click.argument('node_name', metavar='NODE')
@click.option('--days', '-d', help='Clean files older than <days>.', type=int, default=None)
@click.option('--cancel', help='Cancel files marked for cleaning', is_flag=True)
@click.option('--force', '-f', help='Force cleaning on an archive node.', is_flag=True)
@click.option('--now', '-n', help='Force immediate removal.', is_flag=True)
@click.option('--target', metavar='TARGET_GROUP', default=None, type=str,
              help='Only clean files already available in this group.')
@click.option('--acq', metavar='ACQ', default=None, type=str,
              help='Limit removal to acquisition.')
def clean(node_name, days, cancel, force, now, target, acq):
    """Clean up NODE by marking older files as potentially removable.

    Files will never be removed until they are available on at least two
    archival nodes.

    Normally, files are marked to be removed only if the disk space on the node
    is running low. With the --now flag, they will be made available for
    immediate removal. Either way, they will *never* be actually removed until
    there are sufficient archival copies.

    Using the --cancel option undoes previous cleaning operations by marking
    files that are still on the node and that were marked as available for
    removal as "must keep".

    If --target is specified, the command will only affect files already
    available in the TARGET_GROUP. This is useful for cleaning out intermediate
    locations such as transport disks.

    Using the --days flag will only clean correlator and housekeeping
    files which have a timestamp associated with them. It will not
    touch other types. If no --days flag is given, all files will be
    considered for removal.
    """

    if cancel and now:
        print('Options --cancel and --now are mutually exclusive.')
        exit(1)

    _init_config_db()

    try:
        this_node = st.StorageNode.get(st.StorageNode.name == node_name)
    except pw.DoesNotExist:
        print('Storage node "%s" does not exist.' % node_name)
        exit(1)

    # Check to see if we are on an archive node
    if this_node.storage_type == 'A':
        if force or click.confirm('DANGER: run clean on archive node "%s"?' % node_name):
            print('"%s" is an archive node. Forcing clean.' % node_name)
        else:
            print('Cannot clean archive node "%s" without forcing.' % node_name)
            exit(1)

    # Select FileCopys on this node.
    files = ar.ArchiveFileCopy.select(ar.ArchiveFileCopy.id).where(
        ar.ArchiveFileCopy.node == this_node,
        ar.ArchiveFileCopy.has_file == 'Y'
    )

    if now:
        # In 'now' cleaning, every copy will be set to wants_file="No", if it
        # wasn't already
        files = files.where(ar.ArchiveFileCopy.wants_file != 'N')
    elif cancel:
        # Undo any "Maybe" and "No" want_files and reset them to "Yes"
        files = files.where(ar.ArchiveFileCopy.wants_file != 'Y')
    else:
        # In regular cleaning, we only mark as "Maybe" want_files that are
        # currently "Yes", but leave "No" unchanged
        files = files.where(ar.ArchiveFileCopy.wants_file == 'Y')

    # Limit to acquisition
    if acq is not None:
        try:
            acq = ac.ArchiveAcq.get(name=acq)
        except pw.DoesNotExit:
            raise RuntimeError("Specified acquisition %s does not exist" % acq)

        files_in_acq = ac.ArchiveFile.select().where(ac.ArchiveFile.acq == acq)

        files = files.where(ar.ArchiveFileCopy.file << files_in_acq)

    # If the target option has been specified, only clean files also available there...
    if target is not None:

        # Fetch a reference to the target group
        try:
            target_group = st.StorageGroup.get(name=target)
        except pw.DoesNotExist:
            raise RuntimeError("Target group \"%s\" does not exist in the DB." % target)

        # First get the nodes at the destination...
        nodes_at_target = st.StorageNode.select().where(st.StorageNode.group == target_group)

        # Then use this to get a list of all files at the destination...
        files_at_target = ac.ArchiveFile.select().join(ar.ArchiveFileCopy).where(
            ar.ArchiveFileCopy.node << nodes_at_target,
            ar.ArchiveFileCopy.has_file == 'Y'
        )

        # Only match files that are also available at the target
        files = files.where(ar.ArchiveFileCopy.file << files_at_target)

    # If --days has been set we need to restrict to files older than the given
    # time. This only works for a few particular file types
    if days is not None and days > 0:

        # TODO: how to handle file types now?
        raise "'--days' feature has not been implemented yet"

        # # Get the time for the oldest files to keep
        # oldest = datetime.datetime.now() - datetime.timedelta(days)
        # oldest_unix = ephemeris.ensure_unix(oldest)
        #
        # # List of filetypes we want to update, needs a human readable name and a
        # # FileInfo table.
        # filetypes = [ ['correlation', di.CorrFileInfo],
        #               ['housekeeping', di.HKFileInfo] ]
        #
        # file_ids = []
        #
        # # Iterate over file types for cleaning
        # for name, infotable in filetypes:
        #
        #     # Filter to fetch only ones with a start time older than `oldest`
        #     oldfiles = files.join(ac.ArchiveFile).join(infotable)\
        #         .where(infotable.start_time < oldest_unix)
        #
        #     local_file_ids = list(oldfiles)
        #
        #     # Get number of correlation files
        #     count = oldfiles.count()
        #
        #     if count > 0:
        #         size_bytes = ar.ArchiveFileCopy.select().where(ar.ArchiveFileCopy.id << local_file_ids)\
        #             .join(ac.ArchiveFile).aggregate(pw.fn.Sum(ac.ArchiveFile.size_b))
        #
        #         size_gb = int(size_bytes) / 2**30.0
        #
        #         print "Cleaning up %i %s files (%.1f GB) from %s " % (count, name, size_gb, node_name)
        #
        #         file_ids += local_file_ids

    # If days is not set, then just select all files that meet the requirements so far
    else:

        file_ids = list(files)
        count = files.count()

        if count > 0:
            size_bytes = ar.ArchiveFileCopy.select().where(
                ar.ArchiveFileCopy.id << file_ids
            ).join(ac.ArchiveFile).select(pw.fn.Sum(ac.ArchiveFile.size_b)).scalar()

            size_gb = int(size_bytes) / 1073741824.0

            print('Mark %i files (%.1f GB) from "%s" %s.' %
                  (count, size_gb, node_name,
                   'for keeping' if cancel else 'available for removal'))

    # If there are any files to clean, ask for confirmation and the mark them in
    # the database for removal
    if len(file_ids) > 0:
        if force or click.confirm("  Are you sure?"):
            print("  Marking...")

            if cancel:
                state = 'Y'
            else:
                state = 'N' if now else 'M'

            update = ar.ArchiveFileCopy.update(wants_file=state)\
                .where(ar.ArchiveFileCopy.id << file_ids)

            n = update.execute()

            if cancel:
                print('Marked %i files for keeping.' % n)
            else:
                print('Marked %i files available for removal.' % n)

        else:
            print('  Cancelled. Exit without changes.')
    else:
        print("No files selected for cleaning on %s." % node_name)


@cli.command()
@click.option('--host', '-H', help='use specified host rather than local machine', type=str, default=None)
def active(host):
    """List the nodes active on this, or another specified, machine"""
    import socket

    _init_config_db()

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
@click.argument("serial_num")
def format_transport(serial_num):
    """Interactive routine for formatting a transport disc as a storage
    node; formats and labels the disc as necessary, the adds to the
    database. The disk is specified using the manufacturers
    SERIAL_NUM, which is printed on the disk.
    """
    import os
    import glob

    _init_config_db()

    if os.getuid() != 0:
        print("You must be root to run mount on a transport disc. I quit.")
        return

    # Find the disc.
    dev = glob.glob("/dev/disk/by-id/*%s" % serial_num)
    if len(dev) == 0:
        print("No disc with that serial number is attached.")
        return
    elif len(dev) > 1:
        print("Confused: found more than one device matching that serial number:")
        for d in dev:
            print("  %s" % dev)
        print("Aborting.")
        return
    dev = dev[0]
    dev_part = "%s-part1" % dev

    # Figure out if it is formatted.
    print("Checking to see if disc is formatted. Please wait.")
    formatted = False
    try:
        # check if the block device is partitioned
        subprocess.check_output(['blkid', '-p', dev])

        # now check if the partition is formatted
        if 'TYPE=' in subprocess.check_output(['blkid', '-p', dev_part]):
            formatted = True
    except subprocess.CalledProcessError:
        pass

    if not formatted:
        if not click.confirm("Disc is not formatted. Should I format it?"):
            return
        print("Creating partition. Please wait.")
        try:
            subprocess.check_call(['parted', '-s', '-a', 'optimal', dev,
                                   'mklabel', 'gpt',
                                   '--',
                                   'mkpart', 'primary', '0%', '100%'])
        except subprocess.CalledProcessError as e:
            print("Failed to create the partition! Stat = %s. I quit.\n%s" % (e.returncode, e.output))
            exit(1)

        # pause to give udev rules time to get updated
        time.sleep(1)

        print("Formatting disc. Please wait.")
        try:
            subprocess.check_call(['mkfs.ext4', dev_part, '-m', '0',
                                   '-L', 'CH-{}'.format(serial_num)])
        except subprocess.CalledProcessError as e:
            print("Failed to format the disk! Stat = %s. I quit.\n%s" % (e.returncode, e.output))
            exit(1)
    else:
        print("Disc is already formatted.")

    e2label = get_e2label(dev_part)
    name = "CH-%s" % serial_num
    if e2label and e2label != name:
        print("Disc label %s does not conform to labelling standard, "
              "which is CH-<serialnum>.")
        exit
    elif not e2label:
        print("Labelling the disc as \"%s\" (using e2label) ..." % (name))
        assert dev_part is not None
        assert len(name) <= MAX_E2LABEL_LEN
        try:
            subprocess.check_call(['/sbin/e2label', dev_part, name])
        except subprocess.CalledProcessError as e:
            print("Failed to e2label! Stat = %s. I quit.\n%s" % (e.returncode, e.output))
            exit(1)

    # Ensure the mount path exists.
    root = "/mnt/%s" % name
    if not os.path.isdir(root):
        print("Creating mount point %s." % root)
        os.mkdir(root)

    # Check to see if the disc is mounted.
    try:
        output = subprocess.check_output(['df'])
        dev_part_abs = os.path.realpath(dev_part)
        for l in output.split('\n'):
            if l.find(root) > 0:
                if l[:len(dev_part)] == dev or l[:len(dev_part_abs)] == dev_part_abs:
                    print("%s is already mounted at %s" %
                          (l.split()[0], root))
                else:
                    print("%s is a mount point, but %s is already mounted there."
                          (root, l.split()[0]))
    except subprocess.CalledProcessError as e:
        print("Failed to check the mountpoint! Stat = %s. I quit.\n%s" % (e.returncode, e.output))
        exit(1)

    try:
        node = st.StorageNode.get(name=name)
    except pw.DoesNotExist:
        print("This disc has not been registered yet as a storage node. "
              "Registering now.")
        try:
            group = st.StorageGroup.get(name="transport")
        except pw.DoesNotExist:
            print("Hmmm. Storage group \"transport\" does not exist. I quit.")
            exit(1)

        # TODO: ensure write access to the database
        # # We need to write to the database.
        # di.connect_database(read_write=True)
        node = st.StorageNode.create(name=name, root=root, group=group,
                                     storage_type="T", min_avail_gb=1)

        print("Successfully created storage node.")

    print("Node created but not activated. Run alpenhorn mount_transport for that.")


@cli.command()
@click.pass_context
@click.argument("node")
@click.option("--user", help="username to access this node.", type=str, default=None)
@click.option("--address", help="address for remote access to this node.", type=str, default=None)
def mount_transport(ctx, node, user, address):
    """Mount a transport disk into the system and then make it available to alpenhorn.
    """

    mnt_point = "/mnt/%s" % node

    if os.path.ismount(mnt_point):
        print('{} is already mounted in the filesystem. Proceeding to activate it.'.format(node))
    else:
        print("Mounting disc at %s" % mnt_point)
        os.system("mount %s" % mnt_point)

    ctx.invoke(activate, name=node, path=mnt_point, user=user, address=address)


@cli.command()
@click.pass_context
@click.argument("node")
def unmount_transport(ctx, node):
    """Unmount a transport disk from the system and then remove it from alpenhorn.
    """

    mnt_point = "/mnt/%s" % node

    print("Unmounting disc at %s" % mnt_point)
    os.system("umount %s" % mnt_point)

    ctx.invoke(deactivate, root_or_name=node)


@cli.command()
@click.argument("name")
@click.option("--path", help="Root path for this node", type=str, default=None)
@click.option("--user", help="username to access this node.", type=str, default=None)
@click.option("--address", help="address for remote access to this node.", type=str, default=None)
@click.option("--hostname", type=str, default=None,
              help="hostname running the alpenhornd instance for this node (set to this hostname by default).")
def activate(name, path, user, address, hostname):
    """Interactive routine for activating a storage node located at ROOT."""

    import socket

    _init_config_db()

    try:
        node = st.StorageNode.get(name=name)
    except pw.DoesNotExist:
        click.echo("Storage node \"%s\" does not exist. I quit." % name)
        exit(1)

    if node.active:
        click.echo("Node \"%s\" is already active." % name)
        return

    if path is not None:
        node.root = path

    if not util.alpenhorn_node_check(node):
        click.echo('Node "{}" does not match ALPENHORN_NODE'.format(node.name))
        exit(1)

    # Set the default hostname if required
    if hostname is None:
        hostname = util.get_short_hostname()
        click.echo("I will set the host to \"%s\"." % hostname)

    # Set the parameters of this node
    node.username = user
    node.address = address
    node.active = True
    node.host = hostname

    node.save()

    click.echo("Successfully activated \"%s\"." % name)


@cli.command()
@click.argument("root_or_name")
def deactivate(root_or_name):
    """Deactivate a storage node with location or named ROOT_OR_NAME."""
    import os
    import socket

    _init_config_db()

    try:
        node = st.StorageNode.get(name=root_or_name)
    except pw.DoesNotExist:
        if root_or_name[-1] == "/":
            root_or_name = root_or_name[:len(root_or_name) - 1]

        if not os.path.exists(root_or_name):
            click.echo("That is neither a node name, nor a path on this host. "
                       "I quit.")
            exit(1)
        try:
            node = st.StorageNode.get(root=root_or_name,
                                      host=util.get_short_hostname())
        except pw.DoesNotExist:
            click.echo("That is neither a node name nor a root name that is "
                       "known. I quit.")
            exit(1)

    if not node.active:
        click.echo("There is no active node there any more.")
    else:
        node.active = False
        node.save()
        print("Node successfully deactivated.")


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
    _init_config_db()

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


@cli.command()
@click.argument('group_name', metavar='GROUP')
@click.option('--notes', metavar='NOTES')
def create_group(group_name, notes):
    """Create a storage GROUP and add to database.
    """
    _init_config_db()

    try:
        st.StorageGroup.get(name=group_name)
        print("Group name \"%s\" already exists! Try a different name!" % group_name)
        exit(1)
    except pw.DoesNotExist:
        st.StorageGroup.create(name=group_name, notes=notes)
        print("Added group \"%s\" to database." % group_name)


@cli.command()
@click.argument('node_name', metavar='NODE')
@click.argument('root', metavar='ROOT')
@click.argument('hostname', metavar='HOSTNAME')
@click.argument('group', metavar='GROUP', type=str, default=None)
@click.option('--address', help="Domain name or IP address for the host \
              (if network accessible).", metavar='ADDRESS',
              type=str, default=None)
@click.option('--active', help='Is the node active?', metavar="BOOL",
              type=bool, default=False)
@click.option('--auto_import', help='Should files that appear on this node be \
              automatically added?', metavar='BOOL', type=bool, default=False)
@click.option('--suspect', help='Is this node corrupted?',
              metavar='BOOL', type=bool, default=False)
@click.option('--storage_type', help='What is the type of storage? Options:\
                A - archive for the data, T - for transiting data \
                F - for data in the field (i.e acquisition machines)',
              type=click.Choice(['A', 'T', 'F']), default='A')
@click.option('--max_total_gb', help='The maximum amout of storage we should \
              use.', metavar='FLOAT', type=float, default=-1.)
@click.option('--min_avail_gb', help='What is the minimum amount of free space \
               we should leave on this node?', metavar='FLOAT',
              type=float, default=-1.)
@click.option('--min_delete_age_days', help='What is the minimum amount of time \
              a file must remain on the node before we are allowed to delete \
              it?', metavar='FLOAT', type=float, default=30)
@click.option('--notes', help='Any notes or comments about this node.',
              type=str, default=None)
def create_node(node_name, root, hostname, group, address, active, auto_import,
                suspect, storage_type, max_total_gb, min_avail_gb,
                min_delete_age_days, notes):
    """Create a storage NODE within storage GROUP with a ROOT directory on
    HOSTNAME.
    """
    _init_config_db()

    try:
        this_group = st.StorageGroup.get(name=group)
    except pw.DoesNotExist:
        print("Requested group \"%s\" does not exit in DB." % group)
        exit(1)

    try:
        this_node = st.StorageNode.get(name=node_name)
        print("Node name \"%s\" already exists! Try a different name!" % node_name)
        exit(1)

    except pw.DoesNotExist:
        st.StorageNode.create(name=node_name, root=root, host=hostname,
                              address=address, group=this_group.id, active=active,
                              auto_import=auto_import, suspect=suspect,
                              storage_type=storage_type, max_total_gb=max_total_gb,
                              min_avail_gb=min_avail_gb, min_delete_age_days=min_delete_age_days,
                              notes=notes)

        print('Added node "%(node)s" belonging to group "%(group)s" in the directory '
              '"%(root)s" at host "%(host)s" to database.' % dict(
                  node=node_name,
                  root=root,
                  group=group,
                  host=hostname
              ))

# A few utility routines for dealing with filesystems
MAX_E2LABEL_LEN = 16


def get_e2label(dev):
    """Read filesystem label on an Ext{2,3,4}fs device

    Parameters
    ----------
    dev: str
        The path to the device file.

    Returns
    -------
    str or None
        the filesystem label, or None if reading it failed.
    """

    try:
        output = subprocess.check_output(["/sbin/e2label", dev]).strip()
        if len(output) < MAX_E2LABEL_LEN:
            return output
    except subprocess.CalledProcessError:
        return None


def _init_config_db():
    """Load the config, start the database and register extensions.
    """
    # Load the configuration and initialise the database connection
    config.load_config()
    extensions.load_extensions()
    db.config_connect()

    # Register the acq/file type extensions
    extensions.register_type_extensions()
