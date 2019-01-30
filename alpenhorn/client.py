"""Alpenhorn client interface."""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import sys
import os
import datetime
import logging

import click
import peewee as pw

from alpenhorn import config, extensions, db, util
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.acquisition as ac
import alpenhorn.auto_import as ai


log = logging.getLogger(__name__)


@click.group()
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
        raise Exception("Node \"%s\" does not exist in the DB." % node_name)
    try:
        to_group = st.StorageGroup.get(name=group_name)
    except pw.DoesNotExist:
        raise Exception("Group \"%s\" does not exist in the DB." % group_name)

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
            raise RuntimeError("Target group \"%s\" does not exist in the DB." % target)

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

    size_bytes = copy.aggregate(pw.fn.Sum(ac.ArchiveFile.size_b))
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
            ar.ArchiveFileCopyRequest.node_from == from_node
        )
        req_file_ids = [req.file_id for req in requests]

        # Separate the files into ones that already have requests and ones that don't
        files_in = [x for x in files_ids if x in req_file_ids]
        files_out = [x for x in files_ids if x not in req_file_ids]

        click.echo("Updating %i existing requests and inserting %i new ones.\n" % (len(files_in), len(files_out)))

        # Perform an update of all the existing copy requests
        if len(files_in) > 0:
            update = ar.ArchiveFileCopyRequest.update(nice=nice, completed=False, cancelled=False, timestamp=dtnow,
                                                      n_requests=ar.ArchiveFileCopyRequest.n_requests + 1)

            update = update.where(ar.ArchiveFileCopyRequest.file << files_in,
                                  ar.ArchiveFileCopyRequest.group_to == to_group,
                                  ar.ArchiveFileCopyRequest.node_from == from_node)
            update.execute()

        # Insert any new requests
        if len(files_out) > 0:

            # Construct a list of all the rows to insert
            insert = [{'file': fid, 'node_from': from_node, 'nice': 0,
                       'group_to': to_group, 'completed': False,
                       'n_requests': 1, 'timestamp': dtnow} for fid in files_out]

            # Do a bulk insert of these new rows
            ar.ArchiveFileCopyRequest.insert_many(insert).execute()


@cli.command()
@click.option('--all', help='Show the status of all nodes, not just mounted ones.', is_flag=True)
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
        .join(ar.ArchiveFileCopy).where(ar.ArchiveFileCopy.has_file == 'Y')\
        .join(ac.ArchiveFile).group_by(st.StorageNode).order_by(st.StorageNode.name)

    log.info("Nodes: %s (all=%s)" % (nodes.count(), all))
    if not all:
        nodes = nodes.where(st.StorageNode.mounted)

    log.info("Nodes: %s" % nodes.count())

    # Totals for the whole archive
    tot = ac.ArchiveFile.select(pw.fn.Count(ac.ArchiveFile.id).alias('count'),
                                pw.fn.Sum(ac.ArchiveFile.size_b).alias('total_size')).scalar(as_tuple=True)

    data = [[node[0], int(node[1]), int(node[2]) / 2**40.0,
             100.0 * int(node[1]) / int(tot[0]), 100.0 * int(node[2]) / int(tot[1]),
             '%s:%s' % (node[3], node[4])] for node in nodes.tuples()]

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
        this_node = st.StorageNode.get(st.StorageNode.name == node_name)
    except pw.DoesNotExist:
        click.echo("Specified node does not exist.")
        return

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
@click.option('--force', '-f', help='Force cleaning on an archive node.', is_flag=True)
@click.option('--now', '-n', help='Force immediate removal.', is_flag=True)
@click.option('--target', metavar='TARGET_GROUP', default=None, type=str,
              help='Only clean files already available in this group.')
@click.option('--acq', metavar='ACQ', default=None, type=str,
              help='Limit removal to acquisition.')
def clean(node_name, days, force, now, target, acq):
    """Clean up NODE by marking older files as potentially removable.

    If --target is specified we will only remove files already available in the
    TARGET_GROUP. This is useful for cleaning out intermediate locations such as
    transport disks.

    Using the --days flag will only clean correlator and housekeeping
    files which have a timestamp associated with them. It will not
    touch other types. If no --days flag is given, all files will be
    considered for removal.
    """

    _init_config_db()

    try:
        this_node = st.StorageNode.get(st.StorageNode.name == node_name)
    except pw.DoesNotExist:
        print("Specified node does not exist.")
        return

    # Check to see if we are on an archive node
    if this_node.storage_type == 'A':
        if force or click.confirm('DANGER: run clean on archive node?'):
            print("%s is an archive node. Forcing clean." % node_name)
        else:
            print("Cannot clean archive node %s without forcing." % node_name)
            return

    # Select FileCopys on this node.
    files = ar.ArchiveFileCopy.select(ar.ArchiveFileCopy.id).where(
        ar.ArchiveFileCopy.node == this_node,
        ar.ArchiveFileCopy.has_file == 'Y'
    )

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
            ).join(ac.ArchiveFile).aggregate(pw.fn.Sum(ac.ArchiveFile.size_b))

            size_gb = int(size_bytes) / 1073741824.0

            print('Cleaning up %i files (%.1f GB) from %s.' % (count, size_gb, node_name))

    # If there are any files to clean, ask for confirmation and the mark them in
    # the database for removal
    if len(file_ids) > 0:
        if force or click.confirm("  Are you sure?"):
            print("  Marking files for cleaning.")

            state = 'N' if now else 'M'

            update = ar.ArchiveFileCopy.update(wants_file=state)\
                .where(ar.ArchiveFileCopy.id << file_ids)

            n = update.execute()

            print("Marked %i files for cleaning" % n)

        else:
            print("  Cancelled")
    else:
        print("No files selected for cleaning on %s." % node_name)


@cli.command()
@click.option('--host', '-H', help='use specified host rather than local machine', type=str, default=None)
def mounted(host):
    """List the nodes mounted on this, or another specified, machine"""
    import socket

    _init_config_db()

    if host is None:
        host = util.get_short_hostname()
    zero = True
    for node in (st.StorageNode.select()
                 .where(st.StorageNode.host == host, st.StorageNode.mounted)):
        n_file = ar.ArchiveFileCopy \
                   .select() \
                   .where(ar.ArchiveFileCopy.node == node) \
                   .count()
        print("%-25s %-30s %5d files" % (node.name, node.root, n_file))
        zero = False
    if zero:
        print("No nodes are mounted on host %s." % host)


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
    fp = os.popen("parted -s %s print" % dev)
    formatted = False
    part_start = False
    while True:
        l = fp.readline()
        if not l:
            break
        if l.find("Number") == 0 and l.find("Start") > 0 and l.find("File system") > 0:
            part_start = True
        elif l.strip() != "" and part_start:
            formatted = True
    fp.close()

    if not formatted:
        if not click.confirm("Disc is not formatted. Should I format it?"):
            return
        print("Creating partition. Please wait.")
        os.system("parted -s -a optimal %s mklabel gpt -- mkpart primary 0%% 100%%" % dev)
        print("Formatting disc. Please wait.")
        os.system("mkfs.ext4 %s -m 0 -L CH-%s" % (dev_part, serial_num))
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
        stat = os.system("/sbin/e2label %s %s" % (dev_part, name))
        if stat:
            print("Failed to e2label! Stat = %s. I quit." % (stat))
            exit(1)

    # Ensure the mount path exists.
    root = "/mnt/%s" % name
    if not os.path.isdir(root):
        print("Creating mount point %s." % root)
        os.mkdir(root)

    # Check to see if the disc is mounted.
    fp = os.popen("df")
    mounted = False
    dev_part_abs = os.path.realpath(dev_part)
    while 1:
        l = fp.readline()
        if not l:
            break
        if l.find(root) > 0:
            if l[:len(dev_part)] == dev or l[:len(dev_part_abs)] == dev_part_abs:
                mounted = True
            else:
                print("%s is a mount point, but %s is already mounted there."
                      (root, l.split()[0]))
    fp.close()

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

    print("Node created but not mounted. Run alpenhorn mount_transport for that.")


@cli.command()
@click.pass_context
@click.argument("node")
@click.option("--user", help="username to access this node.", type=str, default=None)
@click.option("--address", help="address for remote access to this node.", type=str, default=None)
def mount_transport(ctx, node, user, address):
    """Mount a transport disk into the system and then make it available to alpenhorn.
    """

    mnt_point = "/mnt/%s" % node

    print("Mounting disc at %s" % mnt_point)
    os.system("mount %s" % mnt_point)

    ctx.invoke(mount, name=node, path=mnt_point, user=user, address=address)


@cli.command()
@click.pass_context
@click.argument("node")
def unmount_transport(ctx, node):
    """Unmount a transport disk from the system and then remove it from alpenhorn.
    """

    mnt_point = "/mnt/%s" % node

    print("Unmounting disc at %s" % mnt_point)
    os.system("umount %s" % mnt_point)

    ctx.invoke(unmount, root_or_name=node)


@cli.command()
@click.argument("name")
@click.option("--path", help="Root path for this node", type=str, default=None)
@click.option("--user", help="username to access this node.", type=str, default=None)
@click.option("--address", help="address for remote access to this node.", type=str, default=None)
@click.option("--hostname", type=str, default=None,
              help="hostname running the alpenhornd instance for this node (set to this hostname by default).")
def mount(name, path, user, address, hostname):
    """Interactive routine for mounting a storage node located at ROOT."""

    import socket

    _init_config_db()

    try:
        node = st.StorageNode.get(name=name)
    except pw.DoesNotExist:
        click.echo("Storage node \"%s\" does not exist. I quit." % name)

    if node.mounted:
        click.echo("Node \"%s\" is already mounted." % name)
        return

    # Set the default hostname if required
    if hostname is None:
        hostname = util.get_short_hostname()
        click.echo("I will set the host to \"%s\"." % hostname)

    # Set the parameters of this node
    node.username = user
    node.address = address
    node.mounted = True
    node.host = hostname

    if path is not None:
        node.root = path

    node.save()

    click.echo("Successfully mounted \"%s\"." % name)


@cli.command()
@click.argument("root_or_name")
def unmount(root_or_name):
    """Unmount a storage node with location or named ROOT_OR_NAME."""
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

    if not node.mounted:
        click.echo("There is no node mounted there any more.")
    else:
        node.mounted = False
        node.save()
        print("Node successfully unmounted.")


@cli.command()
@click.argument('node_name', metavar='NODE')
@click.option('-v', '--verbose', count=True)
@click.option('--acq', help='Limit import to specified acquisition directories.', multiple=True, default=None)
@click.option('--dry', '-d', help='Dry run. Do not modify database.', is_flag=True)
def import_files(node_name, verbose, acq, dry):
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
    not_acqs = []  # Directories which were not known acquisitions

    # Fetch a reference to the node
    try:
        node = st.StorageNode.select().where(st.StorageNode.name == node_name).get()
    except pw.DoesNotExist:
        print("Unknown node.")
        return

    # Construct list of directories that might be acquisitions
    if len(acq) == 0:
        db_acqs = dict(ac.ArchiveAcq
                       .select(ac.ArchiveAcq.name, ac.ArchiveAcq.id)
                       .tuples())

        acqs = []           # Directories which are known acquisitions
        for d, ds, fs in os.walk(os.getcwd()):
            d = os.path.relpath(d)
            if d == '.':            # skip the current directory
                continue
            for acq in db_acqs.keys():
                if d == acq:
                    # the directory is the acquisition
                    acqs.append(d)
                    break
                if d.startswith(acq + "/"):
                    # the directory is inside an acquisition
                    break
                if acq.startswith(d + "/"):
                    # the directory is an acquisition's ancestor
                    break
            else:
                not_acqs.append(d)
    else:
        acqs = [acq]

    with click.progressbar(acqs, label='Scanning acquisitions') as acq_iter:

        for acq_name in acq_iter:
            try:
                acq = ac.ArchiveAcq.select().where(ac.ArchiveAcq.name == acq_name).get()
            except pw.DoesNotExist:
                not_acqs.append(acq_name)
                continue

            files = []
            for d, ds, fs in os.walk(acq_name):
                d = os.path.relpath(d, acq_name)
                if d == '.':
                    d = ''
                else:
                    d += '/'
                for f in fs:
                    files.append(d + f)

            # Fetch lists of all files in this acquisition, and all
            # files in this acq with local copies
            file_names = [f.name for f in acq.files]
            local_file_names = [f.name for f in acq.files.join(ar.ArchiveFileCopy).where(ar.ArchiveFileCopy.node == node)]

            for f_name in files:
                file_path = os.path.join(acq.name, f_name)

                # Check if file exists in database
                if f_name not in file_names:
                    unknown_files.append(file_path)
                    continue

                # Check if file is already registered on this node
                if f_name in local_file_names:
                    registered_files.append(file_path)
                else:
                    archive_file = ac.ArchiveFile.select().where(ac.ArchiveFile.name == f_name, ac.ArchiveFile.acq == acq).get()

                    if (os.path.getsize(file_path) != archive_file.size_b):
                        corrupt_files.append(file_path)
                        continue

                    added_files.append(file_path)
                    if not dry:
                        ar.ArchiveFileCopy.create(file=archive_file, node=node, has_file='Y', wants_file='Y')

    # now find the minimum unknown acqs paths that we can report
    not_acqs_roots = []
    last_acq_root = ''
    for d in sorted(not_acqs):
        common = os.path.commonprefix([last_acq_root, d])
        if common == '':
            not_acqs_roots.append(d)
            last_acq_root = d

    print("\n==== Summary ====")
    print()
    print("Added %i files" % len(added_files))
    print()
    print("%i corrupt files." % len(corrupt_files))
    print("%i files already registered." % len(registered_files))
    print("%i files not known" % len(unknown_files))
    print("%i directories were not acquisitions." % len(not_acqs_roots))

    if verbose > 0:
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
@click.option('--mounted', help='Is the node mounted?', metavar="BOOL",
              type=bool, default=False)
@click.option('--auto_import', help='Should files that appear on this node be \
              automatically added?', metavar='BOOL', type=bool, default=False)
@click.option('--suspect', help='Is this node corrupted?',
              metavar='BOOL', type=bool, default=False)
@click.option('--storage_type', help='What is the type of storage? Options:\
                A - archive for the data, T - for transiting data \
                F - for data in the field (i.e acquisition machines)',
              type=str, default='A')
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
def create_node(node_name, root, hostname, group, address, mounted, auto_import,
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
                              address=address, group=this_group.id, mounted=mounted,
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
    import os

    pin, pout, perr = os.popen3("/sbin/e2label %s" % dev, "r")
    pin.close()
    res = pout.read().strip()
    err = perr.read()
    pout.close()
    perr.close()
    if not len(err) and len(res) < MAX_E2LABEL_LEN:
        return res
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
