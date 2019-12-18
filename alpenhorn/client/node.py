"""Alpenhorn client interface for operations on `StorageNode`s."""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import click
import peewee as pw

import alpenhorn.acquisition as ac
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.util as util

from .connect_db import config_connect


@click.group(context_settings={'help_option_names': ['-h', '--help']})
def cli():
    """Commands operating on storage nodes. Use to create, modify, mount drives, etc."""
    pass


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
def create(node_name, root, hostname, group, address, active, auto_import,
           suspect, storage_type, max_total_gb, min_avail_gb,
           min_delete_age_days, notes):
    """Create a storage NODE within storage GROUP with a ROOT directory on
    HOSTNAME.
    """
    config_connect()

    try:
        this_group = st.StorageGroup.get(name=group)
    except pw.DoesNotExist:
        print('Requested group "%s" does not exit in DB.' % group)
        exit(1)

    try:
        this_node = st.StorageNode.get(name=node_name)
        print('Node name "%s" already exists! Try a different name!' % node_name)
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


@cli.command()
@click.argument("name")
@click.option("--path", help="Root path for this node", type=str, default=None)
@click.option("--user", help="username to access this node.", type=str, default=None)
@click.option("--address", help="address for remote access to this node.", type=str, default=None)
@click.option("--hostname", type=str, default=None,
              help="hostname running the alpenhornd instance for this node (set to this hostname by default).")
def activate(name, path, user, address, hostname):
    """Interactive routine for activating a storage node located at ROOT."""

    config_connect()

    try:
        node = st.StorageNode.get(name=name)
    except pw.DoesNotExist:
        click.echo('Storage node "%s" does not exist. I quit.' % name)
        exit(1)

    if node.active:
        click.echo('Node "%s" is already active.' % name)
        return

    if path is not None:
        node.root = path

    if not util.alpenhorn_node_check(node):
        click.echo('Node "{}" does not match ALPENHORN_NODE'.format(node.name))
        exit(1)

    # Set the default hostname if required
    if hostname is None:
        hostname = util.get_short_hostname()
        click.echo('I will set the host to "%s".' % hostname)

    # Set the parameters of this node
    node.username = user
    node.address = address
    node.active = True
    node.host = hostname

    node.save()

    click.echo('Successfully activated "%s".' % name)


@cli.command()
@click.argument("root_or_name")
def deactivate(root_or_name):
    """Deactivate a storage node with location or named ROOT_OR_NAME."""
    import os

    config_connect()

    try:
        node = st.StorageNode.get(name=root_or_name)
    except pw.DoesNotExist:
        if root_or_name[-1] == "/":
            root_or_name = root_or_name[: len(root_or_name) - 1]

        if not os.path.exists(root_or_name):
            click.echo(
                'That is neither a node name, nor a path on this host. I quit.'
            )
            exit(1)
        try:
            node = st.StorageNode.get(root=root_or_name,
                                      host=util.get_short_hostname())
        except pw.DoesNotExist:
            click.echo(
                'That is neither a node name, nor a root name that is known. I quit.'
            )
            exit(1)

    if not node.active:
        click.echo("There is no active node there any more.")
    else:
        node.active = False
        node.save()
        print("Node successfully deactivated.")


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

    config_connect()

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

        exit(status)


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

    config_connect()

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
            raise RuntimeError('Target group "%s" does not exist in the DB.' % target)

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

