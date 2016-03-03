"""Alpenhorn client interface."""

import sys
import os
import datetime

import click
import peewee as pw

from ch_util import data_index as di
from ch_util import ephemeris


@click.group()
def cli():
    """Client interface for alpenhorn. Use to request transfers, mount drives,
    check status etc."""
    pass


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

    # Make sure we connect RW
    di.connect_database(read_write=True)

    try:
        from_node = di.StorageNode.get(name=node_name)
    except pw.DoesNotExist:
        raise Exception("Node \"%s\" does not exist in the DB." % node_name)
    try:
        to_group = di.StorageGroup.get(name=group_name)
    except pw.DoesNotExist:
        raise Exception("Group \"%s\" does not exist in the DB." % group_name)

    # Construct list of file copies that are available on the source node, and
    # not available on any nodes at the destination. This query is quite complex
    # so I've broken it up...

    # First get the nodes at the destination...
    nodes_at_dest = di.StorageNode.select().where(di.StorageNode.group == to_group)

    # Then use this to get a list of all files at the destination...
    files_at_dest = di.ArchiveFile.select().join(di.ArchiveFileCopy).where(
        di.ArchiveFileCopy.node << nodes_at_dest,
        di.ArchiveFileCopy.has_file == 'Y'
    )

    # Then combine to get all file(copies) that are available at the source but
    # not at the destination...
    copy = di.ArchiveFileCopy.select().where(
        di.ArchiveFileCopy.node == from_node,
        di.ArchiveFileCopy.has_file == 'Y',
        ~(di.ArchiveFileCopy.file << files_at_dest))

    # If the target option has been specified, only copy nodes also not
    # available there...
    if target is not None:

        # Fetch a reference to the target group
        try:
            target_group = di.StorageGroup.get(name=target)
        except pw.DoesNotExist:
            raise RuntimeError("Target group \"%s\" does not exist in the DB." % target)

        # First get the nodes at the destination...
        nodes_at_target = di.StorageNode.select().where(di.StorageNode.group == target_group)

        # Then use this to get a list of all files at the destination...
        files_at_target = di.ArchiveFile.select().join(di.ArchiveFileCopy).where(
            di.ArchiveFileCopy.node << nodes_at_target,
            di.ArchiveFileCopy.has_file == 'Y'
        )

        # Only match files that are also not available at the target
        copy = copy.where(~(di.ArchiveFileCopy.file << files_at_target))

    # In transport mode (DEPRECATED) we only move files that don't have an
    # archive copy elsewhere...
    if transport:
        import warnings
        warnings.warn('Transport mode is deprecated. Try to use --target instead.')

        # Get list of other archive nodes
        other_archive_nodes = di.StorageNode.select().where(
            di.StorageNode.storage_type == "A",
            di.StorageNode.id != from_node
        )

        files_in_archive = di.ArchiveFile.select().join(di.ArchiveFileCopy).where(
            di.ArchiveFileCopy.node << other_archive_nodes,
            di.ArchiveFileCopy.has_file == "Y"
        )

        copy = copy.where(~(di.ArchiveFileCopy.file << files_in_archive))

    # Join onto ArchiveFile for later query parts
    copy = copy.join(di.ArchiveFile)

    # If requested, limit query to a specific acquisition...
    if acq is not None:

        # Fetch acq if specified
        try:
            acq = di.ArchiveAcq.get(name=acq)
        except pw.DoesNotExist:
            raise Exception("Acquisition \"%s\" does not exist in the DB." % acq)

        # Restrict files to be in the acquisition
        copy = copy.where(di.ArchiveFile.acq == acq)

    if not copy.count():
        print "No files to copy from node %s." % (node_name)
        return

    # Show acquisitions based summary of files to be copied
    if show_acq:
        acqs = [c.file.acq.name for c in copy]

        import collections
        for acq, count in collections.Counter(acqs).items():
            print "%s [%i files]" % (acq, count)

    # Show all files to be copied
    if show_files:
        for c in copy:
            print "%s/%s" % (c.file.acq.name, c.file.name)

    size_bytes = copy.aggregate(pw.fn.Sum(di.ArchiveFile.size_b))
    size_gb = int(size_bytes) / 1073741824.0

    print ('Will request that %d files (%.1f GB) be copied from node %s to group %s.' %
           (copy.count(), size_gb, node_name, group_name))

    if not (force or click.confirm("Do you want to proceed?")):
        print "Aborted."
        return

    dtnow = datetime.datetime.now()

    # Perform update in a transaction to avoid any clobbering from concurrent updates
    with di.ArchiveFileCopyRequest._meta.database.atomic():

        # Get a list of all the file ids for the copies we should perform
        files_ids = [c.file_id for c in copy]

        # Get a list of all the file ids for exisiting requests
        requests = di.ArchiveFileCopyRequest.select().where(
            di.ArchiveFileCopyRequest.group_to == to_group,
            di.ArchiveFileCopyRequest.node_from == from_node
        )
        req_file_ids = [req.file_id for req in requests]

        # Separate the files into ones that already have requests and ones that don't
        files_in = filter(lambda x: x in req_file_ids, files_ids)
        files_out = filter(lambda x: x not in req_file_ids, files_ids)

        sys.stdout.write("Updating %i existing requests and inserting %i new ones.\n" % (len(files_in), len(files_out)))

        # Perform an update of all the existing copy requests
        if len(files_in) > 0:
            update = di.ArchiveFileCopyRequest.update(nice=nice, completed=False, cancelled=False, timestamp=dtnow,
                                                      n_requests=di.ArchiveFileCopyRequest.n_requests + 1)

            update = update.where(di.ArchiveFileCopyRequest.file << files_in,
                                  di.ArchiveFileCopyRequest.group_to == to_group,
                                  di.ArchiveFileCopyRequest.node_from == from_node)
            update.execute()

        # Insert any new requests
        if len(files_out) > 0:

            # Construct a list of all the rows to insert
            insert = [{ 'file': fid, 'node_from': from_node, 'nice': 0,
                        'group_to': to_group, 'completed': False,
                        'n_requests': 1, 'timestamp': dtnow} for fid in files_out]

            # Do a bulk insert of these new rows
            di.ArchiveFileCopyRequest.insert_many(insert).execute()


@cli.command()
def status(width=80):
    """give a short summary of the archive status"""
    col1 = 15
    col2 = 6
    col3 = width - col1 - col2 - 10

    print
    print "Summary of Data Index at %s." % datetime.datetime.now()
    hline = "+-%-*s-+-%*s-+-%-*s-+" % (col1, "-" * col1, col2, "-" * col2,
                                       col3, "-" * col3)
    print hline
    print "| %-*s | %*s | %-*s |" % (col1, "Node", col2, "N File", col3,
                                     "Mount Point")
    print hline
    for node in di.StorageNode.select():
        n_file = di.ArchiveFileCopy.select().where(
            di.ArchiveFileCopy.node == node).count()
        if node.mounted:
            mount_point = "%s:%s" % (node.host, node.root)
        else:
            mount_point = "<unmounted>"
        print "| %-*s | %*d | %-*s |" % (col1, node.name, col2, n_file, col3,
                                         mount_point)
    print hline
    print


@cli.command()
@click.argument('node_name', metavar='NODE')
@click.option('--md5', help='perform full check against md5sum', is_flag=True)
@click.option('--fixdb', help='fix up the database to be consistent with reality', is_flag=True)
def verify(node_name, md5, fixdb):
    """Verify the archive on NODE against the database.
    """

    import os

    try:
        this_node = di.StorageNode.get(di.StorageNode.name == node_name)
    except pw.DoesNotExist:
        print "Specified node does not exist."
        return

    ## Use a complicated query with a tuples construct to fetch everything we
    ## need in a single query. This massively speeds up the whole process versus
    ## fetching all the FileCopy's then querying for Files and Acqs.
    lfiles = di.ArchiveFile\
               .select(di.ArchiveFile.name, di.ArchiveAcq.name,
                          di.ArchiveFile.size_b, di.ArchiveFile.md5sum,
                          di.ArchiveFileCopy.id)\
               .join(di.ArchiveAcq)\
               .switch(di.ArchiveFile)\
               .join(di.ArchiveFileCopy)\
               .where(di.ArchiveFileCopy.node == this_node,
                      di.ArchiveFileCopy.has_file == 'Y')\
               .tuples()
    nfiles = lfiles.count()

    missing_files = []
    corrupt_files = []

    missing_ids = []
    corrupt_ids = []

    # Try to use progress bar if available
    try:
        from progress.bar import Bar
        lfiles = Bar('Checking files', max=nfiles).iter(lfiles)
    except ImportError:
        pass

    for filename, acqname, filesize, md5sum, fc_id in lfiles:

        filepath = this_node.root + '/' + acqname + '/' + filename

        # Check if file is plain missing
        if not os.path.exists(filepath):
            missing_files.append(filepath)
            missing_ids.append(fc_id)
            continue

        if md5:
            file_md5 = di.md5sum_file(filepath)
            corrupt = (file_md5 != md5sum)
        else:
            corrupt = (os.path.getsize(filepath) != filesize)

        if corrupt:
            corrupt_files.append(filepath)
            corrupt_ids.append(fc_id)
            continue

    if len(missing_files) > 0:
        print
        print "=== Missing files ==="
        for fname in missing_files:
            print fname

    if len(corrupt_files) > 0:
        print
        print "=== Corrupt files ==="
        for fname in corrupt_files:
            print fname

    print
    print "=== Summary ==="
    print "  %i total files" % nfiles
    print "  %i missing files" % len(missing_files)
    print "  %i corrupt files" % len(corrupt_files)
    print

    # Fix up the database by marking files as missing, and marking
    # corrupt files for verification by alpenhornd.
    if fixdb:

        # Make sure we connect RW
        di.connect_database(read_write=True)

        if (len(missing_files) > 0) and click.confirm('Fix missing files'):
            missing_count = di.ArchiveFileCopy\
                              .update(has_file='N')\
                              .where(di.ArchiveFileCopy.id << missing_ids)\
                              .execute()
            print "  %i marked as missing" % missing_count

        if (len(corrupt_files) > 0) and click.confirm('Fix corrupt files'):
            corrupt_count = di.ArchiveFileCopy\
                              .update(has_file='M')\
                              .where(di.ArchiveFileCopy.id << corrupt_ids)\
                              .execute()
            print "  %i corrupt files marked for verification" % corrupt_count


@cli.command()
@click.argument('node_name', metavar='NODE')
@click.option('--days', '-d', help='clean files older than <days>', default=None)
@click.option('--force', '-f', help='force cleaning on an archive node', is_flag=True)
@click.option('--now', '-n', help='force immediate removal', is_flag=True)
@click.option('--target', metavar='TARGET_GROUP', default=None, type=str,
              help='Only clean files already available in this group.')
def clean(node_name, days, force, now, target):
    """Clean up NODE by marking older files as potentially removable.

    If --target is specified we will only remove files already available in the
    TARGET_GROUP. This is useful for cleaning out intermediate locations such as
    transport disks.

    Using the --days flag will only clean correlator and housekeeping files
    which have a timestamp associated with them. It will not touch other types.
    """

    import peewee as pw
    di.connect_database(read_write=True)

    try:
        this_node = di.StorageNode.get(di.StorageNode.name == node_name)
    except pw.DoesNotExist:
        print "Specified node does not exist."

    # Check to see if we are on an archive node
    if this_node.storage_type == 'A':
        if force or click.confirm('DANGER: run clean on archive node?'):
            print "%s is an archive node. Forcing clean." % node_name
        else:
            print "Cannot clean archive node %s without forcing." % node_name
            return

    # Select FileCopys on this node.
    files = di.ArchiveFileCopy.select(di.ArchiveFileCopy.id).where(
        di.ArchiveFileCopy.node == this_node,
        di.ArchiveFileCopy.wants_file == 'Y'
    )

    # If the target option has been specified, only clean files also available there...
    if target is not None:

        # Fetch a reference to the target group
        try:
            target_group = di.StorageGroup.get(name=target)
        except pw.DoesNotExist:
            raise RuntimeError("Target group \"%s\" does not exist in the DB." % target)

        # First get the nodes at the destination...
        nodes_at_target = di.StorageNode.select().where(di.StorageNode.group == target_group)

        # Then use this to get a list of all files at the destination...
        files_at_target = di.ArchiveFile.select().join(di.ArchiveFileCopy).where(
            di.ArchiveFileCopy.node << nodes_at_target,
            di.ArchiveFileCopy.has_file == 'Y'
        )

        # Only match files that are also available at the target
        files = files.where(di.ArchiveFileCopy.file << files_at_target)


    # If --days has been set we need to restrict to files older than the given
    # time. This only works for a few particular file types
    if days is not None and days > 0:

        # Get the time for the oldest files to keep
        oldest = datetime.datetime.now() - datetime.timedelta(days)
        oldest_unix = ephemeris.ensure_unix(oldest)

        # List of filetypes we want to update, needs a human readable name and a
        # FileInfo table.
        filetypes = [ ['correlation', di.CorrFileInfo],
                      ['housekeeping', di.HKFileInfo] ]

        file_ids = []

        # Iterate over file types for cleaning
        for name, infotable in filetypes:

            # Filter to fetch only ones with a start time older than `oldest`
            oldfiles = files.join(di.ArchiveFile).join(infotable)\
                .where(infotable.start_time < oldest_unix)

            local_file_ids = list(oldfiles)

            # Get number of correlation files
            count = oldfiles.count()

            if count > 0:
                size_bytes = di.ArchiveFileCopy.select().where(di.ArchiveFileCopy.id << local_file_ids)\
                    .join(di.ArchiveFile).aggregate(pw.fn.Sum(di.ArchiveFile.size_b))

                size_gb = int(size_bytes) / 2**30.0

                print "Cleaning up %i %s files (%.1f GB) from %s " % (count, name, size_gb, node_name)

                file_ids += local_file_ids

    # If days is not set, then just select all files that meet the requirements so far
    else:

        file_ids = list(files)
        count = files.count()

        if count > 0:
            size_bytes = di.ArchiveFileCopy.select().where(
                di.ArchiveFileCopy.id << file_ids
            ).join(di.ArchiveFile).aggregate(pw.fn.Sum(di.ArchiveFile.size_b))

            size_gb = int(size_bytes) / 1073741824.0

            print 'Cleaning up %i files (%.1f GB) from %s ' % (count, size_gb, node_name)

    # If there are any files to clean, ask for confirmation and the mark them in
    # the database for removal
    if len(file_ids) > 0:
        if force or click.confirm("  Are you sure?"):
            print "  Marking files for cleaning."

            state = 'N' if now else 'M'

            update = di.ArchiveFileCopy.update(wants_file=state)\
                .where(di.ArchiveFileCopy.id << file_ids)

            n = update.execute()

            print "Marked %i files for cleaning" % n

        else:
            print "  Cancelled"
    else:
        print "No files selected for cleaning on %s." % node_name


@cli.command()
@click.option('--host', '-H', help='use specified host rather than local machine', type=str, default=None)
def mounted(host):
    """list the nodes mounted on this, or another specified, machine"""
    import socket

    if host is None:
        host = socket.gethostname().split(".")[0]
    zero = True
    for node in di.StorageNode \
                  .select() \
                  .where(di.StorageNode.host == host, di.StorageNode.mounted == True):
        n_file = di.ArchiveFileCopy \
                   .select() \
                   .where(di.ArchiveFileCopy.node == node) \
                   .count()
        print "%-25s %-30s %5d files" % (node.name, node.root, n_file)
        zero = False
    if zero:
        print "No nodes are mounted on host %s." % host


@cli.command()
@click.argument("serial_num")
def mount_transport(serial_num):
    """Interactive routine for mounting a transport disc as a storage node;
    formats, labels and OS mounts the disc as necessary. The disk is specified
    using the manufacturers SERIAL_NUM, which is printed on the disk."""
    import os
    import glob

    if os.getuid() != 0:
        print "You must be root to run mount on a transport disc. I quit."
        return

    # Find the disc.
    dev = glob.glob("/dev/disk/by-id/*%s" % serial_num)
    if len(dev) == 0:
        print "No disc with that serial number is attached."
        return
    elif len(dev) > 1:
        print "Confused: found more than one device matching that serial number:"
        for d in dev:
            print "  %s" % dev
        print "Aborting."
        return
    dev = dev[0]
    dev_part = "%s-part1" % dev

    # Figure out if it is formatted.
    print "Checking to see if disc is formatted. Please wait."
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
        print "Creating partition. Please wait."
        os.system("parted -s -a optimal %s mklabel gpt -- mkpart primary 0%% 100%%" % dev)
        print "Formatting disc. Please wait."
        os.system("mkfs.ext4 %s -m 0 -L CH-%s" % (dev_part, serial_num))
    else:
        print "Disc is already formatted."

    e2label = get_e2label(dev_part)
    name = "CH-%s" % serial_num
    if e2label and e2label != name:
        print "Disc label %s does not conform to labelling standard, " \
              "which is CH-<serialnum>."
        exit
    elif not e2label:
        print "Labelling the disc as \"%s\" (using e2label) ..." % (name)
        assert dev_part is not None
        assert len(name) <= MAX_E2LABEL_LEN
        stat = os.system("/sbin/e2label %s %s" % (dev_part, name))
        if stat:
            print "Failed to e2label! Stat = %s. I quit." % (stat)
            exit()

    # Ensure the mount path exists.
    root = "/mnt/%s" % name
    if not os.path.isdir(root):
        print "Creating mount point %s." % root
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
                print "%s is a mount point, but %s is already mounted there." \
                      (root, l.split()[0])
    fp.close()
    if not mounted:
        print "Mounting disc at %s." % root
        os.system("mount %s %s" % (dev_part, root))

    try:
        node = di.StorageNode.get(name=name)
    except pw.DoesNotExist:
        print "This disc has not been registered yet as a storage node. " \
              "Registering now."
        try:
            group = di.StorageGroup.get(name="transport")
        except pw.DoesNotExist:
            print "Hmmm. Storage group \"transport\" does not exist. I quit."
            exit()

        # We need to write to the database.
        di.connect_database(read_write=True)
        node = di.StorageNode.create(name=name, root=root, group=group,
                                     storage_type="T", min_avail_gb=1)

        print "Successfully created storage node."

    _mount_work(root, None)


@cli.command()
@click.argument("root")
@click.option("--name", help="name of this node; only enter if it is not a storage node", type=str, default=None)
def mount(root, name):
    """Interactive routine for mounting a storage node located at ROOT."""
    _mount_work(root, name)


def _mount_work(root, name):
    # The implementation of the mount command. Factored out so it can also be called by mount_transport.

    import os
    import socket

    # We need to write to the database.
    di.connect_database(read_write=True)

    if root[-1] == "/":
        root = root[:len(root) - 1]

    if name and os.path.ismount(root):
        print "You should not enter a name if this is a transport disc." \
              "I quit."
        exit()
    elif not name:
        transport = True
        if os.getuid() != 0:
            print "You must be root to run mount on a transport disc. I quit."
            return
        if not os.path.ismount(root):
            print "You must enter a name if this is not a transport disc. " \
                  "I quit."
            exit()
        dev = get_mount_device(root)
        name = get_e2label(dev)
        if not name:
            print "Could not find disc label. This disc has probably never " \
                  "been formatted as a transport disc. I quit."
            exit()
    else:
        transport = False

    try:
        node = di.StorageNode.get(name=name)
    except pw.DoesNotExist:
        print "Storage node \"%s\" does not exist. I quit." % (name)

    if node.mounted:
        print "Node \"%s\" is already mounted." % (name)
        exit()

    node.host = socket.gethostname()
    print "I will set the host to \"%s\"." % (node.host)
    if transport:
        node.address = None
        node.username = None
    else:
        node.address = click.prompt("Enter the address of this host",
                                    default="localhost").strip()
        node.username = click.prompt("Enter the user name for rsyncing",
                                     default=os.environ.get("USER"))
    node.mounted = True
    node.root = root
    node.save()
    print "Successfully mounted \"%s\"." % (name)


@cli.command()
@click.argument("root_or_name")
def unmount(root_or_name):
    """Unmount a storage node with location or named ROOT_OR_NAME."""
    import os
    import socket

    # We need to write to the database.
    di.connect_database(read_write=True)

    try:
        node = di.StorageNode.get(name=root_or_name)
    except pw.DoesNotExist:
        if root_or_name[-1] == "/":
            root_or_name = root_or_name[:len(root_or_name) - 1]

        if not os.path.exists(root_or_name):
            print "That is neither a node name, nor a path on this host. " \
                  "I quit."
            exit()
        try:
            node = di.StorageNode.get(root=root_or_name,
                                      host=socket.gethostname())
        except pw.DoesNotExist:
            print "That is neither a node name nor a root name that is " \
                  "known. I quit."
            exit()

    if not node.mounted:
        print "There is no node mounted there any more."
    else:
        node.mounted = False
        node.save()
        print "Node successfully unmounted."


@cli.command()
@click.argument('node_name', metavar='NODE')
def import_files(node_name):
    """Scan the current directory for known acquisition files and add them into the database for NODE.

    This command is useful for manually maintaining an archive where we can run
    alpenhornd in the usual manner.
    """
    import glob
    from ch_util import data_index as di
    di.connect_database(read_write=True)
    import peewee as pw

    print "Scanning directory...",
    acqs = glob.glob('*')
    print "done."

    # Fetch a reference to the node
    try:
        node = di.StorageNode.select().where(di.StorageNode.name == node_name)
    except pw.DoesNotExist:
        print "Unknown node."
        return

    for acq_name in acqs:

        print "Processing acq %s ..." % acq_name,

        try:
            di.parse_acq_name(acq_name)
        except di.Validation:
            print 'not acquisition'
            continue

        try:
            acq = di.ArchiveAcq.select().where(di.ArchiveAcq.name == acq_name).get()
        except pw.DoesNotExist:
            print 'not found'
            continue

        print 'scanning'

        files = glob.glob(acq_name + '/*')

        for fn in files:

            f_name = os.path.split(fn)[1]

            print f_name,

            try:
                archive_file = di.ArchiveFile.select().where(di.ArchiveFile.name == f_name, di.ArchiveFile.acq == acq).get()
            except pw.DoesNotExist:
                print "cannot find"
                continue

            copies = di.ArchiveFileCopy.select().where(di.ArchiveFileCopy.file == archive_file, di.ArchiveFileCopy.node == node)

            if copies.count() == 0:

                if (os.path.getsize(fn) != archive_file.size_b):
                    print "is corrupt"
                    continue

                print "adding to database"
                di.ArchiveFileCopy.create(file=archive_file, node=node, has_file='Y', wants_file='Y')

            else:
                print "already in database"


# A few utitly routines for dealing with filesystems
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


def get_mount_device(path):
    import os

    p = os.popen("mount", "r")
    res = p.read()
    p.close()
    dev = None
    for l in res.split("\n"):
        if not len(l):
            continue
        s = l.split()
        assert s[1] == "on"
        if s[2] == os.path.abspath(path):
            dev = s[0]
    return dev
