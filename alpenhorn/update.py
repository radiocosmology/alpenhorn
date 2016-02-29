"""Routines for updating the state of a node.
"""
import os
import time
import datetime
import re

import peewee as pw

from ch_util import data_index as di

# Setup the logging
from . import logger
log = logger.get_log()

# Parameters.
max_time_per_node_operation = 300   # Don't let node operations hog time.
min_loop_time = 60    # Main loop at most every 60 seconds.

RSYNC_FLAG = "qtspgoDL"

# Globals.
done_transport_this_cycle = False


def run_command(cmd, **kwargs):
    """Run a command.

    Parameters
    ----------
    cmd : string or list
        A command as a string or list, to be understood by `subprocess.Popen`.
    kwargs : dict
        Passed directly onto `subprocess.Popen.`

    Returns
    -------
    retval : int
        Return code.
    stdout_val : string
        Value of stdout.
    stderr_val : string
        Value of stderr.
    """

    import subprocess

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    stdout_val, stderr_val = proc.communicate()
    retval = proc.returncode

    return retval, stdout_val, stderr_val


def is_md5_hash(h):
    """Is this the correct format to be an md5 hash."""
    return re.match('[a-f0-9]{32}', h) is not None


def command_available(cmd):
    """Is this command available on the system.
    """
    from distutils import spawn
    return spawn.find_executable(cmd) is not None


def update_loop(node_list, host):
    """Loop over nodes performing any updates needed.
    """
    global done_transport_this_cycle

    while True:
        loop_start = time.time()
        done_transport_this_cycle = False

        # Iterate over nodes and perform each update (perform a new query
        # each time in case we get a new node, e.g. transport disk)
        for node in di.StorageNode.select().where(di.StorageNode.host == host):
            update_node(node)

        # Check the time spent so far, and wait if needed
        loop_time = time.time() - loop_start
        log.info("Main loop execution was %d sec." % loop_time)
        remaining = min_loop_time - loop_time
        if remaining > 1:
            time.sleep(remaining)


def update_node_free_space(node):
    """Calculate the free space on the node and update the database with it."""

    # Check with the OS how much free space there is
    x = os.statvfs(node.root)
    avail_gb = float(x.f_bavail) * x.f_bsize / 2**30.0

    # Update the DB with the free space
    node.avail_gb = avail_gb
    node.avail_gb_last_checked = datetime.datetime.now()
    node.save()

    log.info("Node \"%s\" has %.2f GB available." % (node.name, avail_gb))


def update_node_integrity(node):
    """Check the integrity of file copies on the node."""

    # Find suspect file copies in the database
    fcopy_query = di.ArchiveFileCopy.select().where(
        di.ArchiveFileCopy.node == node,
        di.ArchiveFileCopy.has_file == 'M').limit(25)

    # Loop over these file copies and check their md5sum
    for fcopy in fcopy_query:
        fullpath = "%s/%s/%s" % (node.root, fcopy.file.acq.name, fcopy.file.name)
        log.info("Checking file \"%s\" on node \"%s\"." % (fullpath, node.name))

        # If the file exists calculate its md5sum and check against the DB
        if os.path.exists(fullpath):
            if di.md5sum_file(fullpath) == fcopy.file.md5sum:
                log.info("File is A-OK!")
                di.ArchiveFileCopy.update(has_file='Y').where(
                    di.ArchiveFileCopy.id == fcopy.id).execute()
            else:
                log.error("File is corrupted!")
                di.ArchiveFileCopy.update(has_file='X').where(
                    di.ArchiveFileCopy.id == fcopy.id).execute()
        else:
            log.error("File does not exist!")
            fcopy.has_file == 'N'
            di.ArchiveFileCopy.update(has_file='N').where(
                di.ArchiveFileCopy.id == fcopy.id).execute()


def update_node_delete(node):
    """Process this node for files to delete."""

    # If we are below the minimum available size, we should consider all files
    # not explicitly wanted (wants_file != 'Y') as candidates for deletion,
    # otherwise only those explicitly marked (wants_file == 'N')
    # Also do not clean on archive type nodes.
    if node.avail_gb < node.min_avail_gb and node.storage_type != 'A':
        log.info("Hit minimum available space on %s -- considering all unwanted "
                 "files for deletion!" % (node.name))
        dfclause = di.ArchiveFileCopy.wants_file != 'Y'
    else:
        dfclause = di.ArchiveFileCopy.wants_file == 'N'

    # Search db for candidates on this node to delete.
    del_files = di.ArchiveFileCopy.select().where(
        dfclause,
        di.ArchiveFileCopy.node == node,
        di.ArchiveFileCopy.has_file == 'Y')

    # Process candidates for deletion
    del_count = 0  # Counter for no. of deletions (limits no. per node update)
    for fcopy in del_files.order_by(di.ArchiveFileCopy.id):

        # Limit number of deletions to 100 per main loop iteration.
        if del_count >= 100:
            break

        # Get all the *other* copies.
        other_copies = fcopy.file.copies.where(di.ArchiveFileCopy.id != fcopy.id)

        # Get the number of copies on archive nodes
        ncopies = other_copies.join(di.StorageNode) \
                              .where(di.StorageNode.storage_type == 'A').count()

        shortname = "%s/%s" % (fcopy.file.acq.name, fcopy.file.name)
        fullpath = "%s/%s/%s" % (node.root, fcopy.file.acq.name, fcopy.file.name)

        # If at least two other copies we can delete the file.
        if ncopies >= 2:

            # Use transaction such that errors thrown in the os.remove do not leave
            # the database inconsistent.
            with di.database_proxy.transaction():
                if os.path.exists(fullpath):
                    os.remove(fullpath)  # Remove the actual file

                fcopy.has_file = 'N'
                fcopy.wants_file = 'N'  # Set in case it was 'M' before
                fcopy.save()  # Update the FileCopy in the database

                log.info("Removed file copy: %s" % shortname)

            del_count += 1

        else:
            log.info("Too few backups to delete %s" % shortname)


def update_node_requests(node):
    """Process file copy requests onto this node."""

    global done_transport_this_cycle

    # Ensure we are not on an HPSS node
    if is_hpss_node(node):
        log.error("Cannot process HPSS node here.")
        return

    # Check for copy requests (only if this node has not hit the maximum size).
    current_size_gb = 0.0  # Set to zero for the moment. This should be replaced
    # with the actual archive size at somepoint.

    # Note that for transport discs, we only copy to one at a time, so that
    # archives are not striped across multiple discs. This clause enforces that.
    if ((current_size_gb > node.max_total_gb and node.max_total_gb < 0.0) or
       (node.storage_type == "T" and done_transport_this_cycle)):
        return

    start_time = time.time()

    # Fetch requests to process from the database
    requests = di.ArchiveFileCopyRequest.select().where(
        ~di.ArchiveFileCopyRequest.completed,
        ~di.ArchiveFileCopyRequest.cancelled,
        di.ArchiveFileCopyRequest.group_to == node.group
    )

    # Add in constraint that node_from cannot be an HPSS node
    requests = requests.join(di.StorageNode).where(di.StorageNode.address != 'HPSS')

    for req in requests:

        # Only continue if the node is actually mounted
        if not req.node_from.mounted:
            continue

        # For transport disks we should only copy onto the transport
        # node if the from_node is local, this should prevent pointlessly
        # rsyncing across the network
        if node.storage_type == "T" and node.host != req.node_from.host:
            log.info("Skipping request for %s/%s from remote node [%s] onto local "
                     "transport disks" % (req.file.acq.name, req.file.name,
                                          req.node_from.name))

        # Only proceed if the source file actually exists (and is not corrupted).
        try:
            di.ArchiveFileCopy.get(di.ArchiveFileCopy.file == req.file,
                                   di.ArchiveFileCopy.node == req.node_from,
                                   di.ArchiveFileCopy.has_file == 'Y')
        except pw.DoesNotExist:
            log.error("Skipping request for %s/%s since it is not available on "
                      "node \"%s\". [file_id=%i]" % (req.file.acq.name,
                                                     req.file.name,
                                                     req.node_from.name,
                                                     req.file.id))
            continue

        # Only proceed if the destination file does not already exist.
        try:
            di.ArchiveFileCopy.get(di.ArchiveFileCopy.file == req.file,
                                   di.ArchiveFileCopy.node == node,
                                   di.ArchiveFileCopy.has_file == 'Y')
            log.info("Skipping request for %s/%s since it already exists on "
                     "this node (\"%s\"), and updating DB to reflect this." %
                     (req.file.acq.name, req.file.name, node.name))
            di.ArchiveFileCopyRequest.update(completed=True).where(
                di.ArchiveFileCopyRequest.file == req.file).where(
                di.ArchiveFileCopyRequest.group_to ==
                node.group).execute()
            continue
        except pw.DoesNotExist:
            pass

        # Check that there is enough space available.
        if node.avail_gb * 2 ** 30.0 < 2.0 * req.file.size_b:
            log.warning("Node \"%s\" is full: not adding datafile \"%s/%s\"." %
                        (node.name, req.file.acq.name, req.file.name))
            continue

        # Constuct the origin and destination paths.
        from_path = "%s/%s/%s" % (req.node_from.root, req.file.acq.name,
                                  req.file.name)
        if req.node_from.host != node.host:
            from_path = "%s@%s:%s" % (req.node_from.username,
                                      req.node_from.address, from_path)

        to_path = "%s/%s/" % (node.root, req.file.acq.name)
        if not os.path.isdir(to_path):
            log.info("Creating directory \"%s\"." % to_path)
            os.mkdir(to_path)

        # Giddy up!
        log.info("Transferring file \"%s/%s\"." % (req.file.acq.name, req.file.name))
        st = time.time()

        # Attempt to transfer the file. Each of the methods below needs to set a
        # return code `ret` and give an `md5sum` of the transferred file.

        # First we need to check if we are copying over the network
        if req.node_from.host != node.host:

            # First try bbcp which is a fast multistream transfer tool. bbcp can
            # calculate the md5 hash as it goes, so we'll do that to save doing
            # it at the end.
            if command_available('bbcp'):
                cmd = 'bbcp -f -z --port 1500 -W 4M -s 8 -o -E md5= %s %s' % (from_path, to_path)
                ret, stdout, stderr = run_command(cmd.split())

                # Attempt to parse STDERR for the md5 hash
                if ret == 0:
                    mo = re.search('md5 ([a-f0-9]{32})', stderr)
                    if mo is None:
                        log.error('BBCP transfer has gone awry. STDOUT: %s\n STDERR: %s' % (stdout, stderr))
                        ret = -1
                    md5sum = mo.group(1)
                else:
                    md5sum = None

            # Next try rsync over ssh. We need to explicitly calculate the md5
            # hash after the fact
            elif command_available('rsync'):
                cmd = ("rsync -z%s --rsync-path=\"ionice -c4 -n4 rsync\" -e \"ssh -q\" %s %s" %
                       (RSYNC_FLAG, from_path, to_path))
                ret, stdout, stderr = run_command(cmd.split())

                md5sum = di.md5sum_file("%s/%s" % (to_path, req.file.name)) if ret == 0 else None

            # If we get here then we have no idea how to transfer the file...
            else:
                log.warn("No commands available to complete this transfer.")
                ret = -1

        # Okay, great we're just doing a local transfer.
        else:

            # First try to just hard link the file. This will only work if we
            # are on the same filesystem. As there's no actual copying it's
            # probably unecessary to calculate the md5 check sum, so we'll just
            # fake it.
            try:
                link_path = "%s/%s/%s" % (node.root, req.file.acq.name, req.file.name)

                # Check explicitly if link already exists as this and
                # being unable to link will both raise OSError and get
                # confused.
                if os.path.exists(link_path):
                    log.error('File %s already exists. Clean up manually.' % link_path)
                    ret = -1
                else:
                    os.link(from_path, link_path)
                    ret = 0
                    md5sum = req.file.md5sum  # As we're linking the md5sum can't change. Skip the check here...

            # If we couldn't just link the file, try copying it with rsync.
            except OSError:
                if command_available('rsync'):
                    cmd = "rsync -%s %s %s" % (RSYNC_FLAG, from_path, to_path)
                    ret, stdout, stderr = run_command(cmd.split())

                    md5sum = di.md5sum_file("%s/%s" % (to_path, req.file.name)) if ret == 0 else None
                else:
                    log.warn("No commands available to complete this transfer.")
                    ret = -1

        # Check the return code...
        if ret:
            # If the copy didn't work, then the remote file may be corrupted.
            log.error("Rsync failed. Marking source file suspect.")
            di.ArchiveFileCopy.update(has_file='M').where(
                di.ArchiveFileCopy.file == req.file,
                di.ArchiveFileCopy.node == req.node_from).execute()
            continue
        et = time.time()

        # Check integrity.
        if md5sum == req.file.md5sum:
            size_mb = req.file.size_b / 2**20.0
            trans_time = et - st
            rate = size_mb / trans_time
            log.info("Pull complete (md5sum correct). Transferred %.1f MB in %i "
                     "seconds [%.1f MB/s]" % (size_mb, int(trans_time), rate))

            # Update the FileCopy (if exists), or insert a new FileCopy
            try:
                done = False
                while not done:
                    try:
                        fcopy = di.ArchiveFileCopy\
                                  .select()\
                                  .where(di.ArchiveFileCopy.file == req.file,
                                         di.ArchiveFileCopy.node == node)\
                                  .get()
                        fcopy.has_file = 'Y'
                        fcopy.wants_file = 'Y'
                        fcopy.save()
                        done = True
                    except pw.OperationalError:
                        log.error("MySQL connexion dropped. Will attempt to reconnect in "
                                  "five seconds.")
                        time.sleep(5)
                        di.connect_database(True)
            except pw.DoesNotExist:
                di.ArchiveFileCopy.insert(file=req.file, node=node, has_file='Y',
                                          wants_file='Y').execute()

            # Mark any FileCopyRequest for this file as completed
            di.ArchiveFileCopyRequest.update(completed=True).where(
                di.ArchiveFileCopyRequest.file == req.file).where(
                di.ArchiveFileCopyRequest.group_to == node.group).execute()

            if node.storage_type == "T":
                # This node is getting the transport king.
                done_transport_this_cycle = True
        else:
            log.error("Error with md5sum check: %s on node \"%s\", but %s on "
                      "this node, \"%s\"." % (req.file.md5sum, req.node_from.name,
                                              md5sum, node.name))
            log.error("Removing file \"%s/%s\"." % (to_path, req.file.name))
            try:
                pass
                #os.remove("%s/%s" % (to_path, req.file.name))
            except:
                log.error("Could not remove file.")

            # Since the md5sum failed, the remote file may be corrupted.
            log.error("Marking source file suspect.")
            di.ArchiveFileCopy.update(has_file='M').where(
                di.ArchiveFileCopy.file == req.file,
                di.ArchiveFileCopy.node == req.node_from).execute()

        if time.time() - start_time > max_time_per_node_operation:
            break  # Don't hog all the time.


def update_node(node):
    """Update the status of the node, and process eligible transfers onto it.
    """

    # Check if this is an HPSS node, and if so call the special handler
    if is_hpss_node(node):
        update_node_hpss_inbound(node)
        return

    # Make sure this node is usable.
    if not node.mounted:
        log.debug("Skipping unmounted node \"%s\"." % node.name)
        return
    if node.suspect:
        log.debug("Skipping suspected node \"%s\"." % node.name)

    log.info("Updating node \"%s\"." % (node.name))

    # Check and update the amount of free space
    update_node_free_space(node)

    # Check the integrity of any questionable files (has_file=M)
    update_node_integrity(node)

    # Delete any upwanted files to cleanup space
    update_node_delete(node)

    # Process any regular transfers requests onto this node
    update_node_requests(node)

    # Process any tranfers out of HPSS onto this node
    update_node_hpss_outbound(node)


def is_hpss_node(node):
    """Test if a node is an HPSS tape node or not."""
    return (node.address == 'HPSS')


def _check_and_bundle_requests(requests, node):
    """Find eligible HPSS transfer requests, and return a bundle of them up to
    some maximum size."""

    # Size to bundle transfers into (in bytes)
    max_group_size = 800.0 * 2**30.0

    group_size = 0.0
    requests_to_process = []

    # Construct list of requests to process by finding eligible requests up to
    # the maximum single transfer size
    for req in requests:

        # Check to ensure both source and dest nodes are on the same host
        if req.node_from.host != node.host:
            log.error('Source file is not on this host [request_id=%i].' % req.id)
            continue

        # Check that there is actually a copy of the file at the source
        filecopy_src = di.ArchiveFileCopy.select().where(di.ArchiveFileCopy.file == req.file,
                                                         di.ArchiveFileCopy.node == req.node_from,
                                                         di.ArchiveFileCopy.has_file == 'Y')
        if not filecopy_src.exists():
            log.error("Skipping request for %s/%s since it is not available on "
                      "node \"%s\". [file_id=%i]" % (req.file.acq.name,
                                                     req.file.name,
                                                     req.node_from.name,
                                                     req.file.id))
            continue

        # Check if there is already a copy at the destination, and skip the request if there is
        filecopy_dst = di.ArchiveFileCopy.select().where(di.ArchiveFileCopy.file == req.file,
                                                         di.ArchiveFileCopy.node == node,
                                                         di.ArchiveFileCopy.has_file == 'Y')
        if filecopy_dst.exists():

            log.info("Skipping request for %s/%s since it already exists on "
                     "this node (\"%s\"), and updating DB to reflect this." %
                     (req.file.acq.name, req.file.name, node.name))
            di.ArchiveFileCopyRequest.update(completed=True).where(
                di.ArchiveFileCopyRequest.file == req.file,
                di.ArchiveFileCopyRequest.group_to == node.group).execute()
            continue

        # Ensure that we only attempt to transfer into HPSS online from an HPSS offline node
        if node.group.name == 'hpss_online' and req.node_from.group.name != 'hpss_offline':
            log.error("Can only transfer into hpss_online group from hpss_offline." +
                      "Skipping request of %s/%s from node \"%s\" to node \"%s\" [file_id=%i]" %
                      (req.file.acq.name, req.file.name, req.node_from.name, node.name, req.file.id))
            continue

        # Add the request into the list to process (provided we haven't hit the maximum transfer size)
        if group_size + req.file.size_b < max_group_size:
            requests_to_process.append(req)
            group_size += req.file.size_b
        else:
            break

    return requests_to_process


def update_node_hpss_inbound(node):
    """Process transfers into an HPSS node.
    """

    if not is_hpss_node(node):
        log.error('This is not an HPSS node.')

    log.info('Processing HPSS inbound transfers (%s)' % node.name)

    # Fetch requests for transfer onto this node
    requests = di.ArchiveFileCopyRequest.select().where(
        ~di.ArchiveFileCopyRequest.completed,
        ~di.ArchiveFileCopyRequest.cancelled,
        di.ArchiveFileCopyRequest.group_to == node.group
    )

    # Get the requests we should actually process
    requests_to_process = _check_and_bundle_requests(requests, node)

    # Exit if there are no requests to process
    if len(requests_to_process) == 0:
        return

    # Construct final list of requests to process
    for req in requests_to_process:
        log.info('Pushing file %s/%s into HPSS' % (req.file.acq.name, req.file.name))

        # Mark any FileCopyRequest for this file as completed
        di.ArchiveFileCopyRequest.update(completed=True).where(
            di.ArchiveFileCopyRequest.file == req.file).where(
            di.ArchiveFileCopyRequest.group_to == node.group).execute()

    script_name = _create_hpss_push_script(requests_to_process, node)
    log.info('Submitting HPSS job %s' % script_name)
    _submit_hpss_script(script_name)


def update_node_hpss_outbound(node):
    """Process transfers out of an HPSS tape node.
    """

    log.info('Processing HPSS outbound transfers (%s)' % node.name)

    # Fetch requests for transfer onto this node
    requests = di.ArchiveFileCopyRequest.select().where(
        ~di.ArchiveFileCopyRequest.completed,
        ~di.ArchiveFileCopyRequest.cancelled,
        di.ArchiveFileCopyRequest.group_to == node.group
    )

    # Add constraint that transfers must be from an HPSS node
    requests = requests.join(di.StorageNode).where(di.StorageNode.address == 'HPSS')

    # Get the requests we should actually process
    requests_to_process = _check_and_bundle_requests(requests, node)

    # Exit if there are no requests to process
    if len(requests_to_process) == 0:
        return

    # Construct final list of requests to process
    for req in requests_to_process:

        log.info('Pulling file %s/%s from HPSS' % (req.file.acq.name, req.file.name))

        # Mark any FileCopyRequest for this file as completed
        di.ArchiveFileCopyRequest.update(completed=True).where(
            di.ArchiveFileCopyRequest.file == req.file).where(
            di.ArchiveFileCopyRequest.group_to == node.group).execute()

    script_name = _create_hpss_pull_script(requests_to_process, node)
    log.info('Submitting HPSS job %s' % script_name)
    _submit_hpss_script(script_name)


def _create_hpss_push_script(requests, node):

    start = """#!/bin/bash
#PBS -l walltime=4:00:00
#PBS -q archive
#PBS -N push_%(jobname)s
#PBS -j oe
#PBS -m e

# Transfer files from CHIME archive to HPSS

DESTDIR=%(offline_node_root)s

## Looping section
"""

    loop = """


######## Processing file %(acq)s/%(file)s ########

echo 'Starting push of %(acq)s/%(file)s'

# Ensure the acquisition directory exists
hsi -q mkdir $DESTDIR/%(acq)s  # This always succeeds

# Copy the file into a temorary location
hsi -q put -c on -H md5 %(node_root)s/%(acq)s/%(file)s : $DESTDIR/%(acq)s/tmp.%(file)s

# Extract the MD5 hash of the file
HPSSHASH=$(hsi -q lshash $DESTDIR/%(acq)s/tmp.%(file)s 2>&1 | awk '{print $1}')


if [ $HPSSHASH == '%(file_hash)s' ]
then
    # Move the file into its final location
    hsi -q mv $DESTDIR/%(acq)s/tmp.%(file)s $DESTDIR/%(acq)s/%(file)s

    # Signal success
    ssh gpc04 'python %(script_path)s push-success %(file_id)i %(node_id)i'
    # ssh gpc04 'echo $(date) \"python %(script_path)s push-success %(file_id)i %(node_id)i\" >> /home/k/krs/jrs65/hpsstest'

    echo 'Finished push.'
else
    # Remove the corrupt file
    hsi -q rm $DESTDIR/%(acq)s/tmp.%(file)s

    # Signal failure
    ssh gpc04 'python %(script_path)s push-failed %(file_id)i %(node_id)i'
    # ssh gpc04 'echo $(date) \"python %(script_path)s push-failed %(file_id)i %(node_id)i\" >> /home/k/krs/jrs65/hpsstest'

    echo "Push failed."
fi
"""

    dtnow = datetime.datetime.now()
    dtstring = dtnow.strftime('%Y%m%dT%H%M%S')

    script = start % {'offline_node_root': node.root, 'jobname': dtstring}

    # Loop over files to construct push script
    for req in requests:

        req_dict = {
            'file': req.file.name,
            'acq': req.file.acq.name,
            'node_root': req.node_from.root,
            'file_hash': req.file.md5sum,
            'script_path': os.path.join(os.path.abspath(os.path.dirname(__file__)), 'alpenhorn_hpss'),
            'file_id': req.file.id,
            'node_id': node.id
        }

        script += loop % req_dict

    SCRATCH = os.getenv('SCRATCH')
    HPSS_SCRIPTS_DIR = os.path.join(SCRATCH, 'chime', 'hpss_scripts')

    script_name = HPSS_SCRIPTS_DIR + '/push_%s.sh' % dtstring

    with open(script_name, 'w') as f:
        f.write(script)

    return script_name


def _create_hpss_pull_script(requests, node):

    start = """#!/bin/bash
#PBS -l walltime=4:00:00
#PBS -q archive
#PBS -N pull_%(jobname)s
#PBS -j oe
#PBS -m e

# Transfer files from HPSS into online archive

DESTDIR=%(online_node_root)s

## Looping section
"""

    loop = """


######## Processing file %(acq)s/%(file)s ########

echo 'Starting pull of %(acq)s/%(file)s'

# Ensure the acquisition directory exists
mkdir -p $DESTDIR/%(acq)s  # This always succeeds

# Copy the file into a temorary location from HPSS offline
hsi -q get $DESTDIR/%(acq)s/tmp.%(file)s : %(node_root)s/%(acq)s/%(file)s

# Calculate the MD5 hash of the file
HPSSHASH=$(md5sum $DESTDIR/%(acq)s/tmp.%(file)s | awk '{print $1}')

if [ $HPSSHASH == '%(file_hash)s' ]
then
    # Move the file into its final location
    mv $DESTDIR/%(acq)s/tmp.%(file)s $DESTDIR/%(acq)s/%(file)s

    # Signal success
    ssh gpc04 'python %(script_path)s pull-success %(file_id)i %(node_id)i'
    # ssh gpc04 'echo $(date) \"python %(script_path)s pull-success %(file_id)i %(node_id)i\" >> /home/k/krs/jrs65/hpsstest'

    echo 'Finished pull.'
else
    # Remove the corrupt file
    rm $DESTDIR/%(acq)s/tmp.%(file)s

    # Signal failure
    ssh gpc04 'python %(script_path)s pull-failed %(file_id)i %(node_id)i'
    # ssh gpc04 'echo $(date) \"python %(script_path)s pull-failed %(file_id)i %(node_id)i\" >> /home/k/krs/jrs65/hpsstest'

    echo "Pull failed."
fi
"""

    dtnow = datetime.datetime.now()
    dtstring = dtnow.strftime('%Y%m%dT%H%M%S')

    script = start % {'online_node_root': node.root, 'jobname': dtstring}

    # Loop over files to construct push script
    for req in requests:

        req_dict = {
            'file': req.file.name,
            'acq': req.file.acq.name,
            'node_root': req.node_from.root,
            'file_hash': req.file.md5sum,
            'script_path': os.path.join(os.path.abspath(os.path.dirname(__file__)), 'alpenhorn_hpss'),
            'file_id': req.file.id,
            'node_id': node.id
        }

        script += loop % req_dict

    SCRATCH = os.getenv('SCRATCH')
    HPSS_SCRIPTS_DIR = os.path.join(SCRATCH, 'chime', 'hpss_scripts')

    script_name = HPSS_SCRIPTS_DIR + '/pull_%s.sh' % dtstring

    with open(script_name, 'w') as f:
        f.write(script)

    return script_name


def _submit_hpss_script(script):
    os.system('ssh gpc04 "cd %s; qsub %s"' % os.path.split(script))
