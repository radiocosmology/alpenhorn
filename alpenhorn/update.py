"""Routines for updating the state of a node.
"""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os
import time
import datetime
import re
import logging

import peewee as pw
from peewee import fn

from . import acquisition as ac
from . import archive as ar
from . import storage as st
from . import util, config, db


log = logging.getLogger(__name__)

# Parameters.
max_time_per_node_operation = 300   # Don't let node operations hog time.

RSYNC_FLAG = "qtspgoDL"

# Globals.
done_transport_this_cycle = False


def update_loop(host):
    """Loop over nodes performing any updates needed.
    """
    global done_transport_this_cycle

    while True:
        loop_start = time.time()
        done_transport_this_cycle = False

        # Iterate over nodes and perform each update (perform a new query
        # each time in case we get a new node, e.g. transport disk)
        for node in st.StorageNode.select().where(st.StorageNode.host == host):
            update_node(node)

        # Check the time spent so far, and wait if needed
        loop_time = time.time() - loop_start
        log.info("Main loop execution was %d sec.", loop_time)
        remaining = config.config['service']['update_interval'] - loop_time
        if remaining > 1:
            time.sleep(remaining)


def update_node(node):
    """Update the status of the node, and process eligible transfers onto it.
    """

    # TODO: bring back HPSS support
    # Check if this is an HPSS node, and if so call the special handler
    # if is_hpss_node(node):
    #     update_node_hpss_inbound(node)
    #     return

    # Make sure this node is usable.
    if not node.mounted:
        log.debug("Skipping unmounted node \"%s\".", node.name)
        return
    if node.suspect:
        log.debug("Skipping suspected node \"%s\".", node.name)

    log.info("Updating node \"%s\".", node.name)

    # Check if the node is acutally mounted in the filesystem
    check_node = update_node_mounted(node)

    if not check_node:
        return

    # Check and update the amount of free space then reload the instance for use
    # in later routines
    update_node_free_space(node)

    # Check the integrity of any questionable files (has_file=M)
    update_node_integrity(node)

    # Delete any upwanted files to cleanup space
    update_node_delete(node)

    # Process any regular transfers requests onto this node
    update_node_requests(node)

    # TODO: bring back HPSS support
    # Process any tranfers out of HPSS onto this node
    # update_node_hpss_outbound(node)


def update_node_mounted(node):
    """Check if a node is actually mounted in the filesystem"""

    fname = 'ALPENHORN_NODE'
    fullpath = os.path.join(node.root, fname)

    # Check if the node shows up as mounted in database
    if node.mounted is True:
        # Check if a file fname exists in the root of the node
        if os.path.exists(fullpath):
            log.debug("Checking if file \"%s\" exists on node \"%s\".", fname, node.name)

            # Open fname
            with open(fullpath, 'r') as f:
                first_line = f.readline()
                # Check if the actual node name is in the textfile
                if node.name == first_line.rstrip():
                    # Great! Everything is as expected. Exit this routine.
                    log.debug("Node %s matches with node name %s in %s file",
                              node.name, first_line, fname)
                    return True
                else:
                    log.error("Node %s does not match string %s in %s file",
                              node.name, first_line, fname)

        # If file does not exist in the root directory of the node, then mark
        # node as unmounted
        else:
            log.error("Node \"%s\" is not mounted as expected from db (missing %s file).",
                      node.name, fname)

    # If we are here the node it not correctly mounted so we should unmount it...
    node.mounted = False
    node.save(only=node.dirty_fields)  # save only fields that have been updated
    log.info("Correcting. Node %s is now set to unmounted", node.name)

    return False

def update_node_free_space(node):
    """Calculate the free space on the node and update the database with it."""

    # Check with the OS how much free space there is
    x = os.statvfs(node.root)
    avail_gb = float(x.f_bavail) * x.f_bsize / 2**30.0

    # Update the DB with the free space. Save only the dirty fields to ensure we
    # don't clobber changes made manually to the database
    node.avail_gb = avail_gb
    node.avail_gb_last_checked = datetime.datetime.now()
    node.save(only=node.dirty_fields)

    log.info("Node \"%s\" has %.2f GB available." % (node.name, avail_gb))


def update_node_integrity(node):
    """Check the integrity of file copies on the node."""

    # Find suspect file copies in the database
    fcopy_query = ar.ArchiveFileCopy.select().where(
        ar.ArchiveFileCopy.node == node,
        ar.ArchiveFileCopy.has_file == 'M').limit(25)

    # Loop over these file copies and check their md5sum
    for fcopy in fcopy_query:
        fullpath = "%s/%s/%s" % (node.root, fcopy.file.acq.name, fcopy.file.name)
        log.info("Checking file \"%s\" on node \"%s\"." % (fullpath, node.name))

        # If the file exists calculate its md5sum and check against the DB
        if os.path.exists(fullpath):
            if util.md5sum_file(fullpath) == fcopy.file.md5sum:
                log.info("File is A-OK!")
                fcopy.has_file = 'Y'
            else:
                log.error("File is corrupted!")
                fcopy.has_file = 'X'
        else:
            log.error("File does not exist!")
            fcopy.has_file = 'N'

        # Update the copy status
        log.info("Updating file copy status [id=%i]." % fcopy.id)
        fcopy.save()


def update_node_delete(node):
    """Process this node for files to delete."""

    # If we have less than the minimum available space, we should consider all files
    # not explicitly wanted (i.e. wants_file != 'Y') as candidates for deletion, provided
    # the copy is not on an archive node. If we have more than the minimum space, or
    # we are on archive node then only those explicitly marked (wants_file == 'N')
    # will be removed.
    #
    # A file will never be removed if there exist less than two copies available elsewhere.
    if node.avail_gb < node.min_avail_gb and node.storage_type != 'A':
        log.info("Hit minimum available space on %s -- considering all unwanted "
                 "files for deletion!" % (node.name))
        dfclause = ar.ArchiveFileCopy.wants_file != 'Y'
    else:
        dfclause = ar.ArchiveFileCopy.wants_file == 'N'

    # Search db for candidates on this node to delete.
    del_files = ar.ArchiveFileCopy.select().where(
        dfclause,
        ar.ArchiveFileCopy.node == node,
        ar.ArchiveFileCopy.has_file == 'Y')

    # Process candidates for deletion
    del_count = 0  # Counter for no. of deletions (limits no. per node update)
    for fcopy in del_files.order_by(ar.ArchiveFileCopy.id):

        # Limit number of deletions to 500 per main loop iteration.
        if del_count >= 500:
            break

        # Get all the *other* copies.
        other_copies = fcopy.file.copies.where(ar.ArchiveFileCopy.id != fcopy.id,
                                               ar.ArchiveFileCopy.has_file == 'Y')

        # Get the number of copies on archive nodes
        ncopies = other_copies.join(st.StorageNode) \
                              .where(st.StorageNode.storage_type == 'A').count()

        shortname = "%s/%s" % (fcopy.file.acq.name, fcopy.file.name)
        fullpath = "%s/%s/%s" % (node.root, fcopy.file.acq.name, fcopy.file.name)

        # If at least two other copies we can delete the file.
        if ncopies >= 2:

            # Use transaction such that errors thrown in the os.remove do not leave
            # the database inconsistent.
            with db.database_proxy.transaction():
                if os.path.exists(fullpath):
                    os.remove(fullpath)  # Remove the actual file

                    # Check if the acquisition directory or containing directories are now empty,
                    # and remove if they are.
                    dirname = os.path.dirname(fullpath)
                    while dirname != node.root:
                        if not os.listdir(dirname):
                            log.info("Removing acquisition directory %s on %s" %
                                     (fcopy.file.acq.name, fcopy.node.name))
                            os.rmdir(dirname)
                            dirname = os.path.dirname(dirname)
                        else:
                            break

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

    # TODO: Fix up HPSS support
    # Ensure we are not on an HPSS node
    # if is_hpss_node(node):
    #     log.error("Cannot process HPSS node here.")
    #     return

    avail_gb = node.avail_gb

    # Skip if node is too full
    if avail_gb < (node.min_avail_gb + 10):
        log.info("Node %s is nearly full. Skip transfers." % node.name)
        return

    # Calculate the total archive size from the database
    size_query = (ac.ArchiveFile.select(fn.Sum(ac.ArchiveFile.size_b))
                  .join(ar.ArchiveFileCopy).where(ar.ArchiveFileCopy.node == node,
                                                  ar.ArchiveFileCopy.has_file == 'Y'))

    size = size_query.scalar(as_tuple=True)[0]
    current_size_gb = float(0.0 if size is None else size) / 2**30.0

    # Stop if the current archive size is bigger than the maximum (if set, i.e. > 0)
    if (current_size_gb > node.max_total_gb and node.max_total_gb > 0.0):
        log.info('Node %s has reached maximum size (current: %.1f GB, limit: %.1f GB)' %
                 (node.name, current_size_gb, node.max_total_gb))
        return

    # ... OR if this is a transport node quit if the transport cycle is done.
    if (node.storage_type == "T" and done_transport_this_cycle):
        log.info('Ignoring transport node %s' % node.name)
        return

    start_time = time.time()

    # Fetch requests to process from the database
    requests = ar.ArchiveFileCopyRequest.select().where(
        ~ar.ArchiveFileCopyRequest.completed,
        ~ar.ArchiveFileCopyRequest.cancelled,
        ar.ArchiveFileCopyRequest.group_to == node.group
    )

    # Add in constraint that node_from cannot be an HPSS node
    requests = requests.join(st.StorageNode).where(st.StorageNode.address != 'HPSS')

    for req in requests:

        # Only continue if the node is actually mounted
        if not req.node_from.mounted:
            continue

        # For transport disks we should only copy onto the transport
        # node if the from_node is local, this should prevent pointlessly
        # rsyncing across the network
        if node.storage_type == "T" and node.host != req.node_from.host:
            log.debug("Skipping request for %s/%s from remote node [%s] onto local "
                      "transport disks" % (req.file.acq.name, req.file.name,
                                           req.node_from.name))
            continue

        # Only proceed if the source file actually exists (and is not corrupted).
        try:
            ar.ArchiveFileCopy.get(ar.ArchiveFileCopy.file == req.file,
                                   ar.ArchiveFileCopy.node == req.node_from,
                                   ar.ArchiveFileCopy.has_file == 'Y')
        except pw.DoesNotExist:
            log.error("Skipping request for %s/%s since it is not available on "
                      "node \"%s\". [file_id=%i]" % (req.file.acq.name,
                                                     req.file.name,
                                                     req.node_from.name,
                                                     req.file.id))
            continue

        # Only proceed if the destination file does not already exist.
        try:
            ar.ArchiveFileCopy.get(ar.ArchiveFileCopy.file == req.file,
                                   ar.ArchiveFileCopy.node == node,
                                   ar.ArchiveFileCopy.has_file == 'Y')
            log.info("Skipping request for %s/%s since it already exists on "
                     "this node (\"%s\"), and updating DB to reflect this." %
                     (req.file.acq.name, req.file.name, node.name))
            ar.ArchiveFileCopyRequest.update(completed=True).where(
                ar.ArchiveFileCopyRequest.file == req.file).where(
                ar.ArchiveFileCopyRequest.group_to ==
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

            if req.node_from.username is None or req.node_from.address is None:
                log.error('Source node (%s) not properly configured (username=%s, address=%s)',
                          req.node_from.name, req.node_from.username, req.node_from.address)
                continue

            from_path = "%s@%s:%s" % (req.node_from.username,
                                      req.node_from.address, from_path)

        to_file = os.path.join(node.root, req.file.acq.name, req.file.name)
        to_dir = os.path.dirname(to_file)
        if not os.path.isdir(to_dir):
            log.info("Creating directory \"%s\"." % to_dir)
            os.makedirs(to_dir)

        # Giddy up!
        log.info("Transferring file \"%s/%s\"." % (req.file.acq.name, req.file.name))
        start_time = time.time()

        # Attempt to transfer the file. Each of the methods below needs to set a
        # return code `ret` and give an `md5sum` of the transferred file.

        # First we need to check if we are copying over the network
        if req.node_from.host != node.host:

            # First try bbcp which is a fast multistream transfer tool. bbcp can
            # calculate the md5 hash as it goes, so we'll do that to save doing
            # it at the end.
            if util.command_available('bbcp'):
                cmd = 'bbcp -f -z --port 4200 -W 4M -s 16 -o -E md5= %s %s' % (from_path, to_dir)
                ret, stdout, stderr = util.run_command(cmd)

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
            elif util.command_available('rsync'):
                cmd = ("rsync -z%s --rsync-path=\"ionice -c2 -n4 rsync\" -e \"ssh -q\" %s %s" %
                       (RSYNC_FLAG, from_path, to_dir))
                ret, stdout, stderr = util.run_command(cmd)

                md5sum = util.md5sum_file(to_file) if ret == 0 else None

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
                link_path = os.path.join(node.root, req.file.acq.name, req.file.name)

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
                if util.command_available('rsync'):
                    cmd = "rsync -%s %s %s" % (RSYNC_FLAG, from_path, to_dir)
                    ret, stdout, stderr = util.run_command(cmd)

                    md5sum = util.md5sum_file(to_file) if ret == 0 else None
                else:
                    log.warn("No commands available to complete this transfer.")
                    ret = -1

        # Check the return code...
        if ret:
            # If the copy didn't work, then the remote file may be corrupted.
            log.error("Rsync failed. Marking source file suspect.")
            ar.ArchiveFileCopy.update(has_file='M').where(
                ar.ArchiveFileCopy.file == req.file,
                ar.ArchiveFileCopy.node == req.node_from).execute()
            continue
        end_time = time.time()

        # Check integrity.
        if md5sum == req.file.md5sum:
            size_mb = req.file.size_b / 2**20.0
            trans_time = end_time - start_time
            rate = size_mb / trans_time
            log.info("Pull complete (md5sum correct). Transferred %.1f MB in %i "
                     "seconds [%.1f MB/s]" % (size_mb, int(trans_time), rate))

            # Update the FileCopy (if exists), or insert a new FileCopy
            try:
                done = False
                while not done:
                    try:
                        fcopy = ar.ArchiveFileCopy\
                                  .select()\
                                  .where(ar.ArchiveFileCopy.file == req.file,
                                         ar.ArchiveFileCopy.node == node)\
                                  .get()
                        fcopy.has_file = 'Y'
                        fcopy.wants_file = 'Y'
                        fcopy.save()
                        done = True
                    except pw.OperationalError:
                        log.error("MySQL connexion dropped. Will attempt to reconnect in "
                                  "five seconds.")
                        time.sleep(5)
                        db.config_connect()
            except pw.DoesNotExist:
                ar.ArchiveFileCopy.insert(file=req.file, node=node, has_file='Y',
                                          wants_file='Y').execute()

            # Mark any FileCopyRequest for this file as completed
            ar.ArchiveFileCopyRequest.update(completed=True).where(
                ar.ArchiveFileCopyRequest.file == req.file).where(
                ar.ArchiveFileCopyRequest.group_to == node.group).execute()

            if node.storage_type == "T":
                # This node is getting the transport king.
                done_transport_this_cycle = True

            # Update local estimate of available space
            avail_gb = avail_gb - req.file.size_b / 2**30.0

        else:
            log.error("Error with md5sum check: %s on node \"%s\", but %s on "
                      "this node, \"%s\"." % (req.file.md5sum, req.node_from.name,
                                              md5sum, node.name))
            log.error("Removing file \"%s\"." % to_file)
            try:
                os.remove(to_file)
            except:
                log.error("Could not remove file.")

            # Since the md5sum failed, the remote file may be corrupted.
            log.error("Marking source file suspect.")
            ar.ArchiveFileCopy.update(has_file='M').where(
                ar.ArchiveFileCopy.file == req.file,
                ar.ArchiveFileCopy.node == req.node_from).execute()

        if time.time() - start_time > max_time_per_node_operation:
            break  # Don't hog all the time.
