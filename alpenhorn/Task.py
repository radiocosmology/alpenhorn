""" Classes to run each task in alpenhorn from a queue.
"""

import datetime as dt
import logging
import os
import re
import time
import queue
import threading

import peewee as pw
from peewee import fn

from . import acquisition as ac
from . import archive as ar
from . import config, db
from . import update

# TODO this is probably going to result in a circular import; i also want to adjust how this works
# can Task.py and update.py go into the same place?
from . import storage as st
from . import util

log = logging.getLogger(__name__)

# Parameters and Globals.
RSYNC_OPTS = [
    "--quiet",
    "--times",
    "--protect-args",
    "--perms",
    "--group",
    "--owner",
    "--copy-links",
    "--sparse",
]

max_time_per_node_operation = 300  # Don't let node operations hog time.
max_time_per_task_queue = 30  # seconds

done_transport_this_cycle = False


class TaskQueue:
    def add_task(self, task):
        """Add task to queue."""
        if self.queue.full():
            # TODO should we add a TIMEOUT to queue.put, and raise an exception if TIMEOUT exceeded?
            # or should we add to log that this thread is blocked
            log.warning(
                "Task queue is full ({:d} tasks). Blocking.".format(self.queue.qsize())
            )
        self.queue.put(task)

    def run_tasks(self):
        """Loop and run tasks from queue."""
        while True:
            try:
                # TODO queue.get is blocked if queue is empty
                # so we should add a Timeout, otherwise queue.Empty will not raise
                # for queue, block = True, timeout = None by default
                task = self.queue.get(timeout=max_time_per_task_queue)
            except queue.Empty:
                pass
            else:
                task.run()
                self.queue.task_done()

    def __init__(self, max_size, num_threads):
        """Create queue and setup thread pool."""
        self.queue = queue.Queue(maxsize=max_size)

        threads = []
        for t in range(num_threads):
            threads.append(threading.Thread(target=self.run_tasks, daemon=True))
            threads[t].start()


class Task:
    def __init__(self, node):
        self.node = node

    def run(self):
        raise NotImplementedError


class IntegrityTask(Task):
    def __init__(self, node):
        super().__init__(node)

    def run(self):
        """Check the integrity of file copies on the node."""

        # TODO should these be log.info?
        print("{} run() with node: {}".format(type(self).__name__, self.node.name))

        # Find suspect file copies in the database
        fcopy_query = (
            ar.ArchiveFileCopy.select()
            .where(
                ar.ArchiveFileCopy.node == self.node, ar.ArchiveFileCopy.has_file == "M"
            )
            .limit(25)
        )

        # Loop over these file copies and check their md5sum
        for fcopy in fcopy_query:
            fullpath = "%s/%s/%s" % (
                self.node.root,
                fcopy.file.acq.name,
                fcopy.file.name,
            )
            log.info('Checking file "%s" on node "%s".' % (fullpath, self.node.name))

            # If the file exists calculate its md5sum and check against the DB
            if os.path.exists(fullpath):
                if util.md5sum_file(fullpath) == fcopy.file.md5sum:
                    log.info("File %s on node %s is A-OK!" % (fullpath, self.node.name))
                    fcopy.has_file = "Y"
                    copy_size_b = os.stat(fullpath).st_blocks * 512
                    fcopy.size_b = copy_size_b
                else:
                    log.error(
                        "File %s on node %s is corrupted!" % (fullpath, self.node.name)
                    )
                    fcopy.has_file = "X"
            else:
                log.error(
                    "File %s on node %s does not exist!" % (fullpath, self.node.name)
                )
                fcopy.has_file = "N"

            # Update the copy status
            log.info(
                "Updating file copy status for file %s on node %s[id=%i]."
                % (fullpath, self.node.name, fcopy.id)
            )
            fcopy.save()


class DeletionTask(Task):
    def __init__(self, node):
        super().__init__(node)

    def run(self):
        """Process this node for files to delete."""

        # TODO log.info?
        print("{} run()...".format(type(self).__name__))

        # If we have less than the minimum available space, we should consider all files
        # not explicitly wanted (i.e. wants_file != 'Y') as candidates for deletion, provided
        # the copy is not on an archive node. If we have more than the minimum space, or
        # we are on archive node then only those explicitly marked (wants_file == 'N')
        # will be removed.
        #
        # A file will never be removed if there exist less than two copies available elsewhere.
        if (
            self.node.avail_gb < self.node.min_avail_gb
            and self.node.storage_type != "A"
        ):
            log.info(
                "Hit minimum available space on %s -- considering all unwanted "
                "files for deletion!" % (self.node.name)
            )
            dfclause = ar.ArchiveFileCopy.wants_file != "Y"
        else:
            dfclause = ar.ArchiveFileCopy.wants_file == "N"

        # Search db for candidates on this node to delete.
        del_files = ar.ArchiveFileCopy.select().where(
            dfclause,
            ar.ArchiveFileCopy.node == self.node,
            ar.ArchiveFileCopy.has_file == "Y",
        )

        # Process candidates for deletion
        del_count = 0  # Counter for no. of deletions (limits no. per node update)
        for fcopy in del_files.order_by(ar.ArchiveFileCopy.id):

            # Limit number of deletions to 500 per main loop iteration.
            if del_count >= 500:
                break

            # Get all the *other* copies.
            other_copies = fcopy.file.copies.where(
                ar.ArchiveFileCopy.id != fcopy.id, ar.ArchiveFileCopy.has_file == "Y"
            )

            # Get the number of copies on archive nodes
            ncopies = (
                other_copies.join(st.StorageNode)
                .where(st.StorageNode.storage_type == "A")
                .count()
            )

            shortname = "%s/%s" % (fcopy.file.acq.name, fcopy.file.name)
            fullpath = "%s/%s/%s" % (
                self.node.root,
                fcopy.file.acq.name,
                fcopy.file.name,
            )

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
                        while dirname != self.node.root:
                            if not os.listdir(dirname):
                                log.info(
                                    "Removing acquisition directory %s on %s"
                                    % (fcopy.file.acq.name, fcopy.node.name)
                                )
                                os.rmdir(dirname)
                                dirname = os.path.dirname(dirname)
                            else:
                                break

                    fcopy.has_file = "N"
                    fcopy.wants_file = "N"  # Set in case it was 'M' before
                    fcopy.save()  # Update the FileCopy in the database

                    log.info(
                        "Removed file copy: %s on %s" % (shortname, self.node.name)
                    )

                del_count += 1

            else:
                log.info(
                    "Too few backups to delete %s on %s" % (shortname, self.node.name)
                )


class TransferTask(Task):
    def __init__(self, node):
        # how do the double inits work?
        super().__init__(node)
        self.fs_type = None

    def run(self):
        if self.fs_type is None:
            log.error(
                "Use appropriate transfer task for file system (i.e. DiskTransferTask or NearlineTransferTask)."
            )

        # TODO log.info?
        print("{} run()...".format(type(self).__name__))

        # TODO we may want to do something better
        global done_transport_this_cycle

        avail_gb = self.node.avail_gb

        # Skip if node is too full
        # TODO figure out if this test is helpful, and if not just delete it
        # TODO if it is useful, what is the right threshold (is it 10 GB?) if it is
        # TODO document the justification for the particular threshold chosen
        if avail_gb < (self.node.min_avail_gb + 10):
            log.info("Node %s is nearly full. Skip transfers." % self.node.name)
            return

        # Calculate the total archive size from the database
        size_query = (
            ac.ArchiveFile.select(fn.Sum(ac.ArchiveFile.size_b))
            .join(ar.ArchiveFileCopy)
            .where(
                ar.ArchiveFileCopy.node == self.node, ar.ArchiveFileCopy.has_file == "Y"
            )
        )

        size = size_query.scalar(as_tuple=True)[0]
        current_size_gb = float(0.0 if size is None else size) / 2 ** 30.0

        # Stop if the current archive size is bigger than the maximum (if set, i.e. > 0)
        if current_size_gb > self.node.max_total_gb and self.node.max_total_gb > 0.0:
            log.info(
                "Node %s has reached maximum size (current: %.1f GB, limit: %.1f GB)"
                % (self.node.name, current_size_gb, self.node.max_total_gb)
            )
            return

        # ... OR if this is a transport node quit if the transport cycle is done.
        if self.node.storage_type == "T" and done_transport_this_cycle:
            log.info("Ignoring transport node %s" % self.node.name)
            return

        start_time = time.time()

        # Fetch requests to process from the database
        requests = ar.ArchiveFileCopyRequest.select().where(
            ~ar.ArchiveFileCopyRequest.completed,
            ~ar.ArchiveFileCopyRequest.cancelled,
            ar.ArchiveFileCopyRequest.group_to == self.node.group,
        )

        # Add in constraint to only process fs_type nodes
        requests = requests.join(st.StorageNode).where(
            st.StorageNode.fs_type == self.fs_type
        )

        for req in requests:

            if time.time() - start_time > max_time_per_node_operation:
                break  # Don't hog all the time.

            # By default, if a copy fails, we mark the source file as suspect
            # so it gets re-MDF5'd on the source node.
            check_source_on_error = True

            # Only continue if the node is actually active
            if not req.node_from.active:
                continue

            # For transport disks we should only copy onto the transport
            # node if the from_node is local, this should prevent pointlessly
            # rsyncing across the network
            if self.node.storage_type == "T" and self.node.host != req.node_from.host:
                log.debug(
                    "Skipping request for %s/%s from remote node [%s] onto local "
                    "transport disks"
                    % (req.file.acq.name, req.file.name, req.node_from.name)
                )
                continue

            # Only proceed if the destination file does not already exist.
            try:
                ar.ArchiveFileCopy.get(
                    ar.ArchiveFileCopy.file == req.file,
                    ar.ArchiveFileCopy.node == self.node,
                    ar.ArchiveFileCopy.has_file == "Y",
                )
                log.info(
                    "Skipping request for %s/%s since it already exists on "
                    'this node ("%s"), and updating DB to reflect this.'
                    % (req.file.acq.name, req.file.name, self.node.name)
                )
                ar.ArchiveFileCopyRequest.update(completed=True).where(
                    ar.ArchiveFileCopyRequest.file == req.file
                ).where(ar.ArchiveFileCopyRequest.group_to == self.node.group).execute()
                continue
            except pw.DoesNotExist:
                pass

            # Only proceed if the source file actually exists (and is not corrupted).
            # On nearline, additionally only proceed if source file is prepared
            try:
                if self.fs_type == "Disk":
                    ar.ArchiveFileCopy.get(
                        ar.ArchiveFileCopy.file == req.file,
                        ar.ArchiveFileCopy.node == req.node_from,
                        ar.ArchiveFileCopy.has_file == "Y",
                    )
                elif self.fs_type == "Nearline":
                    ar.ArchiveFileCopy.get(
                        ar.ArchiveFileCopy.file == req.file,
                        ar.ArchiveFileCopy.node == req.node_from,
                        ar.ArchiveFileCopy.has_file == "Y",
                        ar.ArchiveFileCopy.prepared == True,
                    )
            except pw.DoesNotExist:
                log.error(
                    "Skipping request for %s/%s since it is not available on or is not prepared on"
                    'node "%s". [file_id=%i]'
                    % (
                        req.file.acq.name,
                        req.file.name,
                        req.node_from.name,
                        req.file.id,
                    )
                )
                continue

            # Check that there is enough space available.
            if self.node.avail_gb * 2 ** 30.0 < 2.0 * req.file.size_b:
                log.warning(
                    'Node "%s" is full: not adding datafile "%s/%s".'
                    % (self.node.name, req.file.acq.name, req.file.name)
                )
                continue

            # Constuct the origin and destination paths.
            from_path = "%s/%s/%s" % (
                req.node_from.root,
                req.file.acq.name,
                req.file.name,
            )
            if req.node_from.host != self.node.host:

                if req.node_from.username is None or req.node_from.address is None:
                    log.error(
                        "Source node (%s) not properly configured (username=%s, address=%s)",
                        req.node_from.name,
                        req.node_from.username,
                        req.node_from.address,
                    )
                    continue

                from_path = "%s@%s:%s" % (
                    req.node_from.username,
                    req.node_from.address,
                    from_path,
                )

            to_file = os.path.join(self.node.root, req.file.acq.name, req.file.name)
            to_dir = os.path.dirname(to_file)
            if not os.path.isdir(to_dir):
                log.info('Creating directory "%s".' % to_dir)
                os.makedirs(to_dir)

            # For the potential error message later
            stderr = None

            # Giddy up!
            log.info('Transferring file "%s/%s".' % (req.file.acq.name, req.file.name))
            start_time = time.time()
            req.transfer_started = dt.datetime.fromtimestamp(start_time)
            req.save(only=req.dirty_fields)

            # Attempt to transfer the file. Each of the methods below needs to set a
            # return code `ret` and give an `md5sum` of the transferred file.

            # First we need to check if we are copying over the network
            if req.node_from.host != self.node.host:

                # First try bbcp which is a fast multistream transfer tool. bbcp can
                # calculate the md5 hash as it goes, so we'll do that to save doing
                # it at the end.
                if util.command_available("bbcp"):
                    ret, stdout, stderr = util.run_command(
                        [  # See: https://www.slac.stanford.edu/~abh/bbcp/
                            "bbcp",
                            #
                            #
                            # -V (AKA --vverbose, with two v's) is here because bbcp
                            # bbcp is weirdly broken.
                            #
                            # I have discovered a truly marvelous proof of this
                            # which this comment is too narrow to contain.
                            "-V",
                            #
                            #
                            # force: delete an existing destination file before
                            # transfer
                            "-f",
                            #
                            #
                            # Use a reverse connection to get through a firewall
                            # (This may not be appropriate everywhere -- more reason
                            # we need an edge table in the database.)
                            "-z",
                            #
                            #
                            # Port to use
                            "--port",
                            "4200",
                            #
                            #
                            # TCP window size.  4M is what Linux typically limits
                            # you to (cf. /proc/sys/net/ipv4/tcp_wmem)
                            "-W",
                            "4M",
                            #
                            #
                            # Number of streams
                            "-s",
                            "16",
                            #
                            #
                            # Do block-level checksumming to detect transmission
                            # errors
                            "-e",
                            #
                            #
                            # Calculate _and print_ a MD5 checksum of the whole file
                            # on the source.  MD5ing is done on the source to avoid
                            # the need for the file transfer to occur in order
                            # (which can cause bbcp to lock up).
                            #
                            # See https://www.slac.stanford.edu/~abh/bbcp/#_Toc392015140
                            # and https://github.com/chime-experiment/alpenhorn/pull/15
                            "-E",
                            "%md5=",
                            from_path,
                            to_dir,
                        ]
                    )

                    # Attempt to parse STDERR for the md5 hash
                    if ret == 0:
                        mo = re.search("md5 ([a-f0-9]{32})", stderr)
                        if mo is None:
                            log.error(
                                "BBCP transfer has gone awry. STDOUT: %s\n STDERR: %s"
                                % (stdout, stderr)
                            )
                            ret = -1
                        md5sum = mo.group(1)
                    else:
                        md5sum = None

                # Next try rsync over ssh.
                elif util.command_available("rsync"):
                    ret, stdout, stderr = util.run_command(
                        ["rsync", "--compress"]
                        + RSYNC_OPTS
                        + [
                            "--rsync-path=ionice -c2 -n4 rsync",
                            "--rsh=ssh -q",
                            from_path,
                            to_dir,
                        ]
                    )

                    md5sum = util.md5sum_file(to_file) if ret == 0 else None

                    # If the rsync error occured during `mkstemp` this is a
                    # problem on the destination, not the source
                    if ret and "mkstemp" in stderr:
                        log.warn(
                            'rsync file creation failed on "{0}"'.format(self.node.name)
                        )
                        check_source_on_err = False
                    elif "write failed on" in stderr:
                        log.warn(
                            'rsync failed to write to "{0}": {1}'.format(
                                self.node.name, stderr[stderr.rfind(":") + 2 :].strip()
                            )
                        )
                        check_source_on_err = False

                # If we get here then we have no idea how to transfer the file...
                else:
                    log.warn("No commands available to complete this transfer.")
                    check_source_on_err = False
                    ret = -1

            # Okay, great we're just doing a local transfer.
            else:

                # First try to just hard link the file. This will only work if we
                # are on the same filesystem. As there's no actual copying it's
                # probably unecessary to calculate the md5 check sum, so we'll just
                # fake it.
                try:
                    link_path = os.path.join(
                        self.node.root, req.file.acq.name, req.file.name
                    )

                    # Check explicitly if link already exists as this and
                    # being unable to link will both raise OSError and get
                    # confused.
                    if os.path.exists(link_path):
                        log.error(
                            "File %s already exists. Clean up manually." % link_path
                        )
                        check_source_on_err = False
                        ret = -1
                    else:
                        os.link(from_path, link_path)
                        ret = 0
                        md5sum = (
                            req.file.md5sum
                        )  # As we're linking the md5sum can't change. Skip the check here...

                # If we couldn't just link the file, try copying it with rsync.
                except OSError:
                    if util.command_available("rsync"):
                        ret, stdout, stderr = util.run_command(
                            ["rsync"] + RSYNC_OPTS + [from_path, to_dir]
                        )

                        md5sum = util.md5sum_file(to_file) if ret == 0 else None

                        # If the rsync error occured during `mkstemp` this is a
                        # problem on the destination, not the source
                        if ret and "mkstemp" in stderr:
                            log.warn(
                                'rsync file creation failed on "{0}"'.format(
                                    self.node.name
                                )
                            )
                            check_source_on_err = False
                        elif "write failed on" in stderr:
                            log.warn(
                                'rsync failed to write to "{0}": {1}'.format(
                                    self.node.name,
                                    stderr[stderr.rfind(":") + 2 :].strip(),
                                )
                            )
                            check_source_on_err = False
                    else:
                        log.warn("No commands available to complete this transfer.")
                        check_source_on_err = False
                        ret = -1

            # Check the return code...
            if ret:
                if check_source_on_err:
                    # If the copy didn't work, then the remote file may be corrupted.
                    log.error(
                        "Copy failed: {0}. Marking source file suspect.".format(
                            stderr if stderr is not None else "Unspecified error."
                        )
                    )
                    ar.ArchiveFileCopy.update(has_file="M").where(
                        ar.ArchiveFileCopy.file == req.file,
                        ar.ArchiveFileCopy.node == req.node_from,
                    ).execute()
                else:
                    # An error occurred that can't be due to the source
                    # being corrupt
                    log.error("Copy failed.")
                continue
            end_time = time.time()

            # Check integrity.
            if md5sum == req.file.md5sum:
                size_mb = req.file.size_b / 2 ** 20.0
                copy_size_b = os.stat(to_file).st_blocks * 512
                trans_time = end_time - start_time
                rate = size_mb / trans_time
                log.info(
                    "Pull complete (md5sum correct). Transferred %.1f MB in %i "
                    "seconds [%.1f MB/s]" % (size_mb, int(trans_time), rate)
                )

                # Update the FileCopy (if exists), or insert a new FileCopy
                try:
                    done = False
                    while not done:
                        try:
                            fcopy = (
                                ar.ArchiveFileCopy.select()
                                .where(
                                    ar.ArchiveFileCopy.file == req.file,
                                    ar.ArchiveFileCopy.node == self.node,
                                )
                                .get()
                            )
                            fcopy.has_file = "Y"
                            fcopy.wants_file = "Y"
                            fcopy.size_b = copy_size_b
                            # Prepared attribute only applies to nearline
                            if self.fs_type == "Nearline":
                                fcopy.prepared = True
                            fcopy.save()
                            done = True
                        except pw.OperationalError:
                            log.error(
                                "MySQL connexion dropped. Will attempt to reconnect in "
                                "five seconds."
                            )
                            time.sleep(5)
                            db.config_connect()
                except pw.DoesNotExist:
                    ar.ArchiveFileCopy.insert(
                        file=req.file,
                        node=self.node,
                        has_file="Y",
                        wants_file="Y",
                        prepared=False,
                        size_b=copy_size_b,
                    ).execute()

                # Mark any FileCopyRequest for this file as completed
                ar.ArchiveFileCopyRequest.update(
                    completed=True,
                    transfer_completed=dt.datetime.fromtimestamp(end_time),
                ).where(ar.ArchiveFileCopyRequest.file == req.file).where(
                    ar.ArchiveFileCopyRequest.group_to == self.node.group,
                    ~ar.ArchiveFileCopyRequest.completed,
                    ~ar.ArchiveFileCopyRequest.cancelled,
                ).execute()

                if self.node.storage_type == "T":
                    # This node is getting the transport king.
                    done_transport_this_cycle = True

                # TODO why are these different?
                # TODO why does nearline not use update_node_free_space?
                if self.fs_type == "Disk":
                    # Update node available space
                    update.update_node_free_space(self.node)
                elif self.fs_type == "Nearline":
                    # Update local estimate of available space
                    avail_gb = avail_gb - req.file.size_b / 2 ** 30.0

            else:
                log.error(
                    'Error with md5sum check: %s on node "%s", but %s on '
                    'this node, "%s".'
                    % (req.file.md5sum, req.node_from.name, md5sum, self.node.name)
                )
                log.error('Removing file "%s" on node %s.' % (to_file, self.node.name))
                try:
                    os.remove(to_file)
                except Exception:
                    log.error(
                        "Could not remove file %s on node %s."
                        % (to_file, self.node.name)
                    )

                # Since the md5sum failed, the remote file may be corrupted.
                log.error(
                    "Marking source file %s on node %s suspect ."
                    % (req.file, req.node_from)
                )
                ar.ArchiveFileCopy.update(has_file="M").where(
                    ar.ArchiveFileCopy.file == req.file,
                    ar.ArchiveFileCopy.node == req.node_from,
                ).execute()


class DiskTransferTask(TransferTask):
    fs_type = "Disk"

    def __init__(self, node):
        super().__init__(node)


class NearlineTransferTask(TransferTask):
    fs_type = "Nearline"

    def __init__(self, node):
        super().__init__(node)


class NearlineReleaseTask(Task):
    def run(self):
        """Release files to tape to conserve quota on this node."""

        print("{} run()...".format(type(self).__name__))

        # Fetch completed requests to release files from
        requests = ar.ArchiveFileCopyRequest.select().where(
            ar.ArchiveFileCopyRequest.completed,
            ~ar.ArchiveFileCopyRequest.cancelled,
        )

        # Add in constraint to only process Nearline nodes
        requests = requests.join(st.StorageNode).where(
            st.StorageNode.fs_type == "Nearline"
        )

        for req in requests:

            if time.time() - start_time > max_time_per_node_operation:
                break  # Don't hog all the time.

            # Use lfs to check if file is on disk
            if util.command_available("lfs"):
                file_path = "%s/%s" % (req.file.acq.name, req.file.name)
                cmd = "lfs hsm_state %s" % file_path
                ret, stdout, stderr = util.run_command(cmd)

                # Parse STDERR
                if ret == 0:
                    on_disk_and_tape = re.search("exists archived", stderr)

                    # Only proceed if the file is on disk and tape.
                    if on_disk_and_tape:
                        # Release file (synchronous)
                        release_cmd = "lfs hsm_release %s" % file_path
                        ret, stdout, stderr = util.run_command(release_cmd)
                        if ret == 0:
                            log.info("File: %s has been released to tape." % file_path)
                            continue
                        else:
                            log.error(
                                "lfs hsm_release command has gone awry. STDOUT: %s\n STDERR: %s"
                                % (stdout, stderr)
                            )

                else:
                    log.error(
                        "lfs hsm_state command has returned an error. STDOUT: %s\n STDERR: %s"
                        % (stdout, stderr)
                    )

            else:
                log.error(
                    "lfs command unavailable, so unable to complete this transfer."
                )


class HPSSTransferTask(Task):
    def run(self):
        raise NotImplementedError


class SourceTransferTask(Task):
    def run(self):
        print("{} run()...".format(type(self).__name__))

        start_time = time.time()

        # Fetch requests to process from the database
        requests = ar.ArchiveFileCopyRequest.select().where(
            ~ar.ArchiveFileCopyRequest.completed,
            ~ar.ArchiveFileCopyRequest.cancelled,
            ar.ArchiveFileCopyRequest.node_from == self.node,
        )

        # Add in constraint to only process Nearline nodes
        requests = requests.join(st.StorageNode).where(
            st.StorageNode.fs_type == "Nearline"
        )

        for req in requests:

            if time.time() - start_time > max_time_per_node_operation:
                break  # Don't hog all the time.

            # Use lfs to check if file is on disk
            if util.command_available("lfs"):
                file_path = "%s/%s" % (req.file.acq.name, req.file.name)
                cmd = "lfs hsm_state %s" % file_path
                ret, stdout, stderr = util.run_command(cmd)

                # Parse STDERR
                if ret == 0:
                    on_disk = re.search("(0x00000000)|exists archived", stdout)
                    on_tape = re.search("released archived", stdout)

                    # Only proceed if the source file actually exists (and is not corrupted).
                    if on_disk:
                        try:
                            ar.ArchiveFileCopy.get(
                                ar.ArchiveFileCopy.file == req.file,
                                ar.ArchiveFileCopy.node == req.node_from,
                                ar.ArchiveFileCopy.has_file == "Y",
                            )
                        except pw.DoesNotExist:
                            log.error(
                                "Skipping request for %s/%s since it is not available on "
                                'node "%s". [file_id=%i]'
                                % (
                                    req.file.acq.name,
                                    req.file.name,
                                    req.node_from.name,
                                    req.file.id,
                                )
                            )
                            continue

                    # If the file is on tape, force an asynchronous recall of the file
                    elif on_tape:
                        restore_cmd = "lfs hsm_restore %s" % file_path
                        ret, stdout, stderr = util.run_command(restore_cmd)
                        if ret == 0:
                            log.info(
                                "Skipping request for %s since it is being recalled from tape."
                                % file_path
                            )
                            continue
                        else:
                            log.error(
                                "lfs hsm_restore %s command has gone awry. STDOUT: %s\n STDERR: %s"
                                % (file_path, stdout, stderr)
                            )

                    else:
                        log.error(
                            "lfs hsm_state %s command has gone awry. STDOUT: %s\n STDERR: %s"
                            % (file_path, stdout, stderr)
                        )

                else:
                    log.error(
                        "lfs hsm_state %s command has returned an error. STDOUT: %s\n STDERR: %s"
                        % (file_path, stdout, stderr)
                    )

            else:
                log.error(
                    "lfs command unavailable on node %s, so unable to complete this transfer."
                    % self.node.name
                )

            # Notify destination that transfer can proceed.
            ar.ArchiveFileCopy.update(prepared=True).where(
                ar.ArchiveFileCopy.file == req.file
            ).execute()