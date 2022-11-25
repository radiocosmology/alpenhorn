""" Classes to run each task in alpenhorn from a queue.
"""


class DeletionTask(Task):
    def __init__(self, node):
        super().__init__(node)

    def run(self):
        """Process this node for files to delete."""

        # TODO log.info?
        print("{} run()...".format(type(self).__name__))

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
