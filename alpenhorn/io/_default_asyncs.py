"""DefaultIO asyncs (functions that run asynchronously in tasks)."""

import os
import shutil

from . import ioutil
from .. import archive as ar

import logging

log = logging.getLogger(__name__)


def pull_async(task, node, req):
    """Pull ArchiveFileCopyRequest req onto the local filesystem.  This is asynchronous.

    Things to try:
        - hard link (for nodes on the same filesystem)
        - bbcp (if source is not on this host)
        - rsync if all else fails
    """

    # Is source and dest on the same host?
    local = req.node_from.host == node.host

    # Source spec
    if local:
        from_path = req.node_from.remote.file_path(req.file)
    else:
        try:
            from_path = req.node_from.remote.file_addr(req.file)
        except ValueError:
            log.warning(
                f"Skipping request for {req.file.acq.name}/{req.file.name} "
                f"due to unconfigured route to host for node {req.node_from.name}."
            )
            continue

    # Check that there is enough space available.
    #
    # XXX Is 2.0 the correct fudge factor here?
    reserved_space = 2 * req.file.size_b
    if node.io.reserve_bytes(reserved_space):
        # Automatically release bytes on task completion
        task.cleanup(node.io.release_bytes, args=(reserved_space,))
    else:
        log.warning(
            f"Skipping request for {req.file.acq.name}/{req.file.name}: "
            f"insufficient space on node {req.node_from.name}."
        )
        return

    to_dir = os.path.join(node.root, req.file.acq.name)
    if not os.path.exists(to_dir):
        log.info(f'Creating directory "{to_dir}".')
        os.makedirs(to_dir)

    # Giddy up!
    start_time = time.time()
    to_file = os.path.join(to_dir, req.file.name)
    log.info(f'Transferring file "{req.file.acq.name}/{req.file.name}":')

    # Attempt to transfer the file. Each of the methods below needs to return
    # a dict with required key:
    #  - ret: return code (0 == success)
    # optional keys:
    #  - md5sum: string or True; if True, the sum is guaranteed to be right;
    #            otherwise, it's a md5sum to check against the source
    #            must be present if ret == 0
    #  - stderr: string; if given, printed to the log when ret != 0
    #  - check_src: bool; if given and False, the source file will _not_ be
    #                     marked suspect when ret != 0; otherwise, a failure
    #                     results in a source check

    # First we need to check if we are copying over the network
    if not local:
        if shutil.which("bbcp") is not None:
            # First try bbcp which is a fast multistream transfer tool. bbcp can
            # calculate the md5 hash as it goes, so we'll do that to save doing
            # it at the end.
            ioresult = ioutil.bbcp(from_path, to_dir, req.file.size_b)
        elif shutil.which("rsync") is not None:
            # Next try rsync over ssh.
            ioresult = ioutil.rsync(from_path, to_dir, req.file.size_b, local)
        else:
            # We have no idea how to transfer the file...
            log.error("No commands available to complete remote pull.")
            ioresult = {"ret": -1, "check_src": False}

    else:
        # Okay, great we're just doing a local transfer.

        # First try to just hard link the file. This will only work if we
        # are on the same filesystem.  If it didn't work, ioresult will be None
        ioresult = ioutil.hardlink(from_path, to_dir, req.file.name)

        # If we couldn't just link the file, try copying it with rsync.
        if ioresult is None:
            if shutil.which("rsync") is not None:
                ioresult = ioutil.rsync(from_path, to_dir, req.file.size_b, local)
            else:
                log.error("No commands available to complete local pull.")
                ioresult = {"ret": -1, "check_src": False}

    if not ioutil.copy_request_done(
        req,
        node,
        check_src=ioresult.get("check_src", True),
        copy_size_b=os.stat(to_file).st_blocks * 512 if ioresult["ret"] == 0 else None,
        md5ok=ioresult["md5sum"],
        start_time=start_time,
        stderr=ioresult.get("stderr", None),
        success=(ioresutl["ret"] == 0),
    ):
        # Remove file, on error
        try:
            if os.path.lexists(to_file):
                os.remove(to_file)
        except OSError as e:
            log.error(f"Error removing corrupt file {to_file}: {e}")

    # Whatever has happened, update free space
    node.avail_gb = node.io.bytes_avail() / 2**30
    node.save(only=[st.StorageNode.avail_gb])
    log.info(f"Node {node.name} has {node.avail_gb:.2f} GiB available.")


def check_async(task, node, copy):
    """Check the MD5 sum of a file copy.  This is asynchronous."""

    copyname = os.path.join(
        copy.file.acq.name,
        copy.file.name,
    )

    fullpath = os.path.join(node.root, copyname)

    # Does the copy exist?
    if os.path.exists(fullpath):
        # It does, check the MD5 sum
        if util.md5sum_file(fullpath) == copy.file.md5sum:
            log.info(f"File {copyname} on node {node.name} is A-OK!")
            copy.has_file = "Y"
            copy.size_b = os.stat(fullpath).st_blocks * 512
        else:
            log.error(f"File {copyname} on node {node.name} is corrupt!")
            copy.has_file = "X"
    else:
        log.error(f"File {copyname} on node {node.name} is missing!")
        copy.has_file = "N"

    # Update the copy status
    log.info(f"Updating file copy #{copy.id} for file {copyname} on node {node.name}.")
    copy.save()


def import_async(task, node, acqname, filename, file_path):
    """Try to import a new file.  This is asynchronous.

    Passed acqusition name (dirname), filename, and the full path.
    """
    # Because this task may requeue itself, we need to check whether this
    # invocation is unnecessary
    if node.named_copy_present(acqname, filename):
        log.debug(f"Skipping import {file_path}: already present")
        return

    # What kind of file do we have?
    file_name = os.path.relpath(relpath, acq_name)
    ftype = ac.FileType.detect(file_name, acq, node)

    if ftype is None:
        log.info('Skipping unrecognised file "%s/%s".', acq_name, file_name)
        return

    # Add the file, if necessary.
    try:
        file_ = ac.ArchiveFile.get(
            ac.ArchiveFile.name == file_name, ac.ArchiveFile.acq == acq
        )
        log.debug('File "%s/%s" already in DB. Skipping.', acq_name, file_name)

    except pw.DoesNotExist:
        log.debug('Computing md5sum of "%s".', file_name)
        md5sum = util.md5sum_file(abspath, cmd_line=False)
        size_b = os.path.getsize(abspath)

        done = False
        while not done:
            try:
                with db.database_proxy.atomic():
                    file_ = ac.ArchiveFile.create(
                        acq=acq,
                        type=ftype,
                        name=file_name,
                        size_b=size_b,
                        md5sum=md5sum,
                    )

                    ftype.file_info.new(file_, node)

                done = True
            except pw.OperationalError as e:
                log.exception(e)
                log.error(
                    "MySQL connexion dropped. Will attempt to reconnect in "
                    "five seconds."
                )
                time.sleep(5)

                # TODO: re-implement
                # di.connect_database(True)
        log.info('File "%s/%s" added to DB.', acq_name, file_name)

    # Register the copy of the file here on the collection server, if (1) it
    # does not exist, or (2) if there has previously been a copy here ensure it
    # is checksummed to ensure the archives integrity.
    if not file_.copies.where(ar.ArchiveFileCopy.node == node).count():
        copy_size_b = os.stat(abspath).st_blocks * 512
        copy = ar.ArchiveFileCopy.create(
            file=file_,
            node=node,
            has_file="Y",
            wants_file="Y",
            prepared=False,
            size_b=copy_size_b,
        )
        log.info('Registered file copy "%s/%s" to DB.', acq_name, file_name)
    else:
        # Mark any previous copies as not being present...
        query = ar.ArchiveFileCopy.update(has_file="N").where(
            ar.ArchiveFileCopy.file == file_, ar.ArchiveFileCopy.node == node
        )
        query.execute()

        # ... then take the latest and mark it with has_file=M to force it to be
        # checked.
        copy = (
            ar.ArchiveFileCopy.select()
            .where(ar.ArchiveFileCopy.file == file_, ar.ArchiveFileCopy.node == node)
            .order_by(ar.ArchiveFileCopy.id)
            .get()
        )

        copy.has_file = "M"
        copy.wants_file = "Y"
        copy.save()
