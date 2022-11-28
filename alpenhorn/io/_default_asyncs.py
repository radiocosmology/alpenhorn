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

    # Before we were queued, NodeIO reserved space for this file.
    # Automatically release bytes on task completion
    task.cleanup(node.io.release_bytes, args=(req.file.size_b,))

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

    copyname = copy.file.path
    fullpath = copy.path

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


def delete_async(task, node, copies):
    """Delete some file copies, if possible."""

    # Process candidates for deletion
    for fcopy in del_files:
        # Archived count
        ncopies = fcopy.file.archive_count()

        shortname = fcopy.file.path
        fullpath = fcopy.path

        # If at least two _other_ copies exist, we can delete the file.
        if ncopies >= (3 if copy.node.archive else 2):
            if os.path.exists(fullpath):
                try:
                    os.remove(fullpath)  # Remove the actual file
                except OSError as e:
                    log.warning(f"Error deleting {shortname}: {e}")
                    continue  # Welp, that didn't work

                log.info(f"Removed file copy {shortname} on {node.name}")

            # Check if any containing directory is now empty
            # and remove if they are.
            dirname = shortname.parent

            while dirname != ".":
                if any(os.scandir(dirname)):
                    break  # There was something in the directory

                # dirname is empty; delete it.
                try:
                    os.rmdir(dirname)
                    log.info("Removed directory {dirname} on {node.name}")
                except OSError as e:
                    log.warning(
                        f"Error deleting directory {dirname} on {node.name}: {e}"
                    )
                    # Maybe it failed because it didn't exist; so we'll try to soldier on

                dirname = dirname.parent

            # Update the DB
            ar.ArchiveFileCopy.update(has_file="N", wants_file="N").where(
                ar.ArchiveFileCopy.id == fcopy.id
            ).execute()
        else:
            log.warning(
                f"Too few archive copies ({ncopies}) to delete {shortname} on {node.name}."
            )
