"""DefaultIO asyncs (functions that run asynchronously in tasks)."""

from __future__ import annotations

import errno
import logging
import pathlib
import shutil
import time

import peewee as pw

from ..common.metrics import Metric
from ..common.util import timeout_call
from ..daemon import RemoteNode
from ..db import (
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageNode,
    utcnow,
)
from ..scheduler import Task
from . import ioutil
from .base import BaseGroupIO, BaseNodeIO
from .updownlock import UpDownLock

log = logging.getLogger(__name__)


def _force_check_filecopy(file_: ArchiveFile, node: StorageNode, node_io: BaseNodeIO):
    """Force a check of an unregistered file.

    Upserts an ArchiveFileCopy record to force a check of an
    unregistered file copy on a node.

    Parameters
    ----------
    file_ : ArchiveFile
        The file to check
    node : StorageNode
        The node to run the check on
    node_io : BaseNodeIO
        The node I/O instance.  Used if we need to calculate
        the file size.
    """
    log.info(f"Requesting check of {file_.acq.name}/{file_.name} on node {node.name}.")

    # ready == False is the safe option here: copy will be readied
    # during the subsequent check if needed.
    try:
        # Try to create a new copy
        ArchiveFileCopy.create(
            file=file_,
            node=node,
            has_file="M",
            wants_file="Y",
            ready=False,
            size_b=node_io.filesize(file_.path, actual=True),
        )
    except pw.IntegrityError:
        # Copy already exists, just update the existing
        ArchiveFileCopy.update(
            has_file="M",
            wants_file="Y",
            ready=False,
            last_update=utcnow(),
        ).where(
            ArchiveFileCopy.file == file_,
            ArchiveFileCopy.node == node,
        ).execute()


def pull_async(
    task: Task,
    io: BaseNodeIO,
    tree_lock: UpDownLock,
    req: ArchiveFileCopyRequest,
    did_search: bool,
) -> None:
    """Fulfill `req` by pulling a file onto the local node.

    Things to try:
        - hard link (for nodes on the same filesystem)
        - bbcp (remote transfers only)
        - rsync
        - shutil.copy (local transfers only)

    Parameters
    ----------
    task : Task
        The task instance containing this async.
    io : Node I/O instance
        The I/O instance for the pull destination node.
    tree_lock : UpDownLock
        The directory tree modificiation lock.
    req : ArchiveFileCopyRequest
        The request we're fulfilling.
    did_search : bool
        True if a search for an existing unregistered copy was
        already performed.
    """

    # Before we were queued, NodeIO reserved space for this file.
    # Automatically release bytes on task completion
    task.on_cleanup(io.release_bytes, args=(req.file.size_b,))

    pullrun_metric = Metric(
        "pull_running_count",
        "Count of in-progress pulls",
        unbound={"method", "remote"},
        bound={"node": io.node.name},
    )

    # Rerun the database checks, because we don't know how long we've been in the queue
    if not req.check(node_to=io.node):
        return

    # We know dest is local, so if source is too, this is a local transfer
    local = req.node_from.local

    # The Remote Node
    remote = RemoteNode(req.node_from)

    # Source spec
    if local:
        from_path = remote.io.file_path(req.file)
    else:
        try:
            from_path = remote.io.file_addr(req.file)
        except ValueError:
            log.warning(
                f"Skipping request for {req.file.path} "
                f"due to unconfigured route to host for node {req.node_from.name}."
            )
            return

    to_file = pathlib.Path(io.node.root, req.file.path)
    to_dir = to_file.parent

    # Check for existing file, if not done already
    if not did_search:
        try:
            if io.exists(req.file.path):
                log.warning(
                    "Skipping pull request for "
                    f"{req.file.acq.name}/{req.file.name}: "
                    f"file already on disk on node {io.node.name}."
                )

                _force_check_filecopy(req.file, io.node, io)
                # request not resolved.  Should be sorted out after
                # the file check happens.
                return
        except OSError:
            # On error, try to do the pull
            pass

    # Placeholder file
    placeholder = pathlib.Path(to_dir, f".{to_file.name}.placeholder")

    # Create directories.  This must be done while locking up the tree lock
    with tree_lock.up:
        if not to_dir.exists():
            log.info(f'Creating directory "{to_dir}".')
            to_dir.mkdir(parents=True, exist_ok=True)

        # If the file doesn't exist, create a placeholder so we can release
        # the tree lock without having to wait for the transfer to complete
        if not to_file.exists():
            placeholder.touch(mode=0o600, exist_ok=True)

    # Giddy up!
    start_time = time.time()

    # Attempt to transfer the file. Each of the methods below needs to return
    # a dict with required key:
    #  - ret : integer
    #        return code (0 == success)
    # optional keys:
    #  - md5sum : string or True
    #        If True, the sum is guaranteed to be right; otherwise, it's a
    #        md5sum to check against the source.  Must be present if ret == 0
    #  - stderr : string
    #        if given, printed to the log when ret != 0
    #  - check_src : bool
    #        if given and False, the source file will _not_ be marked suspect
    #        when ret != 0; otherwise, a failure results in a source check

    # First we need to check if we are copying over the network
    if not local:
        if shutil.which("bbcp") is not None:
            # First try bbcp which is a fast multistream transfer tool. bbcp can
            # calculate the md5 hash as it goes, so we'll do that to save doing
            # it at the end.
            log.info(f"Pulling remote file {req.file.path} using bbcp")
            pullrun_metric.inc(method="bbcp", remote="1")
            ioresult = ioutil.bbcp(from_path, to_file, req.file.size_b)
            pullrun_metric.dec(method="bbcp", remote="1")
        elif shutil.which("rsync") is not None:
            # Next try rsync over ssh.
            log.info(f"Pulling remote file {req.file.path} using rsync")
            pullrun_metric.inc(method="rsync", remote="1")
            ioresult = ioutil.rsync(from_path, to_file, req.file.size_b, local)
            pullrun_metric.dec(method="rsync", remote="1")
        else:
            # We have no idea how to transfer the file...
            log.error("No commands available to complete remote pull.")
            ioresult = {"ret": -1, "check_src": False}

    else:
        # Okay, great we're just doing a local transfer.

        # First try to just hard link the file. This will only work if we
        # are on the same filesystem.  If it didn't work, ioresult will be None
        #
        # But don't do this if it creates a hardlink between an archive node and
        # a non-archive node
        if req.node_from.archive == io.node.archive:
            pullrun_metric.inc(method="link", remote="0")
            ioresult = ioutil.hardlink(from_path, to_dir, req.file.name)
            pullrun_metric.dec(method="link", remote="0")
            if ioresult is not None:
                log.info(f"Hardlinked local file {req.file.path}")
        else:
            ioresult = None

        # If we couldn't just link the file, try copying it with rsync.
        if ioresult is None:
            if shutil.which("rsync") is not None:
                log.info(f"Pulling local file {req.file.path} using rsync")
                pullrun_metric.inc(method="rsync", remote="0")
                ioresult = ioutil.rsync(from_path, to_file, req.file.size_b, local)
                pullrun_metric.dec(method="rsync", remote="0")
            else:
                # No rsync?  Just use shutil.copy, I guess
                log.warning("Falling back on shutil.copy to complete local pull.")
                pullrun_metric.inc(method="internal", remote="0")
                ioresult = ioutil.local_copy(
                    from_path, to_dir, req.file.name, req.file.size_b
                )
                pullrun_metric.dec(method="internal", remote="0")

    # Delete the placeholder, if we created it
    placeholder.unlink(missing_ok=True)

    if not ioutil.copy_request_done(
        req,
        io,
        check_src=ioresult.get("check_src", True),
        md5ok=ioresult.get("md5sum", None),
        start_time=start_time,
        stderr=ioresult.get("stderr", None),
        success=(ioresult["ret"] == 0),
    ):
        # Remove file, on error
        try:
            to_file.unlink(missing_ok=True)
        except OSError as e:
            log.error(f"Error removing corrupt file {to_file}: {e}")

    # Whatever has happened, update free space, if possible
    new_avail = io.bytes_avail(fast=True)

    # This was a fast update, so don't save "None" to the database
    if new_avail is not None:
        io.node.update_avail_gb(new_avail)


def check_async(task: Task, io: BaseNodeIO, copy: ArchiveFileCopy) -> None:
    """Check a file copy.  This is asynchronous.

    First checks for a size mismatch.  If that's okay, then will
    calculat the MD5 hash of the file copy.

    Updates `copy.has_file` based on the result of the check to one of:
    - 'Y': file is not corrupt
    - 'X': file is corrupt
    - 'N': file is missing

    Parameters
    ----------
    task : Task
        The task instance containing this async.
    io : Node I/O instance
        The I/O instance on which the file copy lives
    copy : ArchiveFileCopy
        The copy to check
    """

    copyname = copy.file.path
    fullpath = copy.path

    Metric(
        "verification_checks",
        "Count of verification checks",
        counter=True,
        bound={"node": io.node.name},
    ).inc()

    # Does the copy exist?
    if fullpath.exists():
        # First check the size
        try:
            size = timeout_call(fullpath.stat, 600).st_size
        except (OSError, TimeoutError):
            # Abandon the check attempt if we can't stat
            return

        if copy.file.size_b and size != copy.file.size_b:
            log.error(
                f"File {copyname} on node {io.node.name} is corrupt! "
                f"Size: {size}; expected: {copy.file.size_b}"
            )
            copy.has_file = "X"
        else:
            # If size is okay, check MD5 sum
            md5sum = io.md5(fullpath)
            if md5sum == copy.file.md5sum:
                log.info(f"File {copyname} on node {io.node.name} is A-OK!")
                copy.has_file = "Y"
                copy.size_b = io.filesize(fullpath, actual=True)
            else:
                log.error(
                    f"File {copyname} on node {io.node.name} is corrupt! "
                    f"MD5: {md5sum}; expected: {copy.file.md5sum}"
                )
                copy.has_file = "X"
    else:
        log.error(f"File {copyname} on node {io.node.name} is missing!")
        copy.has_file = "N"

    # Update the copy status
    log.info(
        f"Updating file copy #{copy.id} for file {copyname} on node {io.node.name}."
    )
    copy.last_update = utcnow()
    copy.save()


def delete_async(
    task: Task, tree_lock: UpDownLock, copies: list[ArchiveFileCopy]
) -> None:
    """Delete some file copies, if possible.

    Copies are only deleted if sufficient archived copies exist
    elsewhere.  If there aren't sufficient copies, the deletion doesn't
    happen, but the request to delete isn't resolved (so a future
    update loop will again try to delete the copy).

    Parameters
    ----------
    task : Task
        The task instance containing this async.
    tree_lock : UpDownLock
        The directory tree modificiation lock.
    copies : list of ArchiveFileCopy
        The list of copies to delete.  Never empty.
    """

    # Node name
    name = copies[0].node.name

    # The number archive copies needed to delete a copy.
    # Need at least two _other_ copies to be able to delete a file.
    copies_required = 3 if copies[0].node.archive else 2

    # Process candidates for deletion
    for copy in copies:
        shortname = copy.file.path

        # Archived count
        ncopies = copy.file.archive_count

        # If at least two _other_ copies exist, we can delete the file.
        if ncopies < copies_required:
            log.warning(
                f"Too few archive copies ({ncopies}) to delete {shortname} on {name}."
            )
            continue  # Skip this one

        fullpath = copy.path
        try:
            fullpath.unlink()  # Remove the actual file
            Metric(
                "deleted_files",
                "Count of deleted files",
                counter=True,
                bound={"node": name},
            ).inc()
            if copy.file.size_b:
                Metric(
                    "deleted_bytes",
                    "Size of deleted files",
                    counter=True,
                    bound={"node": name},
                ).add(copy.file.size_b)
            log.info(f"Removed file copy {shortname} on {name}")
        except OSError as e:
            if e.errno == errno.ENOENT:
                # Already deleted, which is not a problem.
                log.info(f"File copy {shortname} missing on {name} during delete")
            else:
                log.warning(f"Error deleting {shortname}: {e}")
                continue  # Welp, that didn't work

        # Check if any containing directory is now empty
        # and remove if they are.
        ioutil.remove_filedir(copy.node, fullpath.parent, tree_lock)

        # Update the DB
        ArchiveFileCopy.update(
            has_file="N", wants_file="N", last_update=utcnow()
        ).where(ArchiveFileCopy.id == copy.id).execute()


def group_search_async(
    task: Task,
    groupio: BaseGroupIO,
    req: ArchiveFileCopyRequest,
) -> None:
    """Check group `io` for an existing, unregistered `req.file`.

    If the file is found, a request is made to check the
    existing file, and the pull is skipped.  If the file
    is not found, `req` is passed to `groupio.pull`, which
    dispatch the ArchvieFileCopyRequest to a node in the
    group to perform the pull request.

    Parameters
    ----------
    task : Task
        The task instance containing this async.
    io : Group I/O instance
        The I/O instance for the pull destination group.
    req : ArchiveFileCopyRequest
        The request we're fulfilling.
    """

    # Before doing anything re-check the DB for something
    # in this group.  The situation may have changed while this
    # task was queued.
    state = groupio.group.state_on_node(req.file)[0]
    if state == "Y" or state == "M":
        log.info(
            "Cancelling pull request for "
            f"{req.file.acq.name}/{req.file.name}: "
            f"file already in group {groupio.group.name}."
        )
        req.cancelled = 1
        req.save()
        return

    # Check whether an actual file exists on the target
    found_missing = False
    for node in groupio.nodes:
        if node.io.exists(req.file.path):
            # file on disk: is it known?
            #
            # Acreate/update the ArchiveFileCopy to force a check next pass
            copy_state = node.db.filecopy_state(req.file)

            if copy_state == "N":
                # Update/create ArchiveFileCopy to force a check.
                _force_check_filecopy(req.file, node.db, node.io)
                found_missing = True

    # If we found something, warn and stop
    if found_missing:
        log.warning(
            "Skipping pull request for "
            f"{req.file.acq.name}/{req.file.name}: "
            f"file already on disk in group {groupio.group.name}."
        )
        return

    # Otherwise, escalate to groupio.pull to actually perform the pull
    groupio.pull(req, did_search=True)
