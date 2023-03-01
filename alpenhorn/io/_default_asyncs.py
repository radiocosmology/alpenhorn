"""DefaultIO asyncs (functions that run asynchronously in tasks)."""
from __future__ import annotations
from typing import TYPE_CHECKING

import errno
import logging
from datetime import datetime

from ..archive import ArchiveFileCopy

if TYPE_CHECKING:
    from .base import BaseNodeIO
    from ..task import Task

log = logging.getLogger(__name__)


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

    # Does the copy exist?
    if fullpath.exists():
        # First check the size
        size = fullpath.stat().st_size
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
    copy.last_update = datetime.now()
    copy.save()


def delete_async(task: Task, copies: list[ArchiveFileCopy]) -> None:
    """Delete some file copies, if possible.

    Copies are only deleted if sufficient archived copies exist
    elsewhere.  If there aren't sufficient copies, the deletion doesn't
    happen, but the request to delete isn't resolved (so a future
    update loop will again try to delete the copy).

    Parameters
    ----------
    task : Task
        The task instance containing this async.
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
                f"Too few archive copies ({ncopies}) "
                f"to delete {shortname} on {name}."
            )
            continue  # Skip this one

        fullpath = copy.path
        try:
            fullpath.unlink()  # Remove the actual file
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
        dirname = fullpath.parent

        # try to delete the directories.
        while dirname != ".":
            try:
                dirname.rmdir()
                log.info(f"Removed directory {dirname} on {name}")
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    # This is fine, but stop trying to rmdir.
                    break
                elif e.errno == errno.ENOENT:
                    # Already deleted, which is fine.
                    pass
                else:
                    log.warning(f"Error deleting directory {dirname} on {name}: {e}")
                    # Otherwise, let's try to soldier on

            dirname = dirname.parent

        # Update the DB
        ArchiveFileCopy.update(
            has_file="N", wants_file="N", last_update=datetime.now()
        ).where(ArchiveFileCopy.id == copy.id).execute()
