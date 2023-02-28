"""DefaultIO asyncs (functions that run asynchronously in tasks)."""
from __future__ import annotations
from typing import TYPE_CHECKING

import logging

if TYPE_CHECKING:
    from .base import BaseNodeIO
    from ..archive import ArchiveFileCopy
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
    copy.save()
