"""Check a suspect file."""

from __future__ import annotations

import logging
import pathlib

import peewee as pw

from ...common.metrics import Metric
from ...common.util import timeout_call
from ...daemon.scheduler import Task
from ...db import (
    ArchiveFile,
    ArchiveFileCopy,
    StorageNode,
    utcnow,
)
from ..base import BaseNodeIO

log = logging.getLogger(__name__)


def force_check_filecopy(file_: ArchiveFile, node: StorageNode, node_io: BaseNodeIO):
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
            size_b=node_io.storage_used(file_.path),
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


def check_async(
    task: Task, io: BaseNodeIO, copy: ArchiveFileCopy, path: pathlib.Path | None = None
) -> None:
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
        The I/O instance on which the file copy lives.
    copy : ArchiveFileCopy
        The file copy to check.
    path : pathlib.Path, optional
        If not None, this is the absolute path to the file.  This parameter is
        not used by Default I/O but is provided as a convenience to other I/O
        Classes which wish to re-use this I/O async.  If not given,
        `copy.path` is used.
    """

    copyname = copy.file.path
    fullpath = path if path else copy.path

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
                copy.size_b = io.storage_used(fullpath)
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
