"""Default I/O file and directory deletion."""

from __future__ import annotations

import errno
import logging
import pathlib

from ...daemon.metrics import Metric
from ...daemon.scheduler import Task
from ...db import (
    ArchiveFileCopy,
    StorageNode,
    utcnow,
)
from .updownlock import UpDownLock

log = logging.getLogger(__name__)


def remove_filedir(
    node: StorageNode, dirname: pathlib.Path, tree_lock: UpDownLock
) -> None:
    """Try to delete a file's parent directory(s) from a node

    Will attempt to remove the enitre tree given as `dirname`
    while holding the `tree_lock` down until reaching `node.root`.
    Blocks until the lock can be acquired.

    The attempt to delete starts at `acq.name` and walks upwards until
    it runs out of path elements in `acq.name`.

    As soon as a non-empty directory is encountered, the attempt stops
    without raising an error.

    If `acq.name` is missing, or partially missing, that is not an error
    either, but an attempt to delete the part remaining will still be
    attempted.

    Parameters
    ----------
    node: StorageNode
        The node to delete the acq directory from.
    dirname: pathlib.Path
        The path to delete.  Must be absolute and rooted at `node.root`.
    tree_lock: UpDownLock
        This function will block until it can acquire the down lock and
        all I/O will happen while holding the lock down.

    Raises
    ------
    ValueError
        `dirname` was not a subdirectory of `node.root`
    """
    # Sanity check
    if not dirname.is_relative_to(node.root):
        raise ValueError(f"dirname {dirname} not rooted under {node.root}")

    # try to delete the directories.  This must be done while locking down the tree lock
    with tree_lock.down:
        while str(dirname) != node.root:
            try:
                dirname.rmdir()
                log.info(f"Removed directory {dirname} on {node.name}")
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    # This is fine, but stop trying to rmdir.
                    break
                if e.errno == errno.ENOENT:
                    # Already deleted, which is fine.
                    pass
                else:
                    log.warning(
                        f"Error deleting directory {dirname} on {node.name}: {e}"
                    )
                    # Otherwise, let's try to soldier on

            dirname = dirname.parent


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
        remove_filedir(copy.node, fullpath.parent, tree_lock)

        # Update the DB
        ArchiveFileCopy.update(
            has_file="N", wants_file="N", last_update=utcnow()
        ).where(ArchiveFileCopy.id == copy.id).execute()
