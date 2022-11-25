"""Routines for the importing of new files on a node."""

import os
import time
from pathlib import PurePath

import peewee as pw
from watchdog.events import FileSystemEventHandler

from . import acquisition as ac
from . import archive as ar
from . import config, db, util

import logging

log = logging.getLogger(__name__)


def import_file(node, file_path, filename=None):
    """Import a file into the DB.

    Parameters
    ----------
    node : storage.StorageNode
        The node we are processing.
    file_path : string
        If filename is None, the path of the file on the node to import.
        If is is an absolute path it must be within the node root,
        otherwise is is assumed to be relative to the node root.
        If filename is not None, this is the acqname
    filename : if not None, the name of the file.
    """

    log.debug(f'Considering "{file_path}" for import to node {node.name}.')

    # Occasionally the watchdog sends events on the node root directory itself. Skip these.
    if file_path == node.root:
        log.debug("Skipping import request on node root")
        return

    # Sort out acqname and filename.
    #
    # This function is either called by catchup(), which provides acqname (in
    # file_path) and filename directly, or else by the observer, which is going to
    # (at least in the DefaultIO case) return an absolute path to a file
    if filename is None:
        path = PurePath(file_path)

        # Try to strip node.root if this is an absolute path
        if path.is_absolute():
            try:
                path = path.relative_to(node.root)
            except ValueError:
                # Not rooted under node.root
                log.debug(
                    "skipping import of {file_path}: not rooted under node {node.root}"
                )
                return
        acqname = str(path.parent)
        filename = str(path.name)

        # i.e. a single element path was specified:
        if acqname == ".":
            log.debug(
                "skipping import of {file_path}: unable to separate acq and file names"
            )
            return
    else:
        acqname = file_path

    # If a copy already exists, we're done
    if node.named_copy_present(acqname, filename):
        log.debug(f"Skipping import {file_path}: already present")
        return

    # Hand off to the I/O layer
    node.io.import_file(acqname, filename)


# Routines for registering files, acquisitions, copies and info in the DB.
# ========================================================================


def add_acq(acq_type, name, node, comment=""):
    """Add an aquisition to the database.

    This looks for an appropriate acquisition type, and if successful creates
    the ArchiveAcq and AcqInfo entries for the acquisition.

    Parameters
    ----------
    acq_type : AcqType
        Type of the acquisition
    name : string
        Name of the acquisition directory.
    node : StorageNode
        Node that the acquisition is on.
    comment : string, optional
        An optional comment.

    Returns
    -------
    acq : ArchiveAcq
        The ArchiveAcq entry.
    acqinfo : AcqInfoBase
        The AcqInfo entry.
    """

    # Is the acquisition already in the database?
    if ac.ArchiveAcq.select(ac.ArchiveAcq.id).where(ac.ArchiveAcq.name == name).count():
        raise AlreadyExists('Acquisition "%s" already exists in DB.' % name)

    # Create the ArchiveAcq entry and the AcqInfo entry for the acquisition. Run
    # in a transaction so we don't end up with inconsistency.
    with db.database_proxy.atomic():
        # Insert the archive record
        acq = ac.ArchiveAcq.create(name=name, type=acq_type, comment=comment)

        # Generate the metadata table
        acq_type.acq_info.new(acq, node)

    return acq


# Exceptions
# ==========


class Validation(Exception):
    """Raise when validation of a name or field fails."""


class DataBaseError(Exception):
    """Raise when there is some internal inconsistency with the database."""


class AlreadyExists(Exception):
    """Raise when a record already exists in the database."""


class DataFlagged(Exception):
    """Raised when data is affected by a global flag."""


# Watchdog stuff
# ==============


class RegisterFile(FileSystemEventHandler):
    def __init__(self, node, queue):
        log.info(f'Registering node "{node.name}" for auto_import watchdog.')
        self.node = node
        self.queue = queue
        self.root = node.root
        if self.root[-1] == "/":
            self.root = self.root[0:-1]
        super(RegisterFile, self).__init__()

    def on_created(self, event):
        import_file(self.node, self.queue, event.src_path)
        return

    def on_modified(self, event):
        import_file(self.node, self.queue, event.src_path)
        return

    def on_moved(self, event):
        import_file(self.node, self.queue, os.path.split(event.src_path))
        return

    def on_deleted(self, event):
        # For lockfiles: ensure that the file that was locked is added: it is
        # possible that the watchdog notices that a file has been closed before the
        # lockfile is deleted.
        dirname, basename = os.path.split(event.src_path)
        if basename[0] == "." and basename[-5:] == ".lock":
            basename = basename[1:-5]
            import_file(self.node, self.queue, dirname, basename)


# Routines to control the filesystem watchdogs.
# =============================================

obs_list = None


def setup_observers(node_list, queue):
    """Setup the watchdogs to look for new files in the nodes."""

    global obs_list

    # If any node has auto_import set, look for new files and add them to the
    # DB. Then set up a watchdog for it.
    obs_list = []
    for node in node_list:
        if not node.auto_import:
            continue

        obs_list.append(
            node.io.observer(timeout=config.config["service"]["auto_import_interval"])
        )
        obs_list[-1].schedule(RegisterFile(node), node.root, recursive=True)

    # Start up the watchdog threads
    for obs in obs_list:
        obs.start()


def catchup(node_list, queue):
    """Traverse the node directory for new files and importem"""
    for node in node_list:
        if not node.auto_import:
            continue

        # Get list of all files that exist on the node
        already_imported_files = node.all_files()

        log.info(f'Crawling base directory "{node.root}" for new files.')

        for acqdir in node.io.acq_walk():
            log.info(f'Crawling "{acqdir}".')
            for file_name in node.io.file_walk(acqdir):
                if (acqdir, file_name) in already_imported_files:
                    log.debug(f'Skipping already-registered file "{file_name}".')
                else:
                    import_file(node, queue, acqdir, file_name)


def stop_observers():
    """Stop watchidog threads."""
    for obs in obs_list:
        obs.stop()


def join_observers():
    """Wait for watchdog threads to terminate."""
    for obs in obs_list:
        obs.join()
