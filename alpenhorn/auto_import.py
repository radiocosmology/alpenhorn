"""Routines for the importing of new files on a node."""

import logging
import os
import time

import peewee as pw
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from . import acquisition as ac
from . import archive as ar
from . import config, db, util

log = logging.getLogger(__name__)


def import_file(node, file_path):
    done = False
    while not done:
        try:
            _import_file(node, file_path)
            done = True
        except pw.OperationalError as e:
            log.exception(e)
            log.error(
                "MySQL connexion dropped. Will attempt to reconnect in " "five seconds."
            )
            time.sleep(5)
            # TODO: handle reconnection
            db.database_proxy.connect()


def in_directory(file, directory):
    """Test if file is contained within the directory. Does not check existence."""
    directory = os.path.join(directory, "")

    # return true, if the common prefix of both is equal to directory
    # e.g. /a/b/c/d.rst and directory is /a/b, the common prefix is /a/b
    return os.path.commonprefix([file, directory]) == directory


def _import_file(node, file_path):
    """Import a file into the DB.

    This routine adds the following to the database, if they do not already exist
    (or might be corrupted).
    - The acquisition that the file is a part of.
    - Information on the acquisition, if it is of type "corr".
    - The file.
    - Information on the file, if it is of type "corr".
    - Indicates that the file exists on this node.

    Parameters
    ----------
    node : storage.StorageNode
        The node we are processing.
    file_path : string
        Path of file on the node to import. If is is an absolute path it must
        be within the node root, otherwise is is assumed to be relative to
        the node root.
    """

    log.debug('Considering "%s" for import.', file_path)

    # Occasionally the watchdog sends events on the node root directory itself. Skip these.
    if file_path == node.root:
        log.debug('Skipping import request on the node root itself "%s"', node.root)
        return

    # Ensure the path is an absolute path within the node
    if os.path.isabs(file_path):
        if not in_directory(file_path, node.root):
            log.error(
                'File "%s" was not an absolute path within the node "%s"',
                file_path,
                node.root,
            )
            return
    else:
        file_path = os.path.join(node.root, file_path)
    abspath = os.path.normpath(file_path)

    # Skip requests to import a directory. Again these are occasionally sent by the watchdog
    if os.path.isdir(file_path):
        log.debug('Path to import "%s" is a directory. Skipping...', file_path)
        return

    relpath = os.path.relpath(abspath, node.root)

    # Skip the file if there is still a lock on it.
    dir_name, base_name = os.path.split(abspath)
    if os.path.isfile(os.path.join(dir_name, ".%s.lock" % base_name)):
        log.debug('Skipping "%s", which is locked by ch_master.py.', file_path)
        return

    # Check if we can handle this acquisition, and skip if we can't
    acq_type_name = ac.AcqType.detect(relpath, node)
    if acq_type_name is None:
        log.info('Skipping non-acquisition path "%s".', file_path)
        return

    # Figure out which acquisition this is; add if necessary.
    acq_type, acq_name = acq_type_name
    try:
        acq = ac.ArchiveAcq.get(ac.ArchiveAcq.name == acq_name)
        log.debug('Acquisition "%s" already in DB. Skipping.', acq_name)
    except pw.DoesNotExist:
        acq = add_acq(acq_type, acq_name, node)
        log.info('Acquisition "%s" added to DB.', acq_name)

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
            file=file_, node=node, has_file="Y", wants_file="Y", prepared=False, size_b=copy_size_b
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

    # TODO: imported files caching
    # if import_done is not None:
    #     bisect.insort_left(import_done, file_path)
    #     with open(LOCAL_IMPORT_RECORD, "w") as fp:
    #         fp.write("\n".join(import_done))


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
    def __init__(self, node):
        log.info('Registering node "%s" for auto_import watchdog.', node.name)
        self.node = node
        self.root = node.root
        if self.root[-1] == "/":
            self.root = self.root[0:-1]
        super(RegisterFile, self).__init__()

    def on_created(self, event):
        import_file(self.node, event.src_path)
        return

    def on_modified(self, event):
        import_file(self.node, event.src_path)
        return

    def on_moved(self, event):
        import_file(self.node, event.src_path)
        return

    def on_deleted(self, event):
        # For lockfiles: ensure that the file that was locked is added: it is
        # possible that the watchdog notices that a file has been closed before the
        # lockfile is deleted.
        dirname, basename = os.path.split(event.src_path)
        if basename[0] == "." and basename[-5:] == ".lock":
            basename = basename[1:-5]
            import_file(self.node, os.path.join(dirname, basename))


# Routines to control the filesystem watchdogs.
# =============================================

obs_list = None


def setup_observers(node_list):
    """Setup the watchdogs to look for new files in the nodes."""

    global obs_list

    # If any node has auto_import set, look for new files and add them to the
    # DB. Then set up a watchdog for it.
    obs_list = []
    for node in node_list:
        if node.auto_import:
            # TODO: Normal observers don't work via NFS so we use the polling
            # observer, however, we could try and detect this and switch back
            obs_list.append(
                PollingObserver(
                    timeout=config.config["service"]["auto_import_interval"]
                )
            )
            obs_list[-1].schedule(RegisterFile(node), node.root, recursive=True)
        else:
            obs_list.append(None)

    # Start up the watchdog threads
    for obs in obs_list:
        if obs:
            obs.start()


def catchup(node_list):
    """Traverse the node directory for new files and importem"""
    for node in node_list:
        if node.auto_import:
            # Get list of all files that exist on the node
            q = (
                ar.ArchiveFileCopy.select(ac.ArchiveFile.name, ac.ArchiveAcq.name)
                .where(
                    ar.ArchiveFileCopy.node == node, ar.ArchiveFileCopy.has_file == "Y"
                )
                .join(ac.ArchiveFile)
                .join(ac.ArchiveAcq)
            )

            already_imported_files = [os.path.join(a, f) for a, f in q.tuples()]

            log.info('Crawling base directory "%s" for new files.', node.root)

            for dirpath, d, f_list in os.walk(node.root):
                log.info('Crawling "%s".', dirpath)
                for file_name in sorted(f_list):

                    if file_name in already_imported_files:
                        log.debug('Skipping already-registered file "%s".', file_name)
                    else:
                        import_file(node, os.path.join(dirpath, file_name))


def stop_observers():
    """Stop watchidog threads."""
    for obs in obs_list:
        if obs:
            obs.stop()


def join_observers():
    """Wait for watchdog threads to terminate."""
    for obs in obs_list:
        if obs:
            obs.join()
