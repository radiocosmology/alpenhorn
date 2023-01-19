"""Routines for the importing of new files on a node."""

import os
import logging
import pathlib

import peewee as pw
from watchdog.events import FileSystemEventHandler

from . import acquisition as ac
from . import archive as ar
from . import config, db
from .task import Task

log = logging.getLogger(__name__)


def import_file(node, queue, path):
    """Queue a task to import path into node.

    Parameters
    ----------
    node : StorageNode
    queue : FairMultiFIFOQueue
    path : pathlib.PurePath
    """

    # Occasionally the watchdog sends events on the node root directory itself. Skip these.
    if path == pathlib.PurePath(node.root):
        log.debug("Skipping import request of node root")
        return

    # Strip node root, if path is absolute
    if path.is_absolute():
        try:
            path = path.relative_to(node.root)
        except ValueError:
            # Not rooted under node.root
            log.warning(f"skipping import of {path}: not rooted under node {node.root}")
            return

    # Ignore the node info file
    if str(path) == "ALPENHORN_NODE":
        log.debug(f"ignoring ALPENHORN_NODE file during import")
        return

    # New import task.  It's better to do this in a worker
    # because the worker pool will take care of DB connection loss.
    Task(
        func=_import_file,
        queue=queue,
        key=node.name,
        args=(node, path),
        name=f"Import {path} on {node.name}",
        # If the job fails due to DB connection loss, re-start the
        # task because unlike tasks made in the main loop, we're
        # never going to revisit this.
        requeue=True,
    )


def _import_file(task, node, path):
    """Import a file into the DB.  This is run by a worker.

    Parameters
    ----------
    task : task.Task
        The task running this function
    node : storage.StorageNode
        The node we are processing.
    path : pathlib.PurePath
        The path should be relative to node.root
    """

    log.debug(f'Considering "{path}" for import to node {node.name}.')

    # Wait for file to become ready
    while not node.io.ready_path(path):
        log.debug(
            f'Path "{path}" not ready for I/O during import.  Waiting 60 seconds.'
        )
        yield 60  # wait 60 seconds and re-queue

    # Check if we can handle this acquisition, and skip if we can't
    acqtype, acqname = ac.AcqType.detect(path, node)
    if acqtype is None:
        log.info(f'Skipping non-acquisition path "{path}".')
        return
    filename = path.relative_to(acqname)

    # If a copy already exists, we're done
    if node.named_copy_present(acqname, filename):
        log.debug(f"Skipping import of {path}: already present")
        return

    # What kind of file do we have?
    filetype = ac.FileType.detect(filename, node, acqtype, acqname)

    if filetype is None:
        log.info(f'Skipping unrecognised file "{path}".')
        return

    # Skip a file if there is still a lock on it.
    if node.io.locked(acqname, filename):
        log.debug(f'Skipping "{path}": locked.')
        return

    # Begin a transaction
    with db.database_proxy.atomic():
        # Add the acqusition, if necessary
        try:
            acq = ac.ArchiveAcq.get(ac.ArchiveAcq.name == acqname)
            log.debug(f'Acquisition "{acqname}" already in DB. Skipping.')
        except pw.DoesNotExist:
            # Create the ArchiveAcq entry and the AcqInfo entry for the acquisition.

            # Insert the archive record
            acq = ac.ArchiveAcq.create(name=acqname, type=acqtype)

            info_class = acqtype.info()
            if info_class.has_model():
                # Generate the acqinfo metadata
                info = info_class(path_=pathlib.Path(acqname), node_=node, acq=acq)
                info.save()
            log.info(f'Acquisition "{acqname}" added to DB.')

        # Add the file, if necessary.
        path = pathlib.PurePath(acqname, filename)
        try:
            file_ = ac.ArchiveFile.get(
                ac.ArchiveFile.name == filename, ac.ArchiveFile.acq == acq
            )
            log.debug(f'File "{path}" already in DB. Skipping.')
        except pw.DoesNotExist:
            log.debug(f'Computing md5sum of "{path}".')
            md5sum = node.io.md5sum_file(acqname, filename)
            size_b = node.io.filesize(path)

            file_ = ac.ArchiveFile.create(
                acq=acq,
                type=filetype,
                name=filename,
                size_b=size_b,
                md5sum=md5sum,
            )

            info_class = filetype.info()
            if info_class.has_model():
                # Generate the fileinfo metadata
                info = info_class(
                    path_=path,
                    node_=node,
                    acqtype_=acqtype,
                    acqname_=acqname,
                    file=file_,
                )
                info.save()
            log.info(f'File "{path}" added to DB.')

        # If we're importing a file that used to exist on this node, set has_file='M'
        # to trigger a integrity check.  (In this case, we can't have md5'ed it
        # above, because the ArchiveFile must have existed.)
        count = (
            ar.ArchiveFileCopy.update(has_file="M", wants_file="Y", ready=True)
            .where(ar.ArchiveFileCopy.file == file_, ar.ArchiveFileCopy.node == node)
            .execute()
        )
        if count > 0:
            log.warning(
                f'Imported file "{path}" formerly present on node {node.name}!  Marking suspect.'
            )
        else:
            # No existing file copy; create a new one.
            ar.ArchiveFileCopy.create(
                file=file_,
                node=node,
                has_file="Y",
                wants_file="Y",
                ready=True,
                size_b=node.io.filesize(path, actual=True),
            )
            log.info(f'Registered file copy "{path}" on node "{node.name}".')


# Watchdog stuff
# ==============


class RegisterFile(FileSystemEventHandler):
    def __init__(self, node, queue):
        self.node = node
        self.queue = queue
        self.root = node.root
        if self.root[-1] == "/":
            self.root = self.root[0:-1]
        super(RegisterFile, self).__init__()

    def on_created(self, event):
        import_file(self.node, self.queue, pathlib.PurePath(event.src_path))
        return

    def on_modified(self, event):
        import_file(self.node, self.queue, pathlib.PurePath(event.src_path))
        return

    def on_moved(self, event):
        import_file(self.node, self.queue, pathlib.PurePath(event.src_path))
        return

    def on_deleted(self, event):
        # For lockfiles: ensure that the file that was locked is added: it is
        # possible that the watchdog notices that a file has been closed before the
        # lockfile is deleted.
        dirname, basename = os.path.split(event.src_path)
        if basename[0] == "." and basename[-5:] == ".lock":
            basename = basename[1:-5]
            import_file(self.node, self.queue, pathlib.PurePath(dirname, basename))


# Routines to control the filesystem watchdogs.
# =============================================

# Observer threads.  One per node I/O class.
_observers = dict()

# Event watchers.  One per watched node.
_watchers = dict()


def update_observer(node, queue):
    """Start or stop auto-importing of a node"""

    io_class = "Default" if node.io_class is None else node.io_class

    if not node.auto_import:
        # If this node isn't being auto-imported, delete a watcher
        # if one was previously scheduled
        if node.name in _watchers:
            _observers[io_class].unschedule(_watchers[node.name])
            del _watchers[node.name]
    else:
        # If there's already a watcher for this node, do nothing
        if node.name in _watchers:
            return

        # Otherwise, if there is no existing observer for this node's
        # io_class, create a new one and start it.
        #
        # Different I/O classes may provide different observer classes to change
        # how notification happens.  One observer runs for each I/O class.
        if io_class not in _observers:
            observer = node.io.observer
            if observer is None:
                log.warning(
                    f'Unable to start auto import on node "{node.name}": '
                    f"no observer for I/O class {node.io_class}."
                )
                return

            _observers[io_class] = observer(
                timeout=config.config["service"]["auto_import_interval"]
            )

            _observers[io_class].start()
            log.debug(f"Started observer for I/O class {io_class}")

        # Schedule a new watcher for our observer
        log.info(f'Watching node "{node.name}" root "{node.root}" for auto import.')
        _watchers[node.name] = _observers[io_class].schedule(
            RegisterFile(node, queue), node.root, recursive=True
        )

        # Now catch up with the existing files to see if there are any new ones
        # that should be imported
        catchup(node, queue)


def stop_observers():
    """Stop all auto_import watchdogs."""

    global _observers, _watchers

    # Stop
    for obs in _observers.values():
        obs.stop()

    # Wait for termination
    for obs in _observers.values():
        obs.join()

    # Reset globals
    _observers = dict()
    _watchers = dict()


def catchup(node, queue):
    """Traverse the node directory for new files and importem"""

    # Get list of all files that exist on the node
    already_imported_files = node.all_files()

    log.info(f'Crawling node "{node.name}" root "{node.root}" for new files.')

    lastparent = None
    for file in node.io.file_walk():
        # Try to remove node.root
        try:
            file = file.relative_to(node.root)
        except ValueError:
            pass

        # Print directory as we pass through them
        parent = file.parent
        if parent != lastparent:
            log.info(f'Crawling "{parent}".')
            lastparent = parent

        # Skip files already imported
        if file in already_imported_files:
            log.debug(f'Skipping already-registered file "{file}".')
        else:
            import_file(node, queue, file)
