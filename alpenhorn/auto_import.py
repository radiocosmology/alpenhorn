"""Routines for the importing of new files on a node."""
from __future__ import annotations
from typing import TYPE_CHECKING

import logging
import pathlib
import peewee as pw
from datetime import datetime
from watchdog.events import FileSystemEventHandler

from . import config, db, extensions
from .acquisition import ArchiveAcq, ArchiveFile
from .archive import ArchiveFileCopy
from .io import ioutil
from .task import Task

if TYPE_CHECKING:
    from .queue import FairMultiFIFOQueue
    from .storage import StorageNode
    from .update import UpdateableNode


log = logging.getLogger(__name__)


def import_file(
    node: UpdateableNode, queue: FairMultiFIFOQueue, path: pathlib.PurePath
) -> None:
    """Queue a task to import `path` into `node`.

    Most of the import is done in a task.

    Parameters
    ----------
    node : UpdateableNode
        The node we're importing onto
    queue : FairMultiFIFOQueue
        The tasks queue
    path : pathlib.PurePath
        The path we're trying to import
    """

    # Occasionally the watchdog sends events on the node root directory itself. Skip these.
    if path == pathlib.PurePath(node.db.root):
        log.debug("Skipping import request of node root")
        return

    # Strip node root, if path is absolute
    if path.is_absolute():
        try:
            path = path.relative_to(node.db.root)
        except ValueError:
            # Not rooted under node.root
            log.warning(
                f"skipping import of {path}: not rooted under node {node.db.root}"
            )
            return

    # Ignore the node info file.  NB: this happens even on nodes which
    # don't use an ALPENHORN_NODE file, meaning even on those nodes,
    # this path is disallowed as a data file.
    if str(path) == "ALPENHORN_NODE":
        log.debug("ignoring ALPENHORN_NODE file during import")
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


def _import_file(task: Task, node: StorageNode, path: pathlib.PurePath) -> None:
    """Import `path` on `node` into the DB.  This is run by a worker.

    Parameters
    ----------
    task : task.Task
        The task running this function
    node : storage.StorageNode
        The node we are processing.
    path : pathlib.PurePath
        The path should be relative to `node.root`
    """

    log.debug(f'Considering "{path}" for import to node {node.name}.')

    # Wait for file to become ready
    while not node.io.ready_path(path):
        log.debug(
            f'Path "{path}" not ready for I/O during import.  Waiting 60 seconds.'
        )
        yield 60  # wait 60 seconds and re-queue

    # Skip the file if there is still a lock on it.
    if node.io.locked(path):
        log.debug(f'Skipping "{path}": locked.')
        return

    # Step through the detection extensions to find one that's willing
    # to handle this file
    for detector in extensions.import_detection():
        acq_name, callback = detector(path, node)
        if acq_name is not None:
            break
    else:
        # Detection failed, so we're done.
        log.info(f"Skipping non-acquisition path: {path}")
        return

    file_name = path.relative_to(acq_name)

    # If a copy already exists, we're done
    if node.db.named_copy_tracked(acq_name, file_name):
        log.debug(f"Skipping import of {path}: already known")
        return

    # Begin a transaction
    with db.database_proxy.atomic():
        # Add the acqusition, if necessary
        try:
            acq = ArchiveAcq.get(ArchiveAcq.name == acq_name)
            new_acq = None
            log.debug(f'Acquisition "{acq_name}" already in DB. Skipping.')
        except pw.DoesNotExist:
            acq = ArchiveAcq.create(name=acq_name)
            new_acq = acq
            log.info(f'Acquisition "{acq_name}" added to DB.')

        # Add the file, if necessary.
        try:
            file_ = ArchiveFile.get(
                ArchiveFile.name == file_name, ArchiveFile.acq == acq
            )
            new_file = None
            log.debug(f'File "{path}" already in DB. Skipping.')
        except pw.DoesNotExist:
            log.debug(f'Computing md5sum of "{path}".')
            md5sum = node.io.md5(acq_name, file_name)
            size_b = node.io.filesize(path)

            file_ = ArchiveFile.create(
                acq=acq, name=file_name, size_b=size_b, md5sum=md5sum
            )
            new_file = file_
            log.info(f'File "{path}" added to DB.')

        # If we're importing a file that used to exist on this node, set has_file='M'
        # to trigger a integrity check.  (In this case, we can't have md5'ed it
        # above, because the ArchiveFile must have existed.)
        try:
            copy = ArchiveFileCopy.get(file=file_, node=node.db)
            copy.has_file = "M"
            copy.wants_file = "Y"
            copy.ready = True
            copy.last_update = datetime.utcnow()
            copy.save()
            log.warning(
                f'Imported file "{path}" formerly present on node {node.name}!  Marking suspect.'
            )
        except pw.DoesNotExist:
            # No existing file copy; create a new one.
            copy = ArchiveFileCopy.create(
                file=file_,
                node=node.db,
                has_file="Y",
                wants_file="Y",
                ready=True,
                size_b=node.io.filesize(path, actual=True),
                last_update=datetime.utcnow(),
            )
            log.info(f'Registered file copy "{path}" on node "{node.name}".')

    # Run post-add actions, if any
    ioutil.post_add(node.db, file_)

    # Run the extension module's callback, if necessary
    if callable(callback):
        callback(copy, new_file, new_acq, node)


# Watchdog stuff
# ==============


class RegisterFile(FileSystemEventHandler):
    """
    A watchdog.FileSystemEventHandler subclass handling watchdog
    events on a storage node.

    Parameters
    ----------
    node : UpdateableNode
        The node we're watching
    queue : FairMultiFIFOQueue
        The task queue.  Import tasks will be submitted to this queue.
    """

    def __init__(self, node: UpdateableNode, queue: FairMultiFIFOQueue) -> None:
        self.node = node
        self.queue = queue
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
        path = pathlib.Path(event.src_path)
        basename = path.name
        if basename[0] == "." and basename[-5:] == ".lock":
            basename = basename[1:-5]
            import_file(self.node, self.queue, path.with_name(basename))


# Routines to control the filesystem watchdogs.
# =============================================

# Observer threads.  One per node I/O class.
_observers = dict()

# Event watchers.  One per watched node.
_watchers = dict()


def update_observer(
    node: UpdateableNode, queue: FairMultiFIFOQueue, force_stop: bool = False
) -> None:
    """Update (start or stop) an auto-import observer for `node`, if needed.

    Parameters
    ----------
    node : UpdateableNode
        The node we're updating the observer for
    queue : FairMultiFIFOQueue
        The task queue
    force_stop : bool, optional
        If True, a running observer is stopped, even if auto_import is enabled
    """

    io_class = "Default" if node.io_class is None else node.io_class

    if force_stop or not node.db.auto_import:
        # If this node isn't being auto-imported or we're force-stopping
        # it, delete a watcher if one was previously scheduled
        if node.name in _watchers:
            _observers[node.io_class].unschedule(_watchers[node.name])
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
        if node.io_class not in _observers:
            observer = node.io.observer
            if observer is None:
                log.warning(
                    f'Unable to start auto import on node "{node.name}": '
                    f"no observer for I/O class {node.io_class}."
                )
                return

            _observers[node.io_class] = observer(
                timeout=config.config["service"]["auto_import_interval"]
            )

            _observers[node.io_class].start()
            log.debug(f"Started observer for I/O class {io_class}")

        # Schedule a new watcher for our observer
        log.info(f'Watching node "{node.name}" root "{node.db.root}" for auto import.')
        _watchers[node.name] = _observers[node.io_class].schedule(
            RegisterFile(node, queue), node.db.root, recursive=True
        )

        # Now catch up with the existing files to see if there are any new ones
        # that should be imported
        #
        # This happens in a worker thread.  I think that's fine.
        #
        # While this catch-up task runs, it is possible for copy requests for already
        # existing files to also be handled run.  But both the import task and the
        # pull task should be able to handle the other causing the file to appear
        # unexpectedly.
        Task(
            func=catchup,
            queue=queue,
            key=node.name,
            args=(node, queue),
            name=f"Catch-up on {node.name}",
            # If the job fails due to DB connection loss, re-start it
            requeue=True,
        )


def catchup(task: Task, node: UpdateableNode, queue: FairMultiFIFOQueue):
    """Traverse the node directory for new files and importem.

    Invoked whenever an auto-import watchdog is started to ensure there's nothing
    that's going to be missed on the node.  This runs in a worker.

    Parameters
    ----------
    task : task.Task
        The task running this function
    node : UpdateableNode
        The node we're crawling
    queue : FairMultiFIFOQueue
        The task queue
    """

    # Get set of all files that are known on the node
    already_imported_files = node.db.get_all_files(
        present=True, corrupt=True, unknown=True
    )

    log.info(f'Crawling node "{node.name}" root "{node.db.root}" for new files.')

    lastparent = None
    for file in node.io.file_walk():
        # Try to remove node root
        try:
            file = file.relative_to(node.db.root)
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


def stop_observers() -> None:
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
