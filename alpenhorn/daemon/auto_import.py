"""Routines for the importing of files on a node.

This module should probably be called "import", because it's
not just used for auto-importing, but that's a difficult name
for a module.
"""

from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

import peewee as pw
from watchdog.events import FileSystemEventHandler

from ..common import config, extensions, metrics
from ..common.util import invalid_import_path
from ..db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileImportRequest,
    utcnow,
)
from ..io import ioutil
from ..scheduler import Task

if TYPE_CHECKING:
    import os
    from collections.abc import Generator

    from ..scheduler import FairMultiFIFOQueue
    from .update import UpdateableNode
del TYPE_CHECKING


log = logging.getLogger(__name__)


def import_request_done(req: ArchiveFileImportRequest | None, result: str) -> None:
    """Record a completed import request.

    Including updating metrics.

    Parameters
    ----------
    req:
        Request that has been completed.  If None, this function does nothing.
    result:
        The result of the import request.  Used in the metric
    """

    if not req:
        return

    count = (
        ArchiveFileImportRequest.update(completed=1)
        .where(ArchiveFileImportRequest.id == req.id)
        .execute()
    )

    # Only update metrics if this actually completed the request
    if not count:
        return

    log.info(f"Completed import request #{req.id}.")
    metrics.by_name("requests_completed").inc(
        type="import",
        result=result,
        node=req.node.name,
        group=req.node.group.name,
    )


def import_file(
    node: UpdateableNode,
    queue: FairMultiFIFOQueue,
    path: pathlib.PurePath,
    register: bool,
    req: ArchiveFileImportRequest | None,
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
    register : bool
        True if we should register new files (files without ArchiveFile records),
        or False to only import already registered files.
    req : ArchiveFileImportRequest or None
        This will be None when the import request was triggered by the auto-import
        watchdog.  Otherwise, this is the import request which is being handled.
        If not None, req will be marked as complete if the import isn't skipped.
    """

    path = pathlib.PurePath(path)

    # Occasionally the watchdog sends events on the node root directory itself.
    # Skip these.
    if path == pathlib.PurePath(node.db.root):
        log.debug("Skipping import request of node root")
        import_request_done(req, "ignored")
        return

    # Strip node root, if path is absolute
    if path.is_absolute():
        try:
            path = path.relative_to(node.db.root)
        except ValueError:
            # Not rooted under node.root
            log.warning(
                f"Ignoring import of {path}: not rooted under node {node.db.root}"
            )
            import_request_done(req, "ignored")
            return

    # Ignore the node info file.  NB: this happens even on nodes which
    # don't use an ALPENHORN_NODE file, meaning even on those nodes,
    # this path is disallowed as a data file.
    if pathlib.PurePath(node.db.root).joinpath(path) == pathlib.PurePath(
        node.db.root
    ).joinpath("ALPENHORN_NODE"):
        log.debug("ignoring ALPENHORN_NODE file during import")
        import_request_done(req, "ignored")
        return

    # New import task.
    Task(
        func=_import_file,
        queue=queue,
        key=node.io.fifo,
        args=(node, path, register, req),
        name=f"Import {path} on {node.name}",
        # If the job fails due to DB connection loss, and we don't have a request,
        # re-start the task because unlike tasks made in the main loop, we're
        # never going to revisit this.
        requeue=(req is None),
    )


def _import_file(
    task: Task,
    node: UpdateableNode,
    path: pathlib.PurePath,
    register: bool,
    req: ArchiveFileImportRequest | None,
) -> Generator[int]:
    """Import `path` on `node` into the DB.  This is run by a worker.

    Parameters
    ----------
    task : task.Task
        The task running this function
    node : UpdateableNode
        The node we are processing.
    path : pathlib.PurePath
        The path should be relative to `node.root`
    register : bool
        True if we should register new files (files without ArchiveFile records),
        or False to only import already registered files.
    req : ArchiveFileImportRequest or None
        If not None, req will be marked as complete if the import isn't skipped.
    """

    # Skip non-files
    fullpath = pathlib.Path(node.db.root).joinpath(path)
    if fullpath.is_symlink() or not fullpath.is_file():
        log.info(f'Not importing "{path}": not a file.')
        import_request_done(req, "invalid")
        return

    log.debug(f'Considering "{path}" for import to node {node.name}.')

    # Skip files with a leading dot
    if path.name[0] == ".":
        log.info(f'Not importing "{path}": filename starts with a dot.')
        import_request_done(req, "bad_name")
        return

    # Wait for file to become ready
    while not node.io.ready_path(path):
        log.info(
            f'Path "{path}" not ready for I/O during import.  Waiting 600 seconds.'
        )
        yield 600  # wait 600 seconds and re-queue

    # Skip the file if there is still a lock on it.
    if node.io.locked(path):
        log.info(f'Skipping "{path}": locked.')
        # In this case we don't complete the import request.
        return

    # Step through the detection extensions to find one that's willing
    # to handle this file
    for detector in extensions.import_detection():
        acq_name, callback = detector(path, node)
        if acq_name is not None:
            break
    else:
        # Detection failed, so we're done.
        log.info(f"Not importing non-acquisition path: {path}")
        import_request_done(req, "no_detection")
        return

    # Vet acq_name from extension
    rejection_reason = invalid_import_path(str(acq_name))
    if rejection_reason:
        log.warning(f'Rejecting invalid acq path "{acq_name}": {rejection_reason}')
        import_request_done(req, "bad_acq")
        return

    file_name = path.relative_to(acq_name)

    # If a copy already exists, we're done
    if node.db.named_copy_tracked(acq_name, file_name):
        log.debug(f"Not importing {path}: already known")
        import_request_done(req, "duplicate")
        return

    # Add the acqusition, if necessary
    try:
        acq = ArchiveAcq.get(ArchiveAcq.name == acq_name)
        new_acq = None
        log.debug(f'Acquisition "{acq_name}" already in DB.')
    except pw.DoesNotExist:
        if register:
            try:
                acq = ArchiveAcq.create(name=acq_name)
                new_acq = acq
                log.info(f'Acquisition "{acq_name}" added to DB.')
            except pw.IntegrityError:
                # i.e. record was created by someone else between the first get and
                #      the subsequent create
                acq = ArchiveAcq.get(ArchiveAcq.name == acq_name)
                new_acq = None
                log.debug(f'Acquisition "{acq_name}" already in DB.')
        else:
            log.info(f'Not importing unregistered acquistion: "{acq_name}".')
            import_request_done(req, "unregistered")
            return

    # Add the file, if necessary.
    try:
        file_ = ArchiveFile.get(ArchiveFile.name == file_name, ArchiveFile.acq == acq)
        new_file = None
        log.debug(f'File "{path}" already in DB.')
    except pw.DoesNotExist:
        if register:
            log.debug(f'Computing md5sum of "{path}".')
            md5sum = node.io.md5(acq_name, file_name)
            size_b = node.io.filesize(path)

            try:
                file_ = ArchiveFile.create(
                    acq=acq, name=file_name, size_b=size_b, md5sum=md5sum
                )
                new_file = file_
                log.info(f'File "{path}" added to DB.')
            except pw.IntegrityError:
                # i.e. record was created by someone else between the first get and
                #      the subsequent create
                #
                # We _cannot_ assume that this is due to another worker in _this_
                # daemon: there may be an import for the same file happening at the
                # same time on another host, as unlikely as that seems.
                file_ = ArchiveFile.get(
                    ArchiveFile.name == file_name, ArchiveFile.acq == acq
                )
                new_file = None
                log.debug(f'File "{path}" already in DB.')
        else:
            log.info(f'Not importing unregistered file: "{path}".')
            import_request_done(req, "unregistered")
            return

    try:
        copy = ArchiveFileCopy.get(file=file_, node=node.db)
        # If we're importing a file that's missing (has_file == N but
        # wants_file == Y), set has_file='M' to trigger a integrity check.
        # If it's recorded as having been properly removed, though, just
        # set it to 'Y' and assume it's good now.
        if copy.wants_file == "Y":
            copy.has_file = "M"
            log.warning(
                f'Imported missing file "{path}" on node {node.name}.  Marking suspect.'
            )
        else:
            copy.has_file = "Y"
            copy.wants_file = "Y"
            log.info(f'Imported file copy "{path}" on node "{node.name}".')
        copy.ready = True
        copy.last_update = utcnow()
        copy.save()
    except pw.DoesNotExist:
        # No existing file copy; create a new one.
        try:
            copy = ArchiveFileCopy.create(
                file=file_,
                node=node.db,
                has_file="Y",
                wants_file="Y",
                ready=True,
                size_b=node.io.filesize(path, actual=True),
                last_update=utcnow(),
            )
            log.info(f'Imported file copy "{path}" on node "{node.name}".')
        except pw.IntegrityError:
            log.debug("ArchiveFileCopy created by another worker!")
            # The ArchiveFileCopy record has been created by someone else
            # between our initial .get() and the subsequent .create().
            #
            # In this case, we assume another worker from _this_ daemon
            # has just imported the file, likely due to multiple idential
            # import requests, so just mark the request we're working on as
            # completed and let the other worker deal with fixing up the
            # copy and doing all the post-import stuff
            import_request_done(req, "duplicate")
            return

    import_request_done(req, "success")

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

    def _is_dotfile(self, path):
        """Returns True if the filename in path starts with a '.'."""
        basename = pathlib.PurePath(path).name
        return basename[0] == "."

    def _is_lock_file(self, path):
        """Returns True if path is a lock file."""
        return path[-5:] == ".lock" and self._is_dotfile(path)

    def __init__(self, node: UpdateableNode, queue: FairMultiFIFOQueue) -> None:
        self.node = node
        self.queue = queue
        super().__init__()

    def on_created(self, event):
        if not event.is_directory and not self._is_dotfile(event.src_path):
            import_file(
                self.node, self.queue, pathlib.PurePath(event.src_path), True, None
            )
        return

    def on_moved(self, event):
        if not event.is_directory and not self._is_dotfile(event.dest_path):
            import_file(
                self.node, self.queue, pathlib.PurePath(event.dest_path), True, None
            )
        return

    def on_deleted(self, event):
        # For lockfiles: ensure that the file that was locked is added: it is
        # possible that the watchdog notices that a file has been closed before the
        # lockfile is deleted.
        if not event.is_directory and self._is_lock_file(event.src_path):
            path = pathlib.Path(event.src_path)
            import_file(
                self.node, self.queue, path.with_name(path.name[1:-5]), True, None
            )


# Routines to control the filesystem watchdogs.
# =============================================

# Observer threads.  One per node I/O class.
_observers = {}

# Event watchers.  One per watched node.
_watchers = {}


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
                timeout=config.config["daemon"]["auto_import_interval"]
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
            func=scan,
            queue=queue,
            key=node.io.fifo,
            args=(node, queue, ".", True, None),
            name=f"Catch-up on {node.name}",
            # If the job fails due to DB connection loss, re-start it
            requeue=True,
        )


def scan(
    task: Task,
    node: UpdateableNode,
    queue: FairMultiFIFOQueue,
    path: str | os.PathLike,
    register: bool,
    req: ArchiveFileImportRequest | None,
) -> None:
    """Traverse a directory on a node looking for new files and importem.

    Task invoked to handle ArchiveFileImportRequest and whenever an auto-import
    watchdog is started to ensure there's nothing that's going to be missed
    on the node.

    Attempts to import individual files will be done via further jobs added to
    the node's task queue.

    Parameters
    ----------
    task : task.Task
        The task containing this job
    node : UpdateableNode
        The node we're scanning
    queue : FairMultiFIFOQueue
        The task queue
    path : path-like
        The path to scan.  Relative to `node.root`.
    register : bool
        If True, register new files.  If False, only import files with existing
        ArchiveFile records.
    req : ArchiveFileImportRequest or None
        If not None, req will be marked as complete once the scan finishes.
    """

    path = pathlib.PurePath(path)

    # Get set of all files that are known on the node
    already_imported_files = node.db.get_all_files(
        present=True, corrupt=True, unknown=True
    )

    log.info(f'Scanning "{path}" on "{node.name}" for new files.')

    lastparent = None
    for file in node.io.file_walk(path):
        # Try to remove node root
        try:
            file = file.relative_to(node.db.root)
        except ValueError:
            pass

        # Print directory as we pass through them
        parent = file.parent
        if parent != lastparent:
            log.info(f'Scanning "{parent}".')
            lastparent = parent

        # Skip files already imported
        if file in already_imported_files:
            log.debug(f'Skipping already-registered file "{file}".')
        else:
            import_file(node, queue, file, register, None)

    # This is successful because we've successfully scanned the
    # tree, whether or not that resulted in any imports.
    import_request_done(req, "success")


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
    _observers = {}
    _watchers = {}
