"""BaseIO classes.

Provides the basic infrastructure for StorageNode and StorageGroup I/O.

These are very low-level classes.  Any module implementing the I/O class for
something even remotely resembling a POSIX filesystem may be better served
by subclassing from DefaultIO instead of from here directly.
"""

from __future__ import annotations

import logging
import os
import pathlib
from collections import namedtuple
from collections.abc import Hashable, Iterable
from typing import IO

from ..daemon import UpdateableNode
from ..daemon.scheduler import FairMultiFIFOQueue
from ..db import (
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
)

log = logging.getLogger(__name__)

# This named tuple stands in for IOClassExtension for internal I/O classes
InternalIO = namedtuple("InternalIO", ["full_name", "node_class", "group_class"])


# Comment from DVW:
#
# Separating BaseNodeIO and BaseNodeRemote is primarily to avoid the temptaiton
# of accidentally writing code that tries to perform I/O operations on non-local
# nodes.
class BaseNodeRemote:
    """Base I/O class for a remote StorageNode.

    The remote I/O modules provide read-only information about a possibly
    non-local StorageNode.

    Parameters
    ----------
    node : StorageNode
        The remote node.
    config : dict
        The parsed `node.io_config`. If `node.io_config` is None,
        this is an empty `dict`.
    """

    def __init__(self, node: StorageNode, config: dict) -> None:
        self.node = node
        self.config = config

    def file_addr(self, file: ArchiveFile) -> str:
        """Return an remote file address suitable for use with rsync.

        Typicall, this is a string of the form: ``<username>@<host>:<path>``.

        Parameters
        ----------
        file : ArchiveFile
            The file to return the address for.

        Returns
        -------
        str
            The remote file address.

        Raises
        ------
        ValueError
            `node.username` or `node.address` were not set.
        """
        if self.node.username is None:
            raise ValueError("missing username")
        if self.node.address is None:
            raise ValueError("missing address")

        return f"{self.node.username}@{self.node.address}:{self.file_path(file)}"

    def file_path(self, file: ArchiveFile) -> str:
        """Return a path on the remote node pointing to ArchiveFile `file`.

        By default, returns the path contcatenation of `node.root` and
        `file.path`.

        Parameters
        ----------
        file : ArchiveFile
            The file to return the path for.

        Returns
        -------
        str
            The remote path.
        """
        return str(pathlib.PurePath(self.node.root, file.path))

    def pull_ready(self, file: ArchiveFile) -> bool:
        """Check if `file` ready for pulling from this node.

        Parameters
        ----------
        file : ArchiveFile
            The file to check.

        Returns
        -------
        bool
            ``True`` if `file` is ready on the node; ``False`` otherwise.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def remote_pull_ok(self, host: str) -> bool:
        """Check if a remote pull to `host` is possible.

        Returns True if the daemon on `host` may attempt a remote pull
        out of this node, or False if it cannot.

        Parameters
        ----------
        host : str
            The host on which the pulling daemon is running.

        Returns
        -------
        bool
            True if a remote pull should be attempted, or False
            if a remote pull should be cancelled.
        """
        # By default, we return True
        return True


class BaseNodeIO:
    """Base class for StorageNode I/O modules in alpenhorn.

    Parameters
    ----------
    node : StorageNode
        The node to perform I/O on.
    config : dict
        The JSON-decoded `node.io_config` dict. If `node.io_config` is None,
        this is an empty `dict`.
    queue : FairMultiFIFOQueue
        The task queue.
    fifo : Hashable
        The queue fifo key to use when submitting tasks.
    """

    # SETUP

    # Subclasses should set this to a BaseNodeRemote-derived class.
    remote_class = BaseNodeRemote

    # A class compatible with watchdog.observers.api.BaseObserver which will
    # be used as the auto import observer.
    #
    # This can be set to None if no observation is possible, though the
    # platform-independent watchdog.observers.polling.PollingObserver
    # will work in the vast majority of cases, if others do not.
    observer = None

    def __init__(
        self, node: StorageNode, config: dict, queue: FairMultiFIFOQueue, fifo: Hashable
    ) -> None:
        self.node = node
        self._queue = queue
        self.config = config
        self.fifo = fifo

    def set_storage(self, node: StorageNode) -> None:
        """Update the cached StorageNode instance.

        Called once per update loop on pre-existing I/O instances to
        replace their `self.node` with a new instance fetched from
        the database.  The new `node` instance reflects changes made
        to the database record outside of alpenhornd.

        None of `node.id`, `node.name`, `node.io_class`, or `node.io_config`
        are different than the old `self.node`'s values.  (Changes in these
        attributes cause alpenhorn to re-create the I/O instance instead of
        calling this method.)

        If called, this method is called before the `before_update` hook,
        but it is not called during the main loop iteration that creates
        the I/O instance.

        Parameters
        ----------
        node : StorageNode
            The updated node instance read from the database.
        """
        self.node = node

    # HOOKS

    def before_update(self, idle: bool) -> bool:
        """Pre-update hook.

        Called each update loop before node updates happen.  The I/O
        framework can use this hook to indicate to the daemon whether the
        update can occur this time through the loop.  Any pre-update work
        can also be performed at this time.

        Parameters
        ----------
        idle : bool
            If ``False``, updates for this node are going to be skipped this
            update loop, regardless of what this method returns, because the
            node is not idle.

        Returns
        -------
        bool
            ``True`` if the daemon should perform the update, or ``False``
            if the current update should be skipped.
        """
        # By default, we do nothing and allow the update to continue
        return True

    def idle_update(self, newly_idle: bool) -> None:
        """Idle update hook.

        Called after a regular update that wasn't skipped, but only if,
        after the regular update, there were no tasks pending or in
        progress for this node (i.e. `self.idle` is True).

        This is the place to put low-priority tasks that should only happen
        if no other I/O is happening on the node.

        Parameters
        ----------
        newly_idle : bool
            True if this is the first time idle_update has been called since
            some I/O happened.
        """
        # By default do nothing.
        pass

    def after_update(self) -> None:
        """Post-update hook.  Called at the end of the update loop.

        This method is called once per update loop, after all other processing
        has happened on the node.
        """
        # Do nothing
        pass

    # I/O METHODS

    def bytes_avail(self, fast: bool = False) -> int | None:
        """Report the amount of free space on the node.

        This is a measure of free space on the underlying storage system,
        not how close to `node.max_total_gb` the value of `self.size_bytes()` is.
        The value returned may exceed `node.max_total_gb`.

        Parameters
        ----------
        fast : bool
            If ``True``, then this is a fast call, and I/O classes for which
            checking available space is expensive may skip it by returning
            ``None``.

        Returns
        -------
        int or None
            The total bytes available on the storage system, or ``None`` if that
            can't be or wasn't determined.
        """
        return None

    def check(self, copy: ArchiveFileCopy) -> None:
        """Check ArchiveFileCopy `copy` for corruption.

        Typically this will involve MD5 hashing the file and comparing
        it to the hash stored in the `ArchiveFileCopy`, though some I/O
        implementations may have other, more efficient ways, to detect
        corruption.

        Parameters
        ----------
        copy : ArchiveFileCopy
            The file copy to check.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def check_init(self) -> bool:
        """Check that this node is initialised.

        This check should be done by inspecting the storage system, rather than
        checking the database.

        Returns
        -------
        bool
            ``True`` if the node is initialised, or ``False`` if not.
        """
        # By default, nothing is ever initialised.
        return False

    def delete(self, copies: list[ArchiveFileCopy]) -> None:
        """Delete the ArchiveFileCopy list `copies` from the node.

        Parameters
        ----------
        copies : list of ArchiveFileCopy
            The list of copies to delete.  May be empty.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def exists(self, path: pathlib.PurePath) -> bool:
        """Check if a file at `path` exists.

        Parameters
        ----------
        path : pathlib.PurePath
            The path relative to `node.root`.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def filesize(self, path: pathlib.Path) -> int:
        """Return size in bytes of the file given by `path`.

        This should be the actual size of the file, not the amount of space
        it takes up in the storage system.  For the latter, see the
        `storage_used` method.

        Parameters
        ----------
        path : path-like
            The filepath to check the size of.  May be absolute or relative
            to `node.root`.

        Returns
        -------
        int
            The size, in bytes, of the file
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def storage_used(self, path: pathlib.Path) -> int | None:
        """Return amount of storage space used by the file given by `path`.

        In general, the value returned here should be roughly the amount
        `bytes_avail` would increase by if this file were to be removed (though
        there's no assumption that this would be exact).

        For a normal filesystem, this is the number of blocks used by the file
        times the size of a filesystem block.  If this can't be determined, or is
        not a reasonable thing to compute for a given node, this may be None.

        Parameters
        ----------
        path: path-like
            The filepath to check the size of.  May be absolute or relative
            to `node.root`.

        Returns
        -------
        int or None
            The amount of space, in bytes, taken up by the file, or None if no
            such value can be provided.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def file_walk(self, path: pathlib.Path) -> Iterable[pathlib.PurePath]:
        """Iterate through directory `path`.

        This method should successively yield a pathlib.PurePath for each file
        under `path`, which is relative to the node `root.

        Parameters
        ----------
        path : path-like
            The directory path to iterate through.

        Returns
        -------
        Iterable
            The caller should iterate over the returned value to retrieve
            successive paths.   The paths returned may either be absolute (i.er
            have `node.root` pre-pended) or else be relative to node.root.
            The former is preferred.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def fits(self, size_b: int) -> bool:
        """Check if `size_b` bytes fits on this node.

        Parameters
        ----------
        size_b : int
            The number of bytes we're trying to fit.

        Returns
        -------
        bool
            ``True`` if `size_b` fits on the node.  ``False`` otherwise.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def init(self) -> bool:
        """Initialise this node.

        This method will only be called if `check_init` returns False.
        If initialisation is successful, subsequent `check_init` calls should
        return ``True``.

        Returns
        -------
        bool
            ``True`` if initialisation succeed, or if the node was already
            initialised.  ``False`` if initialisation failed.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def locked(self, path: os.PathLike) -> bool:
        """Check if the file at `path` is locked.

        Locked files cannot be imported.

        Parameters
        ----------
        path : path-like
            The path to check.  May be relative or absolute.

        Returns
        -------
        bool
            ``True`` if `path` is locked; ``False`` otherwise.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def md5(self, path: str | pathlib.Path, *segments) -> str | None:
        """Compute the MD5 hash of the file at the specified path.

        Parameters
        ----------
        path : PathLike
            The path (or first part of the, path if other `segments` provided) to
            the file to hash.  Relative to `node.root`.
        *segments : iterable, optional
            Other path segments path-concatenated and appended to `path`.

        Returns
        -------
        str or None
            The base64-encoded MD5 hash value, or None if the hash couldn't be
            computed.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def open(self, path: os.PathLike | str, binary: bool = True) -> IO:
        """Open the file specified by `path` for reading.

        Parameters
        ----------
        path : pathlike
            The Path to open.  Relative to `node.root`.
        binary : bool, optional
            If ``True`` (the default), open the file in binary mode; otherwise,
            open the file in text mode.

        Returns
        -------
        IO
            The open, read-only file.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def pull(self, req: ArchiveFileCopyRequest, did_search: bool) -> None:
        """Pull file specified by copy request `req` onto `self.node`.

        In this case, `node` is the destination.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            The copy request to fulfill.  We are in the destination group
            (i.e. `req.group_to == self.node.group`).
        did_search : bool
            ``True`` if a group-level pre-pull search for an existing file was
            performed.  ``False`` otherwise.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def ready_path(self, path: os.PathLike) -> bool:
        """Ready a file at `path` for I/O.

        Implementations may assume `path` exists, but are not required to.

        Parameters
        ----------
        path : path-like
            The path that we want to perform I/O on.

        Returns
        -------
        bool
            ``True`` if `path` is ready for I/O.  ``False`` otherwise.
            If ``False``, the caller may wait and then call this method again
            to try again.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def ready_pull(self, req: ArchiveFileCopyRequest) -> None:
        """Ready a file to be pulled as specified by `req`.

        This method is called for all pending requests, even ones that are
        impossible due to the file being corrupt, missing, or some other calamity.

        If such an impossibility arises, this method _may_ cancel the request,
        but that's not required.  (It's the responsibility of the pulling
        alpenhornd to resolve the request.)

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            The copy request to ready.  We are the source node (i.e.
            ``req.node_from == self.node``).
        """
        raise NotImplementedError("method must be re-implemented in subclass.")


class BaseGroupIO:
    """Base class for StorageGroup IO modules in alpenhorn.

    Parameters
    ----------
    group : StorageGroup
        The group.
    config : dict
        The parsed `group.io_config`. If `group.io_config` is None,
        this is an empty `dict`.
    queue : FairMultiFIFOQueue
        The task queue.
    fifo : Hashable
        The queue fifo key to use when submitting tasks.
    """

    # SETUP

    # If True, a "Pre-pull search" job should be run on the group before
    # delegating a pull to a node.  If False, no such job is run, and the
    # pulling node should probably do one instead.
    do_pull_search = True

    def __init__(
        self,
        group: StorageGroup,
        config: dict,
        queue: FairMultiFIFOQueue,
        fifo: Hashable,
    ) -> None:
        self.group = group
        self._queue = queue
        self.config = config
        self.fifo = fifo

    def set_storage(self, group: StorageGroup) -> None:
        """Update the cached StorageGroup instance.

        Called once per update loop on pre-existing I/O instances to
        replace their `self.group` with a new instance fetched from
        the database.  The new `group` instance reflects changes made
        to the database record outside of alpenhornd.

        None of `group.id`, `group.name`, `group.io_class`, or `group.io_config`
        are different than the old `self.group`'s values.  (Changes in these
        attributes cause alpenhorn to re-create the I/O instance instead of
        calling this method.)

        If called, this method is called before the `before_update` hook,
        but it is not called during the main loop iteration that creates
        the I/O instance.

        Parameters
        ----------
        group : StorageGroup
            The new `StorageGroup` instance read from the database.
        """
        self.group = group

    @property
    def nodes(self) -> list[UpdateableNode]:  # numpydoc ignore=RT01
        """The list of nodes in this group.

        Ordering is important: nodes at the start of the list are more likely
        to have I/O performed against them.

        If this group has no nodes, perhaps because it rejected all nodes it was
        offered by the daemon, this should be the empty list.

        The daemon will attempt to assign to this property the list of
        locally-active nodes.  This happens each time through the main loop,
        after regular node I/O has completed but before any group I/O
        updates commence.

        If group I/O cannot proceed with the supplied list of nodes,
        the setter should raise `ValueError` with a message which will
        be written to the log.

        Otherwise, the setter may choose to operate on any non-empty subset of
        the nodes it was provided.  In this case, this `nodes` property should later
        provide the list of nodes which were selected.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    @nodes.setter
    def nodes(self, nodes: list[UpdateableNode]) -> list[UpdateableNode]:
        """Setter for `nodes` (q.v.).

        Parameters
        ----------
        nodes : list of UpdateableNodes
            Local active nodes in this group.  Will never be empty.

        Raises
        ------
        ValueError
            `nodes` was not sufficient to permit group I/O to proceed.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    # HOOKS

    def before_update(self, idle: bool) -> bool:
        """Pre-update hook.

        This method is called once per update loop, before any other
        processing happens on this group.

        Parameters
        ----------
        idle : bool
                ``True`` if all the `nodes` were idle when the current
                update loop started.

        Returns
        -------
        bool
            ``True`` if the daemon should continue with the update.  If
            ``False``, the update will be skipped.
        """
        # By default, we do nothing and allow the update to continue
        return True

    def idle_update(self) -> None:
        """Idle update hook.

        Called after a regular update that wasn't skipped, but only if after
        the regular update, there were no tasks pending or in progress this
        group (i.e. self.idle is True).

        This is the place to put low-priority tasks that should only happen
        if no other I/O is happening on the group.
        """
        # By default do nothing.
        pass

    def after_update(self) -> None:
        """Post-update hook.

        This method is called once per update loop, after all other processing
        has happened on the group but before the node `after_update` hooks
        are called.
        """
        # Do nothing
        pass

    # I/O METHODS

    def exists(self, path: pathlib.PurePath) -> UpdateableNode | None:
        """Check whether the file `path` exists in this group.

        If the file exists on more than one node in the group, implementations
        may use any method to choose which node to return.

        Parameters
        ----------
        path : pathlib.PurePath
            The path, relative to a node root of the file to search for.

        Returns
        -------
        UpdateableNode or None
            If the file exists, this should be the node containing it.
            The value returned should be one of the `UpdateableNode` elements
            in the list returned by the `nodes` attribute.
            If the file doesn't exist in the group, this should be ``None``.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def pull(self, req: ArchiveFileCopyRequest, did_search: bool) -> None:
        """Handle `ArchiveFileCopyRequest` `req` by pulling into this group.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            The request to fulfill.  We are the destination group (i.e.
            ``req.group_to == self.group``).
        did_search : bool
            ``True`` if a group-level pre-pull search for an existing file was
            performed.  ``False`` otherwise.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def pull_search(self, req: ArchiveFileCopyRequest) -> None:
        """Search for an existing copy of a file in a group.

        This method is only called if the attribute `do_pull_search` is ``True``.
        This provides the group an opportunity to search the group for an exising
        unregistered copy of a file which needs to be pulled into this group.

        If a search is performed and a file is found, this method should cancel
        the request, and instead request import of the existing file copy (by
        creating/updating a corresponding `ArchiveFileCopy` record).

        If a search is performed and no file is found, or this method decides
        to skip the search, this method should end with a call to the `pull`
        method to actually perform the pull (setting `did_search` appropriately).

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            The request to fulfill.  We are the destination group (i.e.
            ``req.group_to == self.group``).
        """
        raise NotImplementedError("method must be re-implemented in subclass.")
