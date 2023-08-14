"""BaseIO classes.

Provides the basic infrastructure for StorageNode and StorageGroup I/O.

These are very low-level classes.  Any module implementing the I/O class for
something even remotely resembling a POSIX filesystem may be better served
by subclassing from DefaultIO instead of from here directly.
"""
from __future__ import annotations
from typing import TYPE_CHECKING, IO

import logging
import pathlib

if TYPE_CHECKING:
    import os
    from collections.abc import Iterator
    from ..acquisition import ArchiveFile
    from ..archive import ArchiveFileCopy, ArchiveFileCopyRequest
    from ..queue import FairMultiFIFOQueue
    from ..storage import StorageNode, StorageGroup
    from ..update import UpdateableNode

log = logging.getLogger(__name__)


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
        the remote node
    config : dict
        the parsed `node.io_config`. If `node.io_config` is None,
        this is an empty `dict`.
    """

    def __init__(self, node: StorageNode, config: dict) -> None:
        self.node = node
        self.config = config

    def file_addr(self, file: ArchiveFile) -> str:
        """Return an remote file address suitable for use with rsync.

        i.e., a string of the form: <username>@<host>:<path>

        Parameters
        ----------
        file : ArchiveFile
            The file to return the address for.

        Returns
        -------
        addr : str
            The file address

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

        Parameters:
        -----------
        file : ArchiveFile
            The file to return the path for.

        Returns:
        --------
        path : str
            the remote path
        """
        return str(pathlib.PurePath(self.node.root, file.path))

    def pull_ready(self, file: ArchiveFile) -> bool:
        """Is `file` ready for pulling from this remote node?

        Parameters
        ----------
        file : ArchiveFile
            the file being checked

        Returns
        -------
        ready : bool
            True if `file` is ready on the node; False otherwise.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")


class BaseNodeIO:
    """Base class for StorageNode I/O modules in alpenhorn.

    Parameters
    ----------
    node : StorageNode
        the node
    queue : FairMultiFIFOQueue
        the task queue
    config : dict
        the parsed `node.io_config`. If `node.io_config` is None,
        this is an empty `dict`.
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
        self, node: StorageNode, config: dict, queue: FairMultiFIFOQueue
    ) -> None:
        self.node = node
        self._queue = queue
        self.config = config

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
            the updated node instance read from the database
        """
        self.node = node

    # HOOKS

    def before_update(self, idle: bool) -> bool:
        """Pre-update hook.

        Called each update loop before node updates happen.

        Parameters
        ----------
        idle : bool
            If False, updates for this node are going to be skipped this
            update loop.

        Returns
        -------
        do_update : bool
            Whether to proceed with the update or not (skip it).  If
            False, the update will be skipped.
        """
        # By default, we do nothing and allow the update to continue
        return True

    def idle_update(self) -> None:
        """Idle update hook.

        Called after a regular update that wasn't skipped, but only if,
        after the regular update, there were no tasks pending or in
        progress for this node (i.e. `self.idle` is True).

        This is the place to put low-priority tasks that should only happen
        if no other I/O is happening on the node.
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

    def auto_verify(self, copy: ArchiveFileCopy):
        """Check whether ArchiveFileCopy `copy` is corrupt.

        This is called by the auto-verification process which can be
        run during idle times on a node.

        The default behaviour is to simply pass `copy` on to `self.check`.

        Parameters
        ----------
        copy : ArchiveFileCopy
            The file copy to check
        """
        return self.check(copy)

    def bytes_avail(self, fast: bool = False) -> int | None:
        """bytes_avail: Return amount of free space (in bytes) of the node, or
        None if that cannot be determined.

        Note: this is a measure of free space on the underlying storage system,
        not how close to node.max_total_gb the value of self.size_bytes() is.
        The value returned may exceed node.max_total_gb.

        Parameters
        ----------
        fast : bool
            If True, then this is a fast call, and I/O classes for which
            checking available space is expensive may skip it by returning None.

        Returns
        -------
        bytes_avail : int or None
            the total bytes available on the storage system, or None if that
            can't be or wasn't determined.
        """
        return None

    def check(self, copy: ArchiveFileCopy) -> None:
        """Check whether ArchiveFileCopy `copy` is corrupt.

        Parameters
        ----------
        copy : ArchiveFileCopy
            the file copy to check
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def check_active(self) -> bool:
        """Check that this is an active node.

        This check should be done by inspecting the storage system, rather than
        checking the database, because is meant to catch instances where the
        "active" bit in the database is incorrect.

        Returns True if the node is active, or False if inactive.

        If this can't be determined, it should return self.node.active (i.e.
        assume the database is correct), which is the default behaviour."""
        return self.node.active

    def delete(self, copies: list[ArchiveFileCopy]) -> None:
        """Delete the ArchiveFileCopy list `copies` from the node.

        Parameters
        ----------
        copies : list of ArchiveFileCopy
            The list of copies to delete.  May be empty.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def exists(self, path: pathlib.PurePath) -> bool:
        """Does `path` exist?

        Parameters
        ----------
        path : pathlib.PurePath
            path relative to `node.root`
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def filesize(self, path: pathlib.Path, actual: bool = False) -> int:
        """Return size in bytes of the file given by `path`.

        Parameters
        ----------
        path: path-like
            The filepath to check the size of.  May be absolute or relative
            to `node.root`.
        actual: bool, optional
            If True, return the amount of space the file actually takes
            up on the storage system.  Otherwise return apparent size.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def file_walk(self) -> Iterator[pathlib.PurePath]:
        """Iterate over file copies

        Should successively yield a pathlib.PurePath for each file copy on the
        node.  The returned path may either be absolute (i.e have node.root
        pre-pended) or else be relative to node.root.  The former is preferred.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def fits(self, size_b: int) -> bool:
        """Does `size_b` bytes fit on this node?

        Parameters
        ----------
        size_b : int
            The size of the file we're trying to fit

        Returns
        -------
        fits : bool
            True if `size_b` fits on the node.  False otherwise.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def locked(self, path: os.PathLike) -> bool:
        """Is file `path` locked?

        Locked files cannot be imported.

        Parameters
        ----------
        path : path-like
            The path to check.  May be relative or absolute.

        Returns
        -------
        locked : bool
            True if `path` is locked; False otherwise.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def md5(self, path: str | pathlib.Path, *segments) -> str:
        """Compute the MD5 hash of the file at the specified path.

        Parameters
        ----------
        path : PathLike
            path (or first part of the, path if other `segments` provided) to
            the file to hash.  Relative to `node.root`.
        *segments : iterable, optional
            other path segments path-concatenated and appended to `path`.

        Returns
        -------
        md5sum : str
            the base64-encoded MD5 hash value
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def open(self, path: os.PathLike | str, binary: bool = True) -> IO:
        """Open the file specified by `path` for reading.

        Parameters:
        -----------
        path : pathlike
            Relative to `node.root`
        binary : bool, optional
            If True, open the file in binary mode, otherwise open the file in
            text mode.

        Returns
        -------
        file : file-like
            An open, read-only file.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def pull(self, req: ArchiveFileCopyRequest) -> None:
        """Pull file specified by copy request `req` onto `self.node`.

        In this case, `node` is the destination.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the copy request to fulfill.  We are the destination node (i.e.
            `req.group_to == self.node.group`).
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
        ready : bool
            True if `path` is ready for I/O.  False otherwise.

        Notes
        -----
        If this returns False, the caller may wait and then call this
        method again to try again.
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
            the copy request to ready.  We are the source node (i.e.
            `req.node_from == self.node`).
        """
        raise NotImplementedError("method must be re-implemented in subclass.")


class BaseGroupIO:
    """Base class for StorageGroup IO modules in alpenhorn.

    Parameters
    ----------
    group : StorageGroup
        The group
    config : dict
        The parsed `group.io_config`. If `group.io_config` is None,
        this is an empty `dict`.
    """

    # SETUP

    def __init__(self, group: StorageGroup, config: dict) -> None:
        self.group = group
        self.config = config

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
            the updated group instance read from the database
        """
        self.group = group

    def set_nodes(self, nodes: list[UpdateableNode]) -> list[UpdateableNode]:
        """Set the list of local active nodes in this group.

        This method is called to communicate to the I/O instance the list
        of locally-active nodes.

        This method is called each main loop, after regular
        node I/O has completed but before any group I/O updates commence.

        If group I/O cannot proceed with the supplied list of nodes,
        implementations should raise ValueError with a message which will
        be written to the log.

        Otherwise, it may choose to operate on any non-empty subset of
        `nodes`.  In this case it should return the list of nodes which has
        been selected, and record the list locally, if needed.

        Parameters
        ----------
        nodes : list of UpdateableNodes
            local active nodes in this group.  Will never be empty.

        Returns
        -------
        selected_nodes : list of UpdateableNodes
            `nodes` or a non-empty subset of `nodes` on which group I/O
            will be performed.

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
        idle : boolean
                True if all the `nodes` were idle when the current
                update loop started.

        Returns
        -------
        do_update : bool
            Whether to proceed with the update or not (skip it).  If
            False, the update will be skipped.
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

        If the file exists on more than one node in the group,
        implementations may use any method to choose which node
        to return.

        Parameters
        ----------
        path : pathlib.PurePath
            the path, relative to a node `root` of the file to
            search for.

        Returns
        -------
        node : UpdateableNode or None
            If the file exists, the node containing it.  This should be
            one of the `UpdateableNode` instances provided to `set_nodes`.
            If the file doesn't exist in the group, this is None.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def pull(self, req: ArchiveFileCopyRequest) -> None:
        """Handle ArchiveFileCopyRequest `req` by pulling to this group.

        In general, implementations should choose which node in the group
        to handle the request, and pass `req` to that node's `io.pull` method.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the request to fulfill.  We are the destination group (i.e.
            `req.group_to == self.group`).
        """
        raise NotImplementedError("method must be re-implemented in subclass.")
