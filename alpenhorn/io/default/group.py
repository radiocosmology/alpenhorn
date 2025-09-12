"""Alpenhorn Default Group I/O class.

This is the DefaultGroupIO class, which implements a StorageGroup with only
one StorageNode.  A singleton group like this is by far the most common type
of StorageGroup.

Due to having only a single node, the implementation for this group is simple,
since most I/O is just delegated to the node in the group.
"""

from __future__ import annotations

import logging
import pathlib
from collections.abc import Hashable

from ...daemon import UpdateableNode
from ...daemon.scheduler import FairMultiFIFOQueue, Task
from ...db import (
    ArchiveFileCopyRequest,
    StorageNode,
)
from ..base import BaseGroupIO
from .check import force_check_filecopy

log = logging.getLogger(__name__)


def group_search_async(
    task: Task,
    groupio: BaseGroupIO,
    req: ArchiveFileCopyRequest,
) -> None:
    """Check group `io` for an existing, unregistered `req.file`.

    If the file is found, a request is made to check the
    existing file, and the pull is skipped.  If the file
    is not found, `req` is passed to `groupio.pull`, which
    dispatch the ArchvieFileCopyRequest to a node in the
    group to perform the pull request.

    Parameters
    ----------
    task : Task
        The task instance containing this async.
    io : Group I/O instance
        The I/O instance for the pull destination group.
    req : ArchiveFileCopyRequest
        The request we're fulfilling.
    """

    # Before doing anything re-check the DB for something
    # in this group.  The situation may have changed while this
    # task was queued.
    state = groupio.group.state_on_node(req.file)[0]
    if state == "Y" or state == "M":
        log.info(
            "Cancelling pull request for "
            f"{req.file.acq.name}/{req.file.name}: "
            f"file already in group {groupio.group.name}."
        )
        req.cancelled = 1
        req.save()
        return

    # Check whether an actual file exists on the target
    found_missing = False
    for node in groupio.nodes:
        if node.io.exists(req.file.path):
            # file on disk: is it known?
            #
            # Acreate/update the ArchiveFileCopy to force a check next pass
            copy_state = node.db.filecopy_state(req.file)

            if copy_state == "N":
                # Update/create ArchiveFileCopy to force a check.
                force_check_filecopy(req.file, node.db, node.io)
                found_missing = True

    # If we found something, warn and stop
    if found_missing:
        log.warning(
            "Skipping pull request for "
            f"{req.file.acq.name}/{req.file.name}: "
            f"file already on disk in group {groupio.group.name}."
        )
        return

    # Otherwise, escalate to groupio.pull to actually perform the pull
    groupio.pull(req, did_search=True)


class DefaultGroupIO(BaseGroupIO):
    """A simple StorageGroup.

    Permits any number of StorageNodes in the group, but only permits at most
    one to be active on a given host at any time.
    """

    # Because Default groups allow only a single node, they don't need to do
    # group-level pull searches.  So we set this to False.
    #
    # Note, however, that the DefaultGroupIO still fully implements a "normal"
    # pre-pull search to make it easier for subclasses that use this as a parent
    # to enable the group search by simply setting this back to True.
    do_pull_search = False

    # SETUP

    def __init__(
        self, node: StorageNode, config: dict, queue: FairMultiFIFOQueue, fifo: Hashable
    ) -> None:
        super().__init__(node, config, queue, fifo)
        self._node = None

    @property
    def nodes(self) -> list[UpdateableNode]:
        """The list of nodes in this group.

        This is a single element list containing the node assigned to this I/O
        instance, or the empty list if no node has been assigned."""
        if self._node:
            return [self._node]
        return []

    @nodes.setter
    def nodes(self, nodes: list[UpdateableNode]) -> None:
        """Set the node in this group.

        DefaultGroupIO only accepts a single node to operate on.

        Parameters
        ----------
        nodes : list of UpdateableNodes
            This should always be a single-element list containing the
            group's StorageNode.

        Raises
        ------
        ValueError
            whenever `len(nodes) != 1`
        """

        if len(nodes) != 1:
            # The nodes list passed in is never empty, so this message is reasonable.
            raise ValueError(f"Too many active nodes in group {self.group.name}.")

        self._node = nodes[0]

    # I/O METHODS

    def exists(self, path: pathlib.PurePath) -> UpdateableNode | None:
        """Check whether the file `path` exists in this group.

        Parameters
        ----------
        path : pathlib.PurePath
            the path, relative to node `root` of the file to
            search for.

        Returns
        -------
        node : UpdateableNode or None
            If the file exists, returns the node in the group.
            If the file doesn't exist in the group, this is None.
        """
        if self._node.io.exists(path):
            return self._node

        return None

    def pull(self, req: ArchiveFileCopyRequest, did_search: bool) -> None:
        """Handle ArchiveFileCopyRequest `req` by pulling to this group.

        Simply passes the `req` on to the node in the group.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the request to fulfill.  We are the destination group (i.e.
            `req.group_to == self.group`).
        did_search : boolean
            True if a group-level pre-pull search for an existing file was
            performed.  False otherwise.
        """
        self._node.io.pull(req, did_search)

    def pull_search(self, req: ArchiveFileCopyRequest) -> None:
        """Search for an existing copy of a file in a group.

        Before the pull is dispached to the group, we first check
        whether an existing unregistered file exists in the group.

        If there is, the file is schedule for check and the request
        is skipped.  Otherwise, `pull` will be called to actually
        pull the file.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the request to fulfill.  We are the destination group (i.e.
            `req.group_to == self.group`).

        Notes
        -----
        The DefaultGroupIO class itself sets `do_pull_search` to False
        because it's not needed by the DefaultIO, but this method is
        implemented to dispatch the search task anyways so that other
        I/O classes which derive from DefaultIO can set `do_pull_search`
        back to True and not have to re-implement this method themselves.
        """

        # The existing file search needs to happen in a Task.
        Task(
            func=group_search_async,
            queue=self._queue,
            key=self.fifo,
            args=(self, req),
            name=f"Pre-pull search for {req.file.path} in {self.group.name}",
        )
