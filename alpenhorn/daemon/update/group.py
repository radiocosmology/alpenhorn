"""UpdateableGroup.

This is an active StorageGroup which is being managed by alpenhornd."""

from __future__ import annotations

import logging

from ...db import ArchiveFileCopyRequest, StorageGroup
from ..metrics import Metric
from ..scheduler import FairMultiFIFOQueue
from ._base import UpdateableBase
from .node import RemoteNode, UpdateableNode

log = logging.getLogger(__name__)


class UpdateableGroup(UpdateableBase):
    """Updateable Group

    This is a container class which combines a StorageGroup
    and its I/O class, and implements the update logic
    for the group.

    Parameters
    ----------
    queue : FairMultiFIFOQueue
        The task queue/scheduler
    group : StorageGroup
        The underlying StorageGroup instance
    nodes : list of UpdateableNodes
        The nodes active on this host in this group
    idle : bool
        Were all nodes in `nodes` idle at the start of the
        current update loop?
    """

    is_group = True

    def __init__(
        self,
        *,
        queue: FairMultiFIFOQueue,
        group: StorageGroup,
        nodes: list[UpdateableNode],
        idle: bool,
    ) -> None:
        # Set in reinit()
        self.db = None
        self._do_idle_updates = False

        self._queue = queue
        self._fifo = None

        self.reinit(group=group, nodes=nodes, idle=idle)

        # Metrics that we want to delete when this node goes away
        self._idle_metric = Metric(
            "group_idle", "Group is idle", bound={"name": self.name}
        )

    def __del__(self):
        # Delete metrics
        try:
            self._idle_metric.remove()
        except (KeyError, AttributeError):
            # AttributeError will be raised if this instance wasn't fully initialised
            # KeyError will be raised if the prom metric was never created (because
            #   we never set the metric to anything)
            pass

        super().__del__()

    def reinit(
        self, *, group: StorageGroup, nodes: list[UpdateableNode], idle: bool
    ) -> None:
        """Re-initialise the UpdateableGroup.

        Called once per update loop.

        Parameters
        ----------
        group : StorageGroup
            The newly-fetched StorageGroup instance
        nodes : list of UpdateableNodes
            The nodes active on this host in this group
        idle : bool
            Were all nodes in `nodes` idle at the start of the
            current update loop?
        """
        self._init_idle = idle

        # Takes care of I/O re-init
        super().reinit(group)

        try:
            self.io.nodes = nodes
        except ValueError as e:
            # I/O layer didn't like the nodes we gave it
            log.warning(str(e))

    @property
    def idle(self) -> bool:
        """Is this group idle?

        False whenever any consitiuent node is not idle."""

        # If the group fifo isn't empty, the group is not idle.
        if self._queue.fifo_size(self.io.fifo):
            return False

        # A group with no nodes is not idle.
        if not self.io.nodes:
            return False

        # If any node is not idle, the group is not idle.
        for node in self.io.nodes:
            if not node.idle:
                return False

        return True

    def update_pull(self, req: ArchiveFileCopyRequest) -> None:
        """Process pull request `req`.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            The pull request to process.
        """
        from ..main import host

        # Run early checks on the request
        if not req.check():
            return

        # If this is a non-local pull, check that the remote source node
        # support remote access.  This is not done in `req.check` because
        # req.check doesn't know about RemoteNode.
        if not req.node_from.local:
            remote = RemoteNode(req.node_from)
            if not remote.io.remote_pull_ok(host()):
                req.cancel("non-local")
                return

        # Early checks passed: dispatch this request to the Group I/O layer
        if self.io.do_pull_search:
            self.io.pull_search(req)
        else:
            self.io.pull(req, did_search=False)

    def update(self) -> None:
        """Perform I/O updates on the group"""

        self._do_idle_updates = False

        # If the available nodes weren't acceptable to the I/O layer, do nothing
        if not self.io.nodes:
            return

        # Call the before update hook
        do_update = self.io.before_update(self._init_idle)

        # Update only happens if the queue is empty and the I/O layer hasn't
        # cancelled the update
        if self._init_idle and do_update:
            log.info(f'Updating group "{self.name}".')
            Metric(
                "group_update",
                "Count of updates on a group",
                counter=True,
                bound={"name": self.name},
            ).inc()

            # Remember ArchiveFiles that we're pulling, so we don't end up with
            # overlapping pulls (which would try to write to the same file).
            seen_files = set()

            # Process pulls into this group
            for req in ArchiveFileCopyRequest.select().where(
                ArchiveFileCopyRequest.completed == 0,
                ArchiveFileCopyRequest.cancelled == 0,
                ArchiveFileCopyRequest.group_to == self.db,
            ):
                if req.file not in seen_files:
                    seen_files.add(req.file)
                    self.update_pull(req)

            # Check for idleness at the end
            self._do_idle_updates = self.idle
        else:
            log.info(
                f"Skipping update for group {self.name}: "
                + ("busy" if not self._init_idle else "cancelled")
            )

    def update_idle(self) -> None:
        """Perform idle updates, if appropriate.

        The idle updates are run if the regular update() ran but
        the group was idle when it finished.
        """
        self._idle_metric.set(1 if self._do_idle_updates else 0)
        if self._do_idle_updates:
            self.io.idle_update()
