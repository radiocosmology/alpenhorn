"""BaseIO classes.

Provides the basic infrastructure for StorageNode and StorageGroup I/O.

These are very low-level classes.  Any module implementing the I/O class for
something even remotely resembling a POSIX filesystem may be better served
by subclassing from DefaultIO instead of from here directly.
"""
from __future__ import annotations
from typing import TYPE_CHECKING

import logging

if TYPE_CHECKING:
    from ..queue import FairMultiFIFOQueue
    from ..storage import StorageNode, StorageGroup
    from ..update import UpdateableNode

log = logging.getLogger(__name__)


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

    def __init__(
        self, node: StorageNode, config: dict, queue: FairMultiFIFOQueue
    ) -> None:
        self.node = node
        self._queue = queue
        self.config = config

    def update(self, node: StorageNode) -> None:
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
                If False, the group update is going to be skipped.

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
