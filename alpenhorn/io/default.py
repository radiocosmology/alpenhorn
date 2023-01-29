"""Alpenhorn Default I/O classes.

The Alpenhorn Default I/O classes largely re-create the legacy I/O behaviour
of previous versions of Alpenhorn.

These I/O classes are used by StorageNodes and StorageGroups which do not
explicitly specify `io_class` (as well as being used explicitly when `io_class`
has the value "Default").
"""
from __future__ import annotations
from typing import TYPE_CHECKING

import logging

from .base import BaseNodeIO, BaseGroupIO

if TYPE_CHECKING:
    from ..update import UpdateableNode

log = logging.getLogger(__name__)


class DefaultNodeIO(BaseNodeIO):
    """A simple StorageNode backed by a regular POSIX filesystem."""


class DefaultGroupIO(BaseGroupIO):
    """A simple StorageGroup.

    Permits any number of StorageNodes in the group, but only permits at most
    one to be active on a given host at any time.
    """

    # SETUP

    def set_nodes(self, nodes: list[UpdateableNode]) -> list[UpdateableNode]:
        """Set the list of nodes to operate on.

        DefaultGroupIO only accepts a single node to operate on.

        Parameters
        ----------
        nodes : list of UpdateableNodes
            The local active nodes in this group.  Will never be
            empty.

        Returns
        -------
        nodes : list of UpdateableNodes
            `nodes`, if `len(nodes) == 1`

        Raises
        ------
        ValueError
            whenever `len(nodes) != 1`
        """

        if len(nodes) != 1:
            raise ValueError(f"Too many active nodes in group {self.group.name}.")

        self.node = nodes[0]
        return nodes
