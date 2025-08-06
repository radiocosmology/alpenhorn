"""Transport Group I/O."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .default import DefaultGroupIO

if TYPE_CHECKING:
    import pathlib

    from ..archive import ArchiveFileCopyRequest
    from ..update import UpdateableNode
del TYPE_CHECKING

log = logging.getLogger(__name__)


class TransportGroupIO(DefaultGroupIO):
    """Transport Group I/O.

    This implements (the formerly special-cased) transport disk logic.

    A Transport StorageGroup is used to transfer data onto transiting
    storage.

    Features of a Transport StorageGroup:
        - it may have any number of nodes.  All node must have
            node.db.storage_type == 'T', but no restrictions are put on the
            io_class of nodes
        - all pulls to the StorageGroup must be local: non-local pull
            requests will be ignored
        - when handling pull requests, transport nodes are prioritised by
            increasing free space: the group will attempt to pull a file
            to the fullest node that it thinks it will fit on.
    """

    # Enable the group-level pre-pull search
    do_pull_search = True

    @property
    def nodes(self) -> list[UpdateableNode]:
        """List of nodes in this group.

        Sorted in priority fill order."""

        # Sort nodes by fullness
        def _node_key(node):
            """Sort key function for Transport nodes.

            Returns `node.db.avail_gb`, if that's numeric, or else a very
            large float if it's `None`."""
            n = node.db.avail_gb
            if n is not None:
                return n

            # Using node.db.id here makes ordering stable.  1e9 GB = 1EB, so we'll
            # be fine for a while until disks get too big.
            return node.db.id * 1e9

        return sorted(self._nodes, key=_node_key)

    @nodes.setter
    def nodes(self, nodes: list[UpdateableNode]) -> None:
        """nodes setter

        This checks that nodes in group are transport nodes
        (storage_type == "T").

        Parameters
        ----------
        nodes : list of UpdateableNodes
            local active nodes in this group

        Raises
        ------
        ValueError
            none of the supplied `nodes` were Transport nodes.
        """
        self._nodes = []
        for node in nodes:
            if node.db.storage_type != "T":
                log.warning(
                    f'Ignoring non-transport node "{node.name}" '
                    f'in Transport Group "{self.group.name}"'
                )
            else:
                self._nodes.append(node)

        if not len(self._nodes):
            raise ValueError(
                f"no usable nodes ({len(nodes)} unusable) in "
                f"Transport group {self.group.name}"
            )

    def exists(self, path: pathlib.PurePath) -> UpdateableNode | None:
        """Checks whether a file at `path` exists in this group.

        Parameters
        ----------
        path : pathlib.PurePath
            the path to search for; relative to node root

        Returns
        -------
        exists : UpdateableNode or None
            If `path` is found on a node in the group, this is the node.
            If `path` was not found, this is None.

        Notes
        -----
        If `path` exists on multiple nodes in the group, then one of the
        nodes where it exists is returned.  The caller should _not_ assume
        this is stable: multiple calls to this method with the same `path`
        may return different nodes.
        """
        for node in self.nodes:
            if node.io.exists(path):
                return node

        return None

    def pull(self, req: ArchiveFileCopyRequest, did_search: bool) -> None:
        """Handle a pull request.

        Only local pulls are only fulfilled.  Remote pulls are ignored.

        The request will be handed off to the fullest node that can fit the
        file to be pulled.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the request to fulfill.  We are the destination group (i.e.
            `req.group_to == self.group`).
        did_search : boolean
            True if a group-level pre-pull search for an existing file was
            performed.  False otherwise.
        """

        # If this is a non-local transfer, skip it.
        if not req.node_from.local:
            log.info(
                f"Skipping pull of {req.file.path} from node "
                f"{req.node_from.name} to group {req.group_to.name}: "
                f"non-local transfer request."
            )
            return

        # loop through sorted nodes and pick a node
        #
        # Note: the node list returned by self.nodes has been sorted to
        #       put the most-desired node first
        for node in self.nodes:
            # Skip this node if normal pull-dest checks fail
            if not node.db.check_pull_dest(
                message=f"Ignoring transport node {node.name}", log_level=logging.DEBUG
            ):
                continue

            # Skip this node if the file won't fit
            if not node.io.fits(req.file.size_b):
                log.debug(f"Ignoring transport node {node.name}: not enough space")
                continue

            # If we got here, I guess we're going to use this disk
            # Hand the pull request off to the node
            node.io.pull(req, did_search)
            return

        # If we got here, we couldn't find a disk
        log.debug(f'Unable to find a transport node for "{req.file.path}"')
        return
