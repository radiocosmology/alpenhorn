"""Transport Group I/O."""
import logging

from .base import BaseGroupIO

log = logging.getLogger(__name__)


class TransportGroupIO(BaseGroupIO):
    """Transport Group I/O.

    This implements (the formerly special-cased) transport disk logic.

    A Transport StorageGroup is used to transfer data onto transiting
    storage.

    Features of a Transport StorageGroup:
        - it may have any number of nodes.  All node must have
            node.storage_type == 'T', but no restrictions are put on the
            io_class of nodes
        - all pulls to the StorageGroup must be local: non-local pull
            requests will be ignored
        - when handling pull requests, transport nodes are prioritised by
            increasing free space: the group will attempt to pull a file
            to the fullest node that it thinks it will fit on.
    """

    @property
    def idle(self):
        """Returns the True if no node I/O is occurring."""
        for node in self._nodes:
            if not node.io.idle:  # Short circuit
                return False
        return True

    def before_update(self, nodes, queue_empty):
        """Check that nodes in group are transit nodes.

        Discards ones that aren't.  If that leaves us with no nodes,
        returns True to cancel the update."""
        self._nodes = list()
        for node in nodes:
            if node.storage_type != "T":
                log.warning(
                    f'Ignoring non-transport node "{node.name}" '
                    f'in Transport Group "{self.group.name}"'
                )
            else:
                self._nodes.append(node)

        return len(self._nodes) != 0

    def exists(self, path):
        """Checks whether a file called path exists in this group.

        Returns the StorageNode containing the file, or None if no
        file was found.
        """
        for node in self._nodes:
            if node.io.exists(path):
                return node

        return None

    def pull(self, req):
        """Handle a pull request.

        The request will be handed off to the fullest node that can fit the
        file to be pulled.
        """

        # If this is a non-local transfer, skip it.
        if not req.node_from.local:
            log.info(
                f"Skipping pull of {req.file.path} from node "
                f"{req.node_from.name} to group {req.group_to.name}: "
                f"non-local transfer request."
            )
            return

        # Sort the nodes; this has to be done for every request because
        # available space (hopefully) changes as requests are submitted and
        # complete.

        # Our node-sorting function
        def _node_sort(node):
            """Returns node.avail_gb if that's numeric or else a very large float
            if it's None."""
            n = node.avail_gb
            if n is not None:
                return n

            # Using node.id here makes ordering stable.  1e9 GB = 1EB, so we'll
            # be fine for a while until disks get too big.
            return node.id * 1e9

        # loop through sorted nodes and pick a node
        node_to = None
        for node in sorted(self._nodes, key=_node_sort):
            if node.avail_gb is None:
                # In this case, we've run out of nodes that know how full they are
                # so just use the first one we get.  This is probably going to
                # cause trouble; probably we shouldn't be using transport disks
                # which can't tell us how full they are.
                node_to = node
                break

            # Skip node out of space
            if node.under_min():
                log.debug(f"Ignoring transport node {node.name}: hit min_avail_gb")
                continue

            # Skip full node
            if node.over_max():
                log.debug(f"Ignoring transport node {node.name}: hit max_total_gb")
                continue

            # Skip this node if the file won't fit
            if not node.io.fits(req.file.size_b):
                log.debug(f"Ignoring transport node {node.name}: not enough space")
                continue

            # If we got here, I guess we're going to use this disk
            node_to = node
            break

        # Check that we got a disk
        if node_to is None:
            log.debug(f'Unable to find a transport node for "{req.file.path}"')
            return

        # Otherwise hand the pull request off to the node
        node_to.io.pull(req)
