"""Test DefaultGroupIO."""

import pytest
from unittest.mock import MagicMock

from alpenhorn.update import UpdateableNode, UpdateableGroup


@pytest.fixture
def groupnode(xfs, queue, storagegroup, storagenode):
    """Fixture setting up a default test group.

    Returns both the group and the node."""
    stgroup = storagegroup(name="group")
    node = UpdateableNode(
        queue, storagenode(name="node", group=stgroup, active=True, root="/node")
    )
    group = UpdateableGroup(group=stgroup, nodes=[node], idle=True)

    # Create the directory
    xfs.create_dir("/node")

    return group, node


def test_too_many_nodes(storagegroup, storagenode):
    """Test DefaultGroupIO rejecting more than one node."""

    stgroup = storagegroup(name="group")
    node1 = UpdateableNode(None, storagenode(name="node1", group=stgroup, active=True))
    node2 = UpdateableNode(None, storagenode(name="node2", group=stgroup, active=True))

    group = UpdateableGroup(group=stgroup, nodes=[node1, node2], idle=True)
    assert group._nodes is None


def test_just_enough_nodes(storagegroup, storagenode):
    """Test DefaultGroupIO accepting one node."""

    stgroup = storagegroup(name="group")
    node = UpdateableNode(None, storagenode(name="node1", group=stgroup, active=True))

    group = UpdateableGroup(group=stgroup, nodes=[node], idle=True)
    assert group._nodes == [node]


def test_exists(xfs, groupnode):
    """Test DefaultGroupIO.exists."""

    group, node = groupnode

    # Make something on the node
    xfs.create_file("/node/acq/file")

    # Check
    assert group.io.exists("acq/file") == node
    assert group.io.exists("acq/no-file") is None
    assert group.io.exists("no-acq/no-file") is None


def test_pull_handoff(groupnode, simplerequest):
    """Test DefaultGroupIO.pull()."""

    # We mock the DefaultNodeIO.pull() method because we don't need to test it here.
    group, node = groupnode
    node.io.pull = MagicMock(return_value=None)

    # Call pull
    group.io.pull(simplerequest)

    # Check for hand-off to the node
    node.io.pull.assert_called_with(simplerequest)
