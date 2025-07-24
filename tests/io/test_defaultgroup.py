"""Test DefaultGroupIO."""

from unittest.mock import MagicMock

import pytest

from alpenhorn.daemon.update import UpdateableGroup, UpdateableNode


@pytest.fixture
def groupnode(xfs, dbtables, queue, storagegroup, storagenode):
    """Fixture setting up a default test group.

    Returns both the group and the node."""
    stgroup = storagegroup(name="group")
    node = UpdateableNode(
        queue, storagenode(name="node", group=stgroup, active=True, root="/node")
    )
    group = UpdateableGroup(queue=queue, group=stgroup, nodes=[node], idle=True)

    # Create the directory
    xfs.create_dir("/node")

    return group, node


def test_too_many_nodes(storagegroup, storagenode, queue):
    """Test DefaultGroupIO rejecting more than one node."""

    stgroup = storagegroup(name="group")
    node1 = UpdateableNode(queue, storagenode(name="node1", group=stgroup, active=True))
    node2 = UpdateableNode(queue, storagenode(name="node2", group=stgroup, active=True))

    group = UpdateableGroup(queue=queue, group=stgroup, nodes=[node1, node2], idle=True)
    assert group._nodes is None


def test_just_enough_nodes(storagegroup, storagenode, queue):
    """Test DefaultGroupIO accepting one node."""

    stgroup = storagegroup(name="group")
    node = UpdateableNode(queue, storagenode(name="node1", group=stgroup, active=True))

    group = UpdateableGroup(queue=queue, group=stgroup, nodes=[node], idle=True)
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


def test_pull_handoff(groupnode, simplecopyrequest):
    """Test DefaultGroupIO.pull()."""

    group, node = groupnode
    node.io.pull = MagicMock(return_value=None)

    # Call pull
    group.io.pull(simplecopyrequest, did_search=True)

    # Check for hand-off to the node
    node.io.pull.assert_called_with(simplecopyrequest, True)
