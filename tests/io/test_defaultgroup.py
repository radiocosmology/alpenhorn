"""Test DefaultGroupIO."""

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def groupnode(xfs, queue, storagegroup, storagenode):
    """Fixture setting up a default test group.

    Returns both the group and the node."""
    group = storagegroup(name="group")
    node = storagenode(name="node", group=group, active=True, root="/node")

    # Init Node I/O
    node.io.set_queue(queue)

    # Init Group I/O
    group.io.before_update([node], True)

    # Create the directory
    xfs.create_dir("/node")

    return group, node


def test_too_many_nodes(storagegroup, storagenode):
    """Test DefaultGroupIO rejecting more than one node."""

    group = storagegroup(name="group")
    node1 = storagenode(name="node1", group=group, active=True)
    node2 = storagenode(name="node2", group=group, active=True)

    assert group.io.before_update([node1, node2], True) is False


def test_just_enough_nodes(storagegroup, storagenode):
    """Test DefaultGroupIO accepting one node."""

    group = storagegroup(name="group")
    node = storagenode(name="node1", group=group, active=True)

    assert group.io.before_update([node], True) is True


def test_idle(queue, groupnode):
    """Test DefaultGroupIO.idle."""
    group, node = groupnode

    # Currently idle
    assert group.io.idle is True

    # Enqueue something into this node's queue
    queue.put(None, node.name)

    # Now not idle
    assert group.io.idle is False

    # Dequeue it
    task, key = queue.get()

    # Still not idle, because task is in-progress
    assert group.io.idle is False

    # Finish the task
    queue.task_done(node.name)

    # Now idle again
    assert group.io.idle is True


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
