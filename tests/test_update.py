"""Tests for the alpenhorn.update module."""

import pytest
from unittest.mock import patch, MagicMock

from alpenhorn import pool, update
from alpenhorn.io.base import BaseGroupIO, BaseNodeIO
from alpenhorn.queue import FairMultiFIFOQueue
from alpenhorn.storage import StorageGroup, StorageNode


@pytest.fixture
def emptypool(queue):
    """Create an empty worker pool."""
    return pool.EmptyPool()


@pytest.fixture
def fastqueue():
    """Like FMFQ, but fast.  Because who has time for unittests?"""

    class Fast_FMFQ(FairMultiFIFOQueue):
        """Like FMFQ, but get() returns immediately if the queue is empty."""

        def get(self, timeout=None):
            if self._total_queued == 0:
                return None
            return super().get(timeout)

    return Fast_FMFQ()


@pytest.fixture
def mock_serial_io(dbtables):
    """Mock the serial_io() call so no I/O actually happens.

    As a side benefit, the mocked function is faster at
    doing nothing.
    """

    mock = MagicMock()
    with patch("alpenhorn.update.serial_io", mock):
        yield mock


def test_update_abort():
    """Test update_loop with global_abort set."""

    # Raise global abort
    pool.global_abort.set()

    # This should do nothing except exit, so passing
    # a couple of Nones shouldn't be a problem
    update.update_loop(None, None, None)

    # Reset
    pool.global_abort.clear()


def test_update_no_nodes(
    hostname, dbtables, queue, emptypool, loop_once, mock_serial_io
):
    """Test update_loop with no active nodes."""

    update.update_loop(hostname, queue, emptypool)

    mock_serial_io.assert_called_once_with(queue)


def test_update_node_not_idle(
    hostname, xfs, mockgroupandnode, queue, emptypool, loop_once, mock_serial_io
):
    """Test update_loop with a not idle node."""
    mockio, _, node = mockgroupandnode

    # Set up the I/O mock
    mockio.node.before_update.return_value = True

    # Ensure queue non-idle
    queue.put(None, node.name)

    xfs.create_file("/mocknode/ALPENHORN_NODE", contents="mocknode")

    update.update_loop(hostname, queue, emptypool)

    # Node update started
    mockio.node.before_update.assert_called_once()

    # Idle update didn't happen
    mockio.node.idle_update.assert_not_called()

    # After update hook called
    mockio.node.after_update.assert_called_once()


def test_update_node_idle(
    hostname, xfs, mockgroupandnode, queue, emptypool, loop_once, mock_serial_io
):
    """Test update_loop with an idle node."""

    mockio = mockgroupandnode[0]

    # Set up the I/O mock
    mockio.node.before_update.return_value = True

    xfs.create_file("/mocknode/ALPENHORN_NODE", contents="mocknode")

    update.update_loop(hostname, queue, emptypool)

    # Node update started
    mockio.node.before_update.assert_called_once()

    # Idle update happened
    mockio.node.idle_update.assert_called_once()

    # After update hook called
    mockio.node.after_update.assert_called_once()


def test_update_node_cancelled(
    hostname, xfs, mockgroupandnode, queue, emptypool, loop_once, mock_serial_io
):
    """Test update_loop with a node that cancels the update."""
    mockio = mockgroupandnode[0]

    # Set up the I/O mock
    mockio.node.before_update.return_value = False

    xfs.create_file("/mocknode/ALPENHORN_NODE", contents="mocknode")

    update.update_loop(hostname, queue, emptypool)

    # Node update started
    mockio.node.before_update.assert_called_once()

    # Idle update didn't happen
    mockio.node.idle_update.assert_not_called()

    # After update hook called
    mockio.node.after_update.assert_called_once()


def test_serial_io(fastqueue, set_config):
    """Test serial_io."""

    # This is our task
    task_count = 0

    def task():
        nonlocal task_count
        task_count += 1

    # Put some tasks in the queue
    fastqueue.put(task, "fifo")
    fastqueue.put(task, "fifo")
    fastqueue.put(task, "fifo")

    # Check count
    assert fastqueue.qsize == 3

    # Run serial_io
    update.serial_io(fastqueue)

    # Now the queue is empty
    assert fastqueue.qsize == 0

    # The task was executed three times
    assert task_count == 3


def test_ioload(storagegroup, storagenode):
    """Test instantiation of the I/O classes"""

    for ioclass in ["Default", "Transport", None]:
        group = storagegroup(
            name="none" if ioclass is None else ioclass, io_class=ioclass
        )

    for ioclass, ioconfig in [
        ("Default", None),
        (None, None),
    ]:
        storagenode(
            name="none" if ioclass is None else ioclass,
            group=group,
            io_class=ioclass,
            io_config=ioconfig,
        )

    for node in StorageNode.select().execute():
        unode = update.UpdateableNode(None, node)
        assert isinstance(unode.io, BaseNodeIO)

    for group in StorageGroup.select().execute():
        ugroup = update.UpdateableGroup(group=group, nodes=[], idle=True)
        assert isinstance(ugroup.io, BaseGroupIO)


def test_update_group_not_idle_node(
    hostname, xfs, mockgroupandnode, queue, emptypool, loop_once, mock_serial_io
):
    """Test update_loop with a not idle group.

    Here the node is not idle, so the group update is skipped.
    As a result, the group idle check doesn't happen (group idle
    is forced to be False in this instance)."""

    mockio, _, node = mockgroupandnode

    # Set up the I/O mock
    mockio.node.before_update.return_value = True
    mockio.group.before_update.return_value = True

    # Node not idle
    queue.put(None, node.name)

    xfs.create_file("/mocknode/ALPENHORN_NODE", contents="mocknode")

    update.update_loop(hostname, queue, emptypool)

    # Node update started
    mockio.node.before_update.assert_called_once()

    # Idle update didn't happen
    mockio.group.idle_update.assert_not_called()

    # After update hook called
    mockio.node.after_update.assert_called_once()


def test_update_group_not_idle_group(
    hostname, xfs, mockgroupandnode, queue, emptypool, loop_once, mock_serial_io
):
    """Test update_loop with a not idle group.

    Here the update happens because the node is idle,
    but we force the group to appear non-idle after
    the update."""

    mockio, group, node = mockgroupandnode

    # This function adds something to the queue so that after the
    # node update, it's not idle
    def node_before_update(idle):
        nonlocal queue, node
        queue.put(None, node.name)

        return True

    # Set up the I/O mock
    mockio.node.before_update = node_before_update
    mockio.group.before_update.return_value = True

    xfs.create_file("/mocknode/ALPENHORN_NODE", contents="mocknode")

    update.update_loop(hostname, queue, emptypool)

    # Idle update didn't happen
    mockio.group.idle_update.assert_not_called()

    # After update hook called
    mockio.node.after_update.assert_called_once()


def test_update_group_idle(
    hostname, xfs, mockgroupandnode, queue, emptypool, loop_once, mock_serial_io
):
    """Test update_loop with an idle group."""

    mockio = mockgroupandnode[0]

    # Set up the I/O mock
    mockio.node.before_update.return_value = True
    mockio.group.before_update.return_value = True

    xfs.create_file("/mocknode/ALPENHORN_NODE", contents="mocknode")

    update.update_loop(hostname, queue, emptypool)

    # Group update started
    mockio.group.before_update.assert_called_once()

    # Idle update happened
    mockio.group.idle_update.assert_called_once()

    # After update hook called
    mockio.group.after_update.assert_called_once()


def test_update_group_cancelled(
    hostname, xfs, mockgroupandnode, queue, emptypool, loop_once, mock_serial_io
):
    """Test update_loop with a group that cancels the update."""

    mockio = mockgroupandnode[0]

    # Set up the I/O mock
    mockio.node.before_update.return_value = True
    mockio.group.before_update.return_value = False

    xfs.create_file("/mocknode/ALPENHORN_NODE", contents="mocknode")

    update.update_loop(hostname, queue, emptypool)

    # Group update started
    mockio.group.before_update.assert_called_once()

    # Idle update didn't happen
    mockio.group.idle_update.assert_not_called()

    # After update hook called
    mockio.group.after_update.assert_called_once()
