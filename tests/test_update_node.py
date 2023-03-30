"""Tests for UpdateableNode."""

import pytest

from alpenhorn.storage import StorageNode
from alpenhorn.update import UpdateableNode


def test_bad_ioclass(simplenode):
    """A missing I/O class is a problem."""

    simplenode.io_class = "Missing"
    with pytest.raises(ModuleNotFoundError):
        UpdateableNode(None, simplenode)

    simplenode.io_class = "alpenhorn.update.Missing"
    with pytest.raises(ImportError):
        UpdateableNode(None, simplenode)


def test_reinit(storagenode, simplegroup, queue):
    """Test UpdateableNode.reinit."""

    # Create a node
    stnode = storagenode(name="node", group=simplegroup)
    node = UpdateableNode(queue, stnode)

    # No I/O re-init
    stnode = StorageNode.get(id=stnode.id)
    assert stnode is not node.db
    io = node.io
    assert not node.reinit(stnode)
    assert io is node.io

    # But storagenode is updated
    assert stnode is node.db

    # Also no I/O re-init
    stnode = StorageNode.get(id=stnode.id)
    assert stnode is not node.db
    stnode.avail_gb = 3
    stnode.save()
    io = node.io
    assert not node.reinit(stnode)
    assert io is node.io
    assert stnode is node.db

    # Changing io_config forces re-init
    stnode = StorageNode.get(id=stnode.id)
    assert stnode is not node.db
    stnode.io_config = "{}"
    stnode.save()
    assert node.reinit(stnode)
    assert io is not node.io
    assert stnode is node.db

    # Changing io_class forces re-init
    stnode = StorageNode.get(id=stnode.id)
    assert stnode is not node.db
    stnode.io_class = "Default"
    stnode.save()
    io = node.io
    assert node.reinit(stnode)
    assert io is not node.io
    assert stnode is node.db

    # Changing id forces re-init
    #
    # Alpenhornd indexes UpdateabelNodes by node name, so this would happen if
    # StorageNode records have their names swapped around somehow, though we
    # don't need to do that in this test
    stnode = storagenode(
        name="node2",
        group=simplegroup,
        io_class=stnode.io_class,
        io_config=stnode.io_config,
    )
    assert stnode is not node.db
    io = node.io
    assert node.reinit(stnode)
    assert io is not node.io
    assert stnode is node.db


def test_bad_ioconfig(simplenode):
    """io_config not resolving to a dict is an error."""
    simplenode.io_config = "true"

    with pytest.raises(ValueError):
        UpdateableNode(None, simplenode)

    # But this is fine
    simplenode.io_config = "{}"
    UpdateableNode(None, simplenode)


def test_idle(unode, queue):
    """Test UpdateableNode.idle"""

    # Currently idle
    assert unode.idle is True

    # Enqueue something into this node's queue
    queue.put(None, unode.name)

    # Now not idle
    assert unode.idle is False

    # Dequeue it
    task, key = queue.get()

    # Still not idle, because task is in-progress
    assert unode.idle is False

    # Finish the task
    queue.task_done(unode.name)

    # Now idle again
    assert unode.idle is True
