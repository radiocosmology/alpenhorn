"""Tests for UpdateableNode."""

import pytest
import datetime
from unittest.mock import patch

from alpenhorn.storage import StorageNode
from alpenhorn.update import UpdateableNode


def test_bad_ioclass(simplenode):
    """A missing I/O class is a problem."""

    simplenode.io_class = "Missing"
    unode = UpdateableNode(None, simplenode)
    assert unode.io_class is None
    assert unode.io is None


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


def test_update_active(unode):
    """Test UpdateableNode.update_active."""

    # Starts out active
    unode.db.active = True
    unode.db.save()
    assert unode.db.active

    # Pretend node is actually active
    with patch.object(unode.io, "check_active", lambda: True):
        assert unode.update_active()
    assert unode.db.active
    assert StorageNode.select(StorageNode.active).limit(1).scalar()

    # Pretend node is actually not active
    with patch.object(unode.io, "check_active", lambda: False):
        assert not unode.update_active()
    assert not unode.db.active
    assert not StorageNode.select(StorageNode.active).limit(1).scalar()


def test_update_free_space(unode):
    """Test UpdateableNode.update_free_space."""

    # Set the avail_gb to something
    unode.db.avail_gb = 3
    unode.db.save()

    now = datetime.datetime.now()
    # 2 ** 32 bytes is 4 GiB
    with patch.object(unode.io, "bytes_avail", lambda fast: 2**32):
        unode.update_free_space()

    # Node has been updated.
    node = StorageNode.get(id=unode.db.id)
    assert node.avail_gb == 4
    assert node.avail_gb_last_checked >= now
