"""Tests for UpdateableNode."""

import pytest
import datetime
from unittest.mock import call, patch, MagicMock

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

    now = datetime.datetime.utcnow()
    # 2 ** 32 bytes is 4 GiB
    with patch.object(unode.io, "bytes_avail", lambda fast: 2**32):
        unode.update_free_space()

    # Node has been updated.
    node = StorageNode.get(id=unode.db.id)
    assert node.avail_gb == 4
    assert node.avail_gb_last_checked >= now


def test_auto_verify(unode, simpleacq, archivefile, archivefilecopy):
    """Test UpdateableNode.run_auto_verify()"""

    # Enable auto_verify
    unode.db.auto_verify = 4

    # Last Update time to permit auto verification
    last_update = datetime.datetime.utcnow() - datetime.timedelta(days=10)

    # Make some files to verify
    copyY = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileY", acq=simpleacq),
        has_file="Y",
        last_update=last_update,
    )
    copyN = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileN", acq=simpleacq),
        has_file="N",
        last_update=last_update,
    )
    copyM = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileM", acq=simpleacq),
        has_file="M",
        last_update=last_update,
    )
    copyX = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileX", acq=simpleacq),
        has_file="X",
        last_update=last_update,
    )

    mock = MagicMock()
    with patch.object(unode.io, "auto_verify", mock):
        unode.run_auto_verify()
    calls = list(mock.mock_calls)

    # CopyN not checked
    assert call(copyY) in calls
    assert call(copyN) not in calls
    assert call(copyM) in calls
    assert call(copyX) in calls


def test_auto_verify_dups(unode, simpleacq, archivefile, archivefilecopy):
    """Test getting duplicates in UpdateableNode.run_auto_verify()"""

    # Enable auto_verify
    unode.db.auto_verify = 4

    # Last Update time to permit auto verification
    last_update = datetime.datetime.utcnow() - datetime.timedelta(days=10)

    # Only one file
    copyY = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileY", acq=simpleacq),
        has_file="Y",
        last_update=last_update,
    )

    mock = MagicMock()
    with patch.object(unode.io, "auto_verify", mock):
        unode.run_auto_verify()

    # auto_verify only should be called once
    mock.assert_called_once_with(copyY)


def test_auto_verify_time(unode, simpleacq, archivefile, archivefilecopy):
    """Test checking time in UpdateableNode.run_auto_verify()"""

    # Enable auto_verify
    unode.db.auto_verify = 4

    # Files with different last update times
    copy9 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="file9", acq=simpleacq),
        has_file="Y",
        last_update=datetime.datetime.utcnow() - datetime.timedelta(days=9),
    )
    copy8 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="file8", acq=simpleacq),
        has_file="Y",
        last_update=datetime.datetime.utcnow() - datetime.timedelta(days=8),
    )
    copy6 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="file6", acq=simpleacq),
        has_file="Y",
        last_update=datetime.datetime.utcnow() - datetime.timedelta(days=6),
    )
    copy5 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="file5", acq=simpleacq),
        has_file="Y",
        last_update=datetime.datetime.utcnow() - datetime.timedelta(days=5),
    )

    mock = MagicMock()
    with patch.object(unode.io, "auto_verify", mock):
        unode.run_auto_verify()
    calls = list(mock.mock_calls)

    # Only old files have been checked
    assert call(copy9) in calls
    assert call(copy8) in calls
    assert call(copy6) not in calls
    assert call(copy5) not in calls


@pytest.mark.lfs_hsm_state(
    {
        "/node/simplefile_acq/simplefile": "released",
    }
)
def test_auto_verify_released(
    mock_lfs, xfs, queue, simplenode, simplefile, archivefilecopy
):
    """Shouldn't auto-verify a released file twice."""

    simplenode.io_class = "LustreHSM"
    simplenode.io_config = '{"quota_group": "qgroup", "headroom": 300000}'

    # Enable auto_verify
    simplenode.auto_verify = 1

    # Create file
    xfs.create_file("/node/simplefile_acq/simplefile")

    # Make the node
    unode = UpdateableNode(queue, simplenode)

    # This is the only file
    archivefilecopy(
        node=unode.db,
        file=simplefile,
        has_file="Y",
        last_update=datetime.datetime.utcnow() - datetime.timedelta(days=10),
    )

    unode.run_auto_verify()
    unode.run_auto_verify()

    # There should be one pending check
    assert queue.qsize == 1


def test_update_idle(unode, queue):
    """Test UpdateableNode.update_idle()"""

    # Ensure auto_verify is off
    assert unode.db.auto_verify == 0

    rav = MagicMock()
    ioiu = MagicMock()
    with patch.object(unode, "run_auto_verify", rav):
        with patch.object(unode.io, "idle_update", ioiu):
            unode._updated = False
            unode.update_idle()

            # did not run because update didn't happen
            assert len(rav.mock_calls) == 0
            assert len(ioiu.mock_calls) == 0

            unode._updated = True
            unode.update_idle()

            # now ran, but no auto_verify
            assert len(rav.mock_calls) == 0
            assert len(ioiu.mock_calls) == 1

            queue.put(None, unode.name)
            unode.update_idle()

            # didn't run because not idle
            assert len(rav.mock_calls) == 0
            assert len(ioiu.mock_calls) == 1

            # Empty the queue
            queue.get()
            queue.task_done(unode.name)
            unode.update_idle()

            # idle again, so ran again
            assert len(rav.mock_calls) == 0
            assert len(ioiu.mock_calls) == 2

            # Turn on auto_verify
            unode.db.auto_verify = 1
            unode.update_idle()

            assert len(rav.mock_calls) == 1
            assert len(ioiu.mock_calls) == 3


def test_update_delete_under_min(unode, simpleacq, archivefile, archivefilecopy):
    """Test UpdateableNode.update_delete() when not under min"""

    archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileY", acq=simpleacq),
        has_file="Y",
        wants_file="Y",
    )
    copyM = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileM", acq=simpleacq),
        has_file="Y",
        wants_file="M",
    )
    copyN = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileN", acq=simpleacq),
        has_file="Y",
        wants_file="N",
    )

    # Force under min and not archive
    unode.db.avail_gb = 5
    unode.db.min_avail_gb = 10
    unode.db.storage_type = "F"
    assert unode.db.under_min
    assert not unode.db.archive

    mock_delete = MagicMock()
    with patch.object(unode.io, "delete", mock_delete):
        unode.update_delete()
    mock_delete.assert_called_once_with([copyM, copyN])


def test_update_delete_over_min(unode, simpleacq, archivefile, archivefilecopy):
    """Test UpdateableNode.update_delete() when not under min"""

    archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileY", acq=simpleacq),
        has_file="Y",
        wants_file="Y",
    )
    archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileM", acq=simpleacq),
        has_file="Y",
        wants_file="M",
    )
    copyN = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileN", acq=simpleacq),
        has_file="Y",
        wants_file="N",
    )

    mock_delete = MagicMock()
    with patch.object(unode.io, "delete", mock_delete):
        unode.update_delete()
    mock_delete.assert_called_once_with([copyN])


def test_update_node_run(
    unode, queue, simplegroup, simplefile, archivefilecopy, archivefilecopyrequest
):
    """Test running UpdateableNode.update_node."""

    # Make something to check
    copy = archivefilecopy(node=unode.db, file=simplefile, has_file="M")

    # And something to pull
    afcr = archivefilecopyrequest(
        node_from=unode.db, group_to=simplegroup, file=simplefile
    )

    mock = MagicMock()
    mock.before_update.return_value = True
    mock.bytes_avail.return_value = None
    with patch.object(unode, "io", mock):
        # update runs
        unode.update()

    assert unode._updated is True

    # Check I/O calls
    calls = list(mock.mock_calls)
    assert len(calls) == 5
    assert call.bytes_avail(fast=False) in calls
    assert call.check(copy) in calls
    assert call.delete([]) in calls
    assert call.ready_pull(afcr) in calls
