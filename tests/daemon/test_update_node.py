"""Tests for UpdateableNode."""

import datetime
import pathlib
from unittest.mock import MagicMock, call, patch

import peewee as pw
import pytest

from alpenhorn.daemon.update import UpdateableNode
from alpenhorn.db.archive import ArchiveFileCopy, ArchiveFileImportRequest
from alpenhorn.db.storage import StorageNode


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
    queue.put(None, unode.io.fifo)

    # Now not idle
    assert unode.idle is False

    # Dequeue it
    task, key = queue.get()

    # Still not idle, because task is in-progress
    assert unode.idle is False

    # Finish the task
    queue.task_done(key)

    # Now idle again
    assert unode.idle is True


def test_check_init(unode):
    """Test UpdateableNode.check_init."""

    # Starts out active
    unode.db.active = True
    unode.db.save()
    assert unode.db.active

    # Pretend node is actually initialised
    with patch.object(unode.io, "check_init", lambda: True):
        assert unode.check_init()
    assert unode.db.active
    assert StorageNode.select(StorageNode.active).limit(1).scalar()

    # Pretend node is actually not initialised
    with patch.object(unode.io, "check_init", lambda: False):
        assert not unode.check_init()
    assert unode.db.active
    assert StorageNode.select(StorageNode.active).limit(1).scalar()


def test_update_do_init(xfs, unode, queue, archivefileimportrequest):
    """Test handling an init request"""

    # the req
    req = archivefileimportrequest(node=unode.db, path="ALPENHORN_NODE")

    # Activate node
    unode.db.active = True
    unode.db.save()

    xfs.create_dir("/node")

    # Call check_init
    unode.check_init()

    # Task is queued
    assert queue.qsize == 1

    # Run task
    task, fifo = queue.get()
    task()
    queue.task_done(fifo)

    # Node is initialised
    assert unode.io.check_init() is True

    # Request is complete
    assert ArchiveFileImportRequest.get(id=req.id).completed == 1


def test_update_free_space(unode):
    """Test UpdateableNode.update_free_space."""

    # Set the avail_gb to something
    unode.db.avail_gb = 3
    unode.db.save()

    now = pw.utcnow()
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
    last_update = pw.utcnow() - datetime.timedelta(days=10)

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

    unode.run_auto_verify()

    # CopyN is the only one not checked
    assert ArchiveFileCopy.get(id=copyY.id).has_file == "M"
    assert ArchiveFileCopy.get(id=copyN.id).has_file != "M"
    assert ArchiveFileCopy.get(id=copyM.id).has_file == "M"
    assert ArchiveFileCopy.get(id=copyX.id).has_file == "M"


def test_auto_verify_time(unode, simpleacq, archivefile, archivefilecopy):
    """Test checking time in UpdateableNode.run_auto_verify()"""

    # Enable auto_verify
    unode.db.auto_verify = 4

    # Files with different last update times
    copy9 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="file9", acq=simpleacq),
        has_file="Y",
        last_update=pw.utcnow() - datetime.timedelta(days=9),
    )
    copy8 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="file8", acq=simpleacq),
        has_file="Y",
        last_update=pw.utcnow() - datetime.timedelta(days=8),
    )
    copy6 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="file6", acq=simpleacq),
        has_file="Y",
        last_update=pw.utcnow() - datetime.timedelta(days=6),
    )
    copy5 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="file5", acq=simpleacq),
        has_file="Y",
        last_update=pw.utcnow() - datetime.timedelta(days=5),
    )

    unode.run_auto_verify()

    # Only old files need check
    assert ArchiveFileCopy.get(id=copy9.id).has_file == "M"
    assert ArchiveFileCopy.get(id=copy8.id).has_file == "M"
    assert ArchiveFileCopy.get(id=copy6.id).has_file != "M"
    assert ArchiveFileCopy.get(id=copy5.id).has_file != "M"


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

            queue.put(None, unode.io.fifo)
            unode.update_idle()

            # didn't run because not idle
            assert len(rav.mock_calls) == 0
            assert len(ioiu.mock_calls) == 1

            # Empty the queue
            queue.get()
            queue.task_done(unode.io.fifo)
            unode.update_idle()

            # idle again, so ran again
            assert len(rav.mock_calls) == 0
            assert len(ioiu.mock_calls) == 2

            # Turn on auto_verify
            unode.db.auto_verify = 1
            unode.update_idle()

            assert len(rav.mock_calls) == 1
            assert len(ioiu.mock_calls) == 3

            # newly_idle should only be true in the first call
            assert ioiu.mock_calls == [call(True), call(False), call(False)]


def test_update_delete_under_min(unode, simpleacq, archivefile, archivefilecopy):
    """Test UpdateableNode.update_delete() when under min"""

    archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileY", acq=simpleacq),
        has_file="Y",
        wants_file="Y",
    )
    copyM1 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileM1", acq=simpleacq),
        has_file="Y",
        wants_file="M",
        size_b=2 * 2**30,
    )
    copyN1 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileN1", acq=simpleacq),
        has_file="Y",
        wants_file="N",
        size_b=2 * 2**30,
    )
    copyM2 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileM2", acq=simpleacq),
        has_file="Y",
        wants_file="M",
        size_b=2 * 2**30,
    )
    archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileM3", acq=simpleacq),
        has_file="Y",
        wants_file="M",
        size_b=2 * 2**30,
    )
    copyN2 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileN2", acq=simpleacq),
        has_file="Y",
        wants_file="N",
        size_b=2 * 2**30,
    )
    archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileM4", acq=simpleacq),
        has_file="Y",
        wants_file="M",
        size_b=2 * 2**30,
    )
    copyN3 = archivefilecopy(
        node=unode.db,
        file=archivefile(name="fileN3", acq=simpleacq),
        has_file="Y",
        wants_file="N",
        size_b=2 * 2**30,
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
    # copyY, copyM3 and copyM4 are not deleted
    mock_delete.assert_called_once_with([copyM1, copyN1, copyM2, copyN2, copyN3])


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


def test_update_delete_transfer_pending(
    unode, simplegroup, simplefile, archivefilecopy, archivefilecopyrequest
):
    """update_delete() should skip files that have pending transfers."""

    archivefilecopy(file=simplefile, node=unode.db, has_file="Y", wants_file="N")

    # The blocking AFCR
    archivefilecopyrequest(file=simplefile, node_from=unode.db, group_to=simplegroup)

    mock_delete = MagicMock()
    with patch.object(unode.io, "delete", mock_delete):
        unode.update_delete()

    # A call is not made in this case
    mock_delete.assert_not_called()


def test_update_delete_bad_file(unode, simpleacq, archivefile, archivefilecopy):
    """update_delete() should attempt to delete bad files."""

    fileM = archivefile(name="fileM", acq=simpleacq)
    copyM = archivefilecopy(file=fileM, node=unode.db, has_file="M", wants_file="N")
    fileX = archivefile(name="fileX", acq=simpleacq)
    copyX = archivefilecopy(file=fileX, node=unode.db, has_file="X", wants_file="N")

    mock_delete = MagicMock()
    with patch.object(unode.io, "delete", mock_delete):
        unode.update_delete()

    # Both files were deleted
    mock_delete.assert_called_once_with([copyM, copyX])


def test_update_node_run(
    unode,
    queue,
    simpleacq,
    simplegroup,
    archivefile,
    archivefilecopy,
    archivefilecopyrequest,
):
    """Test running UpdateableNode.update_node."""

    # Make some things to check
    badfileY = archivefile(name="check_me", acq=simpleacq)
    badcopyY = archivefilecopy(
        node=unode.db, file=badfileY, has_file="M", wants_file="Y"
    )

    # This one should be skipped (because it's about to be deleted)
    badfileN = archivefile(name="dont_check_me", acq=simpleacq)
    badcopyN = archivefilecopy(
        node=unode.db, file=badfileN, has_file="M", wants_file="N"
    )

    # And something to ready for a pull
    goodfile = archivefile(name="ready_me", acq=simpleacq)
    archivefilecopy(node=unode.db, file=goodfile, has_file="Y", ready=False)
    afcr_good = archivefilecopyrequest(
        node_from=unode.db, group_to=simplegroup, file=goodfile
    )

    # And something to ignore (i.e. not ready for a pull)
    missingfile = archivefile(name="ignore_me", acq=simpleacq)
    archivefilecopyrequest(node_from=unode.db, group_to=simplegroup, file=missingfile)

    # And something already ready (i.e. no need to re-ready)
    readyfile = archivefile(name="skip_me", acq=simpleacq)
    archivefilecopy(node=unode.db, file=readyfile, has_file="Y", ready=True)
    archivefilecopyrequest(node_from=unode.db, group_to=simplegroup, file=readyfile)

    # Mock the remote so the ready can happen
    pull_ready_calls = set()

    def pull_ready(file):
        pull_ready_calls.add(file)
        return file.name == "skip_me"

    remote = MagicMock()
    remote.io.pull_ready = pull_ready
    with patch("alpenhorn.daemon.update.RemoteNode", lambda x: remote):
        # Mock IO
        mock = MagicMock()
        mock.before_update.return_value = True
        mock.bytes_avail.return_value = None
        mock.fifo = "n:mock"
        with patch.object(unode, "io", mock):
            # update runs
            unode.update()

    assert unode._updated is True

    # Check I/O calls
    calls = list(mock.mock_calls)
    assert len(calls) == 5
    assert call.bytes_avail(fast=False) in calls
    assert call.check(badcopyY) in calls
    assert call.delete([badcopyN]) in calls
    assert call.ready_pull(afcr_good) in calls

    # Check remote.pull_ready calls
    assert pull_ready_calls == {goodfile, readyfile}


def test_update_import_absolute(unode, queue, archivefileimportrequest):
    """update_import() should skip absolute paths."""

    afir = archivefileimportrequest(
        path=f"{unode.db.root}/absolute/path", node=unode.db, register=True
    )

    unode.update_import()

    # Nothing queued
    assert queue.qsize == 0

    # Request is completed and no file was imported
    assert ArchiveFileImportRequest.get(id=afir.id).completed == 1
    assert ArchiveFileCopy.select().count() == 0


def test_update_import_invalid_path(unode, queue, archivefileimportrequest):
    """update_import() should ignore invalid paths."""

    afir = archivefileimportrequest(path="./path/..", node=unode.db)

    unode.update_import()

    # Nothing queued
    assert queue.qsize == 0

    # Request is completed
    assert ArchiveFileImportRequest.get(id=afir.id).completed == 1


def test_update_import_outtree_scan(unode, xfs, queue, archivefileimportrequest):
    """Test rejection of out-of-tree scan paths from import requests"""

    xfs.create_dir("/node/import")

    afir = archivefileimportrequest(path="..", node=unode.db, recurse=True)

    unode.update_import()

    # Nothing queued
    assert queue.qsize == 0

    # Request is completed
    assert ArchiveFileImportRequest.get(id=afir.id).completed == 1


def test_update_import_unresolve_scan(unode, xfs, queue, archivefileimportrequest):
    """Test rejection of unresolvable scan paths from import requests"""

    xfs.create_dir("/node/import")

    afir = archivefileimportrequest(path="path/subpath/..", node=unode.db, recurse=True)

    unode.update_import()

    # Nothing queued
    assert queue.qsize == 0

    # Request is completed
    assert ArchiveFileImportRequest.get(id=afir.id).completed == 1


def test_update_import_no_recurse(unode, queue, archivefileimportrequest):
    """Test update_import() with a non-recursive import request."""

    afir = archivefileimportrequest(
        path="import/path", node=unode.db, recurse=False, register=True
    )

    mock = MagicMock()
    with patch("alpenhorn.daemon.auto_import.import_file", mock):
        unode.update_import()

    # Nothing queued
    assert queue.qsize == 0

    # Mocked import_file was called
    mock.assert_called_with(unode, queue, "import/path", True, afir)


def test_update_import_scan(unode, xfs, queue, archivefileimportrequest):
    """Test update_import() with a recursive import request."""

    xfs.create_dir("/node/import/path")

    afir = archivefileimportrequest(
        path="import/path", node=unode.db, recurse=True, register=True
    )

    mock = MagicMock()
    with patch("alpenhorn.daemon.auto_import.scan", mock):
        unode.update_import()

    # scan queued
    assert queue.qsize == 1

    # Run scan
    task, fifo = queue.get()
    task()
    queue.task_done(fifo)

    # Mocked scan was called
    mock.assert_called_with(task, unode, queue, pathlib.Path("import/path"), True, afir)


def test_update_import_alpenhorn_node(unode, queue, archivefileimportrequest):
    """Test update_import() doesn't try to import ALPENHORN_NODE.

    Even if we try to trick it...
    """

    archivefileimportrequest(
        path="./ALPENHORN_NODE", node=unode.db, recurse=False, register=True
    )

    unode.update_import()

    # nothing queued
    assert queue.qsize == 0
