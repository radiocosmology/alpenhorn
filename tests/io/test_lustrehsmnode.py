"""Test LustreHSMNodeIO."""

from unittest.mock import MagicMock, patch

import peewee as pw
import pytest

from alpenhorn.daemon.update import UpdateableNode
from alpenhorn.db.archive import ArchiveFileCopy


@pytest.fixture
def node(
    mock_lfs,
    queue,
    simplenode,
    simpleacq,
    archivefile,
    archivefilecopy,
):
    """A LustreHSM node for testing with some stuff on it"""
    simplenode.io_class = "LustreHSM"

    simplenode.io_config = (
        '{"quota_group": "qgroup", "headroom": 10250, "release_check_count": 7}'
    )

    # Some files
    files = [
        archivefile(name="file1", acq=simpleacq, size_b=100000),
        archivefile(name="file2", acq=simpleacq, size_b=300000),
        archivefile(name="file3", acq=simpleacq, size_b=400000),
        archivefile(name="file4", acq=simpleacq, size_b=800000),
        archivefile(name="file5", acq=simpleacq, size_b=50000),
        archivefile(name="file6", acq=simpleacq, size_b=300000),
        archivefile(name="file7", acq=simpleacq, size_b=200000),
    ]

    # Some copies
    last_updates = [3, 1, 6, 7, 2, 4, 5]
    for num, file in enumerate(files):
        archivefilecopy(file=file, node=simplenode, has_file="Y", size_b=10, ready=True)
        # We need to do it this way to set last_update
        ArchiveFileCopy.update(last_update=last_updates[num]).where(
            ArchiveFileCopy.id == num + 1
        ).execute()

    return UpdateableNode(queue, simplenode)


def test_init_no_headroom(have_lfs, simplenode):
    """No headroom is an error"""

    simplenode.io_class = "LustreHSM"
    simplenode.io_config = '{"quota_group": "qgroup"}'

    with pytest.raises(KeyError):
        UpdateableNode(None, simplenode)


def test_init_bad_release_count(simplenode, have_lfs):
    """Check for bad release_check_count."""

    simplenode.io_class = "LustreHSM"
    simplenode.io_config = (
        '{"quota_group": "qgroup", "headroom": 300000, "release_check_count": -1}'
    )

    with pytest.raises(ValueError):
        UpdateableNode(None, simplenode)


def test_init_bad_restore_wait(simplenode, have_lfs):
    """Check for bad restore_wait."""

    simplenode.io_class = "LustreHSM"
    simplenode.io_config = (
        '{"quota_group": "qgroup", "headroom": 300000, "restore_wait": -1}'
    )

    with pytest.raises(ValueError):
        UpdateableNode(None, simplenode)


def test_release_files_okay(queue, node):
    """Test running release_files when we're under headroom"""

    node.db.avail_gb = 20000000.0 / 2**30
    node.db.save()

    node.io.release_files()

    # Shouldn't be anything in the queue
    assert queue.qsize == 0


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restored",
        "/node/simpleacq/file2": "restored",
        "/node/simpleacq/file3": "restored",
        "/node/simpleacq/file4": "restored",
        "/node/simpleacq/file5": "restored",
        "/node/simpleacq/file6": "unarchived",
        "/node/simpleacq/file7": "restoring",
    }
)
def test_release_files(queue, mock_lfs, node):
    """Test running release_files."""

    node.db.avail_gb = 10000000.0 / 2**30
    node.db.save()

    # File7 is not ready
    ArchiveFileCopy.update(ready=False).where(ArchiveFileCopy.id == 7).execute()

    node.io.release_files()

    before = pw.utcnow().replace(microsecond=0)

    # Job in queue
    assert queue.qsize == 1

    # Run the task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Headroom is about 10.5MB, slightly more than the 10MB we report for quota
    # remaining.  As a result, we have released 4 files, ordered by last_update:
    #  - file2: 300 kB  [ last_update = 1 ]
    #  - file5:  50 kB  [ last_update = 2 ]
    #  - file1: 100 kB  [ last_update = 3 ]
    #  - file6 is skipped because it's not archived [last_update = 4]
    #  - file7 is skipped because it's being restored [last_update = 5]
    #  - file3: 400 kB  [ last_update = 6 ]
    # file4 remains restored
    assert not ArchiveFileCopy.get(id=1).ready
    assert ArchiveFileCopy.get(id=1).last_update >= before

    assert not ArchiveFileCopy.get(id=2).ready
    assert ArchiveFileCopy.get(id=2).last_update >= before

    assert not ArchiveFileCopy.get(id=3).ready
    assert ArchiveFileCopy.get(id=3).last_update >= before

    assert ArchiveFileCopy.get(id=4).ready

    assert not ArchiveFileCopy.get(id=5).ready
    assert ArchiveFileCopy.get(id=5).last_update >= before

    assert ArchiveFileCopy.get(id=6).last_update == 4
    assert ArchiveFileCopy.get(id=7).last_update == 5

    # Check hsm_relase was actually called
    lfs = mock_lfs("")
    assert lfs.hsm_state("/node/simpleacq/file1") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/simpleacq/file2") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/simpleacq/file3") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/simpleacq/file4") == lfs.HSM_RESTORED
    assert lfs.hsm_state("/node/simpleacq/file5") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/simpleacq/file6") == lfs.HSM_UNARCHIVED
    assert lfs.hsm_state("/node/simpleacq/file7") == lfs.HSM_RESTORING


def test_before_update(queue, node):
    """Test LustreHSMNodeIO.before_update()"""

    node.db.avail_gb = 10000000.0 / 2**30
    node.db.save()

    # When not idle, the release_files task is not run
    node.io.before_update(idle=False)

    assert queue.qsize == 0

    # But when idle it should run
    node.io.before_update(idle=True)

    assert queue.qsize == 1


def test_filesize(xfs, node):
    """Test LustreHSMNodeIO.filesize(), which always returns st_size"""

    xfs.create_file("/node/simpleacq/file1", st_size=100000)
    assert node.io.filesize("simpleacq/file1") == 100000


@pytest.mark.lfs_hsm_state(
    {
        "/node/dir/file1": "released",
        "/node/dir/file2": "restored",
        "/node/dir/file3": "restoring",
    }
)
def test_open_binary(xfs, node):
    """Test binary LustreHSMNodeIO.open()"""

    xfs.create_file("/node/dir/file1", contents="file1 contents")
    xfs.create_file("/node/dir/file2", contents="file2 contents")
    xfs.create_file("/node/dir/file3", contents="file3 contents")

    with pytest.raises(OSError):
        node.io.open("dir/file1", binary=True)

    with node.io.open("dir/file2", binary=True) as f:
        assert f.read() == b"file2 contents"

    with pytest.raises(OSError):
        node.io.open("dir/file3", binary=True)


@pytest.mark.lfs_hsm_state(
    {
        "/node/dir/file1": "released",
        "/node/dir/file2": "restored",
        "/node/dir/file3": "restoring",
    }
)
def test_open_text(xfs, node):
    """Test text LustreHSMNodeIO.open()"""

    xfs.create_file("/node/dir/file1", contents="file1 contents")
    xfs.create_file("/node/dir/file2", contents="file2 contents")
    xfs.create_file("/node/dir/file3", contents="file3 contents")

    with pytest.raises(OSError):
        node.io.open("dir/file1", binary=False)

    with node.io.open("dir/file2", binary=False) as f:
        assert f.read() == "file2 contents"

    with pytest.raises(OSError):
        node.io.open("dir/file3", binary=False)


def test_check_missing(queue, node):
    """Test auto_verification on a missing file."""
    node.io.check(ArchiveFileCopy.get(id=1))

    # Job is queued
    assert queue.qsize == 1

    # Mock the check async, which is called by the task to do the heavy lifting
    async_mock = MagicMock()
    with patch("alpenhorn.io._default_asyncs.check_async", async_mock):
        # Run task
        task, key = queue.get()
        task()
        queue.task_done(key)

    # Mock was not called (task realised early that the file was missing)
    async_mock.assert_not_called()

    # File has been marked as missing
    assert ArchiveFileCopy.get(id=1).has_file == "N"


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restored",
    }
)
def test_check_ready_restored(xfs, queue, node, mock_lfs):
    """Test check on a restored, ready file."""

    # Restored file to check
    xfs.create_file("/node/simpleacq/file1")
    copy = ArchiveFileCopy.get(id=1)
    copy.has_file = "M"
    copy.ready = True
    copy.save()

    node.io.check(copy)

    # Job is queued
    assert queue.qsize == 1

    async_mock = MagicMock()
    with patch("alpenhorn.io._default_asyncs.check_async", async_mock):
        # Run task
        task, key = queue.get()
        task()
        queue.task_done(key)

    async_mock.assert_called_once()

    # File is still restored
    assert ArchiveFileCopy.get(id=1).ready
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORED


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
    }
)
@pytest.mark.lfs_hsm_restore_result("restore")
def test_check_released(xfs, queue, mock_lfs, node):
    """Test check on a non-ready, released file."""

    xfs.create_file("/node/simpleacq/file1")
    copy = ArchiveFileCopy.get(id=1)
    copy.ready = False
    copy.save()
    node.io.check(copy)

    # Task in queue
    assert queue.qsize == 1

    # Mock the check async, which is called by the task to do the heavy lifting
    async_mock = MagicMock()
    with patch("alpenhorn.io._default_asyncs.check_async", async_mock):
        # Run task
        task, key = queue.get()
        task()
        queue.task_done(key)

        # Task is now deferred
        assert queue.deferred_size == 1
        assert queue.qsize == 0

        # Calling check again doesn't add another task
        node.io.check(copy)
        assert queue.qsize == 0

        # Don't wait for the deferral to expire, just run the task again
        task()

    async_mock.assert_called_once()

    # File has been re-released
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RELEASED


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
    }
)
@pytest.mark.lfs_hsm_restore_result("restore")
def test_check_ready_released(xfs, queue, mock_lfs, node):
    """Test check on a ready, released file."""

    xfs.create_file("/node/simpleacq/file1")

    copy = ArchiveFileCopy.get(id=1)
    copy.ready = True
    copy.save()
    node.io.check(copy)

    # Task in queue
    assert queue.qsize == 1

    # Mock the check async, which is called by the task to do the heavy lifting
    async_mock = MagicMock()
    with patch("alpenhorn.io._default_asyncs.check_async", async_mock):
        # Run task
        task, key = queue.get()
        task()
        queue.task_done(key)

        # Task is now deferred
        assert queue.deferred_size == 1

        # Don't wait for the deferral to expire, just run the task again
        task()

    async_mock.assert_called_once()

    # File has been restored
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORED


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restored",
        "/node/simpleacq/file2": "released",
        "/node/simpleacq/file3": "unarchived",
        "/node/simpleacq/file4": "missing",
        "/node/simpleacq/file5": "restoring",
    }
)
def test_ready_path(mock_lfs, node):
    """Test LustreHSMNodeIO.ready_path."""

    # Return indicates readiness before recall
    assert node.io.ready_path("/node/simpleacq/file1")
    assert not node.io.ready_path("/node/simpleacq/file2")
    assert node.io.ready_path("/node/simpleacq/file3")
    assert not node.io.ready_path("/node/simpleacq/file4")
    assert not node.io.ready_path("/node/simpleacq/file5")

    # But now released file is recalled.
    lfs = mock_lfs("")
    assert lfs.hsm_state("/node/simpleacq/file1") == lfs.HSM_RESTORED
    assert lfs.hsm_state("/node/simpleacq/file2") == lfs.HSM_RESTORING
    assert lfs.hsm_state("/node/simpleacq/file3") == lfs.HSM_UNARCHIVED
    assert lfs.hsm_state("/node/simpleacq/file4") == lfs.HSM_MISSING
    assert lfs.hsm_state("/node/simpleacq/file5") == lfs.HSM_RESTORING


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restored",
    }
)
def test_ready_pull_restored(mock_lfs, node, queue, archivefilecopyrequest):
    """Test LustreHSMNodeIO.ready_pull on a restored file that isn't ready."""

    before = pw.utcnow().replace(microsecond=0)

    copy = ArchiveFileCopy.get(id=1)
    copy.ready = False
    copy.save()
    afcr = archivefilecopyrequest(
        file=copy.file, node_from=node.db, group_to=node.db.group
    )

    node.io.ready_pull(afcr)

    # Task in queue
    assert queue.qsize == 1

    # Run task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # File is ready
    assert ArchiveFileCopy.get(id=1).ready
    assert ArchiveFileCopy.get(id=1).last_update >= before

    # File is restored
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORED


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restoring",
    }
)
def test_ready_pull_restoring(mock_lfs, node, queue, archivefilecopyrequest):
    """Test LustreHSMNodeIO.ready_pull on file already being restored."""

    copy = ArchiveFileCopy.get(id=1)
    copy.ready = False
    copy.save()
    afcr = archivefilecopyrequest(
        file=copy.file, node_from=node.db, group_to=node.db.group
    )

    node.io.ready_pull(afcr)

    # Task in queue
    assert queue.qsize == 1

    # Run task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # File is still not ready
    assert not ArchiveFileCopy.get(id=1).ready

    # File is still being restored
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORING


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
    }
)
def test_ready_pull_released(mock_lfs, node, queue, archivefilecopyrequest):
    """Test LustreHSMNodeIO.ready on a released file that isn't ready."""

    copy = ArchiveFileCopy.get(id=1)
    afcr = archivefilecopyrequest(
        file=copy.file, node_from=node.db, group_to=node.db.group
    )

    node.io.ready_pull(afcr)

    # Task in queue
    assert queue.qsize == 1

    # Run task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Task is now deferred
    assert queue.deferred_size == 1
    assert queue.qsize == 0

    # Calling ready_pull again doesn't add another task
    node.io.ready_pull(afcr)
    assert queue.qsize == 0

    # Don't wait for the deferral to expire, just run the task again
    task()

    # File is being restored
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORING


def test_idle_update_empty(queue, mock_lfs, node):
    """Test LustreHSMNodeIO.idle_update with no files."""

    # Delete all copies
    ArchiveFileCopy.update(has_file="N").execute()

    node.io.idle_update(False)

    # QW has not been initialised
    assert node.io._statecheck_qw is None

    # No item in queue
    assert queue.qsize == 0


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
        "/node/simpleacq/file2": "restored",
        "/node/simpleacq/file3": "unarchived",
        "/node/simpleacq/file4": "missing",
        "/node/simpleacq/file5": "restoring",
    }
)
def test_idle_update_ready(xfs, queue, mock_lfs, node):
    """Test LustreHSMNodeIO.idle_update with copies ready"""

    before = pw.utcnow().replace(microsecond=0)

    node.io.idle_update(False)

    # QW has been initialised
    assert node.io._statecheck_qw is not None

    # Item in queue
    assert queue.qsize == 1

    # Run the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # check readiness
    assert not ArchiveFileCopy.get(id=1).ready
    assert ArchiveFileCopy.get(id=1).last_update >= before

    # These haven't changed
    assert ArchiveFileCopy.get(id=2).ready
    assert ArchiveFileCopy.get(id=3).ready

    # Copy four is no longer on node
    assert not ArchiveFileCopy.get(id=4).ready
    assert ArchiveFileCopy.get(id=4).last_update >= before
    assert ArchiveFileCopy.get(id=4).has_file == "N"

    # Copy five is not ready (being restored)
    assert not ArchiveFileCopy.get(id=5).ready


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
        "/node/simpleacq/file2": "restored",
        "/node/simpleacq/file3": "unarchived",
        "/node/simpleacq/file4": "missing",
        "/node/simpleacq/file5": "restoring",
    }
)
def test_idle_update_not_ready(xfs, queue, mock_lfs, node):
    """Test LustreHSMNodeIO.idle_update with copies not ready"""

    before = pw.utcnow().replace(microsecond=0)

    # Update all copies
    ArchiveFileCopy.update(ready=False).execute()

    node.io.idle_update(False)

    # QW has been initialised
    assert node.io._statecheck_qw is not None

    # Item in queue
    assert queue.qsize == 1

    # Run the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # check readiness
    assert not ArchiveFileCopy.get(id=1).ready

    assert ArchiveFileCopy.get(id=2).ready
    assert ArchiveFileCopy.get(id=2).last_update >= before

    assert ArchiveFileCopy.get(id=3).ready
    assert ArchiveFileCopy.get(id=3).last_update >= before

    # Copy four is no longer on node
    assert not ArchiveFileCopy.get(id=4).ready
    assert ArchiveFileCopy.get(id=4).last_update >= before
    assert ArchiveFileCopy.get(id=4).has_file == "N"

    # Copy five is not ready (being restored)
    assert not ArchiveFileCopy.get(id=5).ready


@pytest.mark.lfs_hsm_state({"/node/simpleacq/file1": "released"})
@pytest.mark.lfs_hsm_restore_result("wait")
def test_hsm_restore_twice(xfs, queue, mock_lfs, node):
    """Test that only one restore request is made."""

    # File is not ready, and maybe corrupt
    xfs.create_file("/node/simpleacq/file1")
    copy = ArchiveFileCopy.get(id=1)
    copy.has_file = "M"
    copy.ready = False
    copy.save()

    # Task will restore the file
    node.io.check(copy)

    # One item in queue now
    assert queue.qsize == 1

    # Run the task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Task has been requeued and is deferred
    assert queue.deferred_size == 1
    assert queue.qsize == 0

    # Check the internal bookkeeping
    assert copy.id in node.io._restoring
    assert copy.id in node.io._restore_start

    # Try to add another task
    node.io.check(copy)

    # Second attempt shouldn't make a new task
    # because the first one added copy.id to the
    # list of in-progress restores
    assert queue.deferred_size == 1
    assert queue.qsize == 0


@pytest.mark.lfs_hsm_state({"/node/simpleacq/file1": "released"})
@pytest.mark.lfs_hsm_restore_result("timeout")
def test_hsm_restore_timeout(xfs, queue, mock_lfs, node):
    """Test handling of timeout in hsm_restore"""

    # File is not ready, and maybe corrupt
    xfs.create_file("/node/simpleacq/file1")
    copy = ArchiveFileCopy.get(id=1)
    copy.has_file = "M"
    copy.ready = False
    copy.save()

    # Task will restore the file
    node.io.check(copy)

    # One item in queue now
    assert queue.qsize == 1

    # Run the task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Task has been requeued and is deferred
    assert queue.deferred_size == 1
    assert queue.qsize == 0

    # Check the internal bookkeeping
    assert copy.id in node.io._restoring
    assert copy.id in node.io._restore_start


@pytest.mark.lfs_hsm_state({"/node/simpleacq/file1": "released"})
@pytest.mark.lfs_hsm_restore_result("fail")
def test_hsm_restore_fail(xfs, queue, mock_lfs, node):
    """Test handling of hsm_restore failure"""

    # File is not ready, and maybe corrupt
    xfs.create_file("/node/simpleacq/file1")
    copy = ArchiveFileCopy.get(id=1)
    copy.has_file = "M"
    copy.ready = False
    copy.save()

    # Task will restore the file
    node.io.check(copy)

    # One item in queue now
    assert queue.qsize == 1

    # Run the task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Task has been abandonned
    assert queue.deferred_size == 0
    assert queue.qsize == 0

    # Check the internal bookkeeping
    assert copy.id not in node.io._restoring
    assert copy.id not in node.io._restore_start
