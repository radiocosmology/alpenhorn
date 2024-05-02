"""Test LustreHSMNodeIO."""

import pytest
import datetime
from unittest.mock import patch, MagicMock

from alpenhorn.archive import ArchiveFileCopy
from alpenhorn.update import UpdateableNode


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
        '{"quota_group": "qgroup", "headroom": 10250, "release_check_count": 6}'
    )

    # Some files
    files = [
        archivefile(name="file1", acq=simpleacq, size_b=100000),
        archivefile(name="file2", acq=simpleacq, size_b=300000),
        archivefile(name="file3", acq=simpleacq, size_b=400000),
        archivefile(name="file4", acq=simpleacq, size_b=800000),
        archivefile(name="file5", acq=simpleacq, size_b=50000),
        archivefile(name="file6", acq=simpleacq, size_b=300000),
    ]

    # Some copies
    last_updates = [3, 1, 5, 6, 2, 4]
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
    """Check for bad realse_check_count."""

    simplenode.io_class = "LustreHSM"
    simplenode.io_config = (
        '{"quota_group": "qgroup", "headroom": 300000, "release_check_count": -1}'
    )

    with pytest.raises(ValueError):
        UpdateableNode(None, simplenode)


@pytest.mark.lfs_quota_remaining(20000000)
def test_release_files_okay(queue, node):
    """Test running release_files when we're under headroom"""

    node.io.release_files()

    # Shouldn't be anything in the queue
    assert queue.qsize == 0


@pytest.mark.lfs_quota_remaining(10000000)
@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restored",
        "/node/simpleacq/file2": "restored",
        "/node/simpleacq/file3": "restored",
        "/node/simpleacq/file4": "restored",
        "/node/simpleacq/file5": "restored",
        "/node/simpleacq/file6": "unarchived",
    }
)
def test_release_files(queue, mock_lfs, node):
    """Test running release_files."""

    node.io.release_files()

    before = datetime.datetime.utcnow().replace(microsecond=0)

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
    #  - file3: 400 kB  [ last_update = 5 ]
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

    assert ArchiveFileCopy.get(id=6).ready

    # Check hsm_relase was actually called
    lfs = mock_lfs("")
    assert lfs.hsm_state("/node/simpleacq/file1") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/simpleacq/file2") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/simpleacq/file3") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/simpleacq/file4") == lfs.HSM_RESTORED
    assert lfs.hsm_state("/node/simpleacq/file5") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/simpleacq/file6") == lfs.HSM_UNARCHIVED


@pytest.mark.lfs_quota_remaining(10000000)
def test_before_update(queue, node):
    """Test LustreHSMNodeIO.before_update()"""

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
    }
)
def test_open_binary(xfs, node):
    """Test binary LustreHSMNodeIO.open()"""

    xfs.create_file("/node/dir/file2", contents="file contents")

    with pytest.raises(OSError):
        node.io.open("dir/file1", binary=True)

    with node.io.open("dir/file2", binary=True) as f:
        assert f.read() == b"file contents"


@pytest.mark.lfs_hsm_state(
    {
        "/node/dir/file1": "released",
        "/node/dir/file2": "restored",
    }
)
def test_open_text(xfs, node):
    """Test text LustreHSMNodeIO.open()"""

    xfs.create_file("/node/dir/file2", contents="file contents")

    with pytest.raises(OSError):
        node.io.open("dir/file1", binary=False)

    with node.io.open("dir/file2", binary=False) as f:
        assert f.read() == "file contents"


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
    }
)
def test_check_released(queue, mock_lfs, node):
    """Test LustreHSMNodeIO.check() on a released file."""

    copy = ArchiveFileCopy.get(id=1)
    copy.ready = False

    node.io.check(copy)

    # Queue is empty
    assert queue.qsize == 0

    # File has been restored
    assert not mock_lfs("").hsm_released(copy.path)


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restored",
    }
)
def test_check_restored(queue, mock_lfs, node):
    """Test LustreHSMNodeIO.check() on a restored file."""
    node.io.check(ArchiveFileCopy.get(id=1))

    # Task has been created
    assert queue.qsize == 1


def test_auto_verify_missing(queue, node):
    """Test auto_verification on a missing file."""
    node.io.auto_verify(ArchiveFileCopy.get(id=1))

    # Queue is empty
    assert queue.qsize == 0

    # File has been marked as missing
    assert ArchiveFileCopy.get(id=1).has_file == "N"


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restored",
    }
)
def test_auto_verify_restored(xfs, node):
    """Test auto_verification on a restored file."""

    # When a file is restored, LustreHSM.auto_verify just tail-calls DefaultIO.check
    check_mock = MagicMock()

    xfs.create_file("/node/simpleacq/file1")
    copy = ArchiveFileCopy.get(id=1)

    with patch("alpenhorn.io.default.DefaultNodeIO.check", check_mock):
        node.io.auto_verify(copy)
    check_mock.assert_called_once_with(copy)


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
    }
)
def test_auto_verify_released(xfs, queue, mock_lfs, node):
    """Test auto_verification on a released file."""

    # Mock the check async, which is called by the task to do the heavy lifting
    async_mock = MagicMock()

    xfs.create_file("/node/simpleacq/file1")

    copy = ArchiveFileCopy.get(id=1)
    copy.ready = False
    copy.save()
    node.io.auto_verify(copy)

    # Task in queue
    assert queue.qsize == 1

    # Run task
    with patch("alpenhorn.io.lustrehsm.check_async", async_mock):
        task, key = queue.get()
        task()
        queue.task_done(key)

    async_mock.assert_called_once()

    # File has been re-released
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RELEASED


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
    }
)
def test_auto_verify_ready_released(xfs, queue, mock_lfs, node):
    """Test auto_verification on a released file that's ready."""

    # Mock the check async, which is called by the task to do the heavy lifting
    async_mock = MagicMock()

    xfs.create_file("/node/simpleacq/file1")

    copy = ArchiveFileCopy.get(id=1)
    node.io.auto_verify(copy)

    # Task in queue
    assert queue.qsize == 1

    # Run task
    with patch("alpenhorn.io.lustrehsm.check_async", async_mock):
        task, key = queue.get()
        task()
        queue.task_done(key)

    async_mock.assert_called_once()

    # File has _not_ been re-released
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORED


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restored",
        "/node/simpleacq/file2": "released",
        "/node/simpleacq/file3": "unarchived",
        "/node/simpleacq/file4": "missing",
    }
)
def test_ready_path(mock_lfs, node):
    """Test LustreHSMNodeIO.ready_path."""

    # Return indicates readiness before recall
    assert node.io.ready_path("/node/simpleacq/file1")
    assert not node.io.ready_path("/node/simpleacq/file2")
    assert node.io.ready_path("/node/simpleacq/file3")
    assert not node.io.ready_path("/node/simpleacq/file4")

    # But now released file is recalled.
    lfs = mock_lfs("")
    assert lfs.hsm_state("/node/simpleacq/file1") == lfs.HSM_RESTORED
    assert lfs.hsm_state("/node/simpleacq/file2") == lfs.HSM_RESTORED
    assert lfs.hsm_state("/node/simpleacq/file3") == lfs.HSM_UNARCHIVED
    assert lfs.hsm_state("/node/simpleacq/file4") == lfs.HSM_MISSING


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "restored",
    }
)
def test_ready_pull_restored(mock_lfs, node, archivefilecopyrequest):
    """Test LustreHSMNodeIO.ready_pull on a restored file that isn't ready."""

    before = datetime.datetime.utcnow().replace(microsecond=0)

    copy = ArchiveFileCopy.get(id=1)
    copy.ready = False
    copy.save()
    afcr = archivefilecopyrequest(
        file=copy.file, node_from=node.db, group_to=node.db.group
    )

    node.io.ready_pull(afcr)

    # File is ready
    assert ArchiveFileCopy.get(id=1).ready
    assert ArchiveFileCopy.get(id=1).last_update >= before

    # File is restored
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORED


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
    }
)
def test_ready_pull_released(mock_lfs, node, archivefilecopyrequest):
    """Test LustreHSMNodeIO.ready on a released file that isn't ready."""

    copy = ArchiveFileCopy.get(id=1)
    afcr = archivefilecopyrequest(
        file=copy.file, node_from=node.db, group_to=node.db.group
    )

    node.io.ready_pull(afcr)

    # File is not ready (because restore hasn't been verified)
    assert not ArchiveFileCopy.get(id=1).ready

    # File is restored
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORED


def test_idle_update_empty(queue, mock_lfs, node):
    """Test LustreHSMNodeIO.idle_update with no files."""

    # Delete all copies
    ArchiveFileCopy.update(has_file="N").execute()

    node.io.idle_update()

    # QW has not been initialised
    assert node.io._release_qw is None

    # No item in queue
    assert queue.qsize == 0


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
        "/node/simpleacq/file2": "restored",
        "/node/simpleacq/file3": "unarchived",
        "/node/simpleacq/file4": "missing",
    }
)
def test_idle_update_ready(xfs, queue, mock_lfs, node):
    """Test LustreHSMNodeIO.idle_update with copies ready"""

    before = datetime.datetime.utcnow().replace(microsecond=0)

    node.io.idle_update()

    # QW has been initialised
    assert node.io._release_qw is not None

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


@pytest.mark.lfs_hsm_state(
    {
        "/node/simpleacq/file1": "released",
        "/node/simpleacq/file2": "restored",
        "/node/simpleacq/file3": "unarchived",
        "/node/simpleacq/file4": "missing",
    }
)
def test_idle_update_not_ready(xfs, queue, mock_lfs, node):
    """Test LustreHSMNodeIO.idle_update with copies not ready"""

    before = datetime.datetime.utcnow().replace(microsecond=0)

    # Update all copies
    ArchiveFileCopy.update(ready=False).execute()

    node.io.idle_update()

    # QW has been initialised
    assert node.io._release_qw is not None

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
