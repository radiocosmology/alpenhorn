"""Test NearlineNodeIO."""

import pytest
from unittest.mock import MagicMock

from alpenhorn.archive import ArchiveFileCopy
from alpenhorn.io.Nearline import NearlineNodeIO


@pytest.fixture
def node(
    mock_lfs,
    queue,
    genericnode,
    genericacq,
    genericfiletype,
    archivefile,
    archivefilecopy,
):
    """A Nearline node for testing with some stuff on it"""
    genericnode.io_class = "Nearline"

    # Fixed quota provided here is only used to detemine headroom
    # which will get set to (41000 / 4) = 10250 kiB
    genericnode.io_config = (
        '{"quota_group": "qgroup", "fixed_quota": 41000, "release_check_count": 6}'
    )

    # Some files
    files = [
        archivefile(name="file1", acq=genericacq, type=genericfiletype, size_b=100000),
        archivefile(name="file2", acq=genericacq, type=genericfiletype, size_b=300000),
        archivefile(name="file3", acq=genericacq, type=genericfiletype, size_b=400000),
        archivefile(name="file4", acq=genericacq, type=genericfiletype, size_b=800000),
        archivefile(name="file5", acq=genericacq, type=genericfiletype, size_b=50000),
        archivefile(name="file6", acq=genericacq, type=genericfiletype, size_b=300000),
    ]

    # Some copies
    last_updates = [3, 1, 5, 6, 2, 4]
    for num, file in enumerate(files):
        archivefilecopy(
            file=file, node=genericnode, has_file="Y", size_b=10, ready=True
        )
        # We need to do it this way to set last_update
        ArchiveFileCopy.update(last_update=last_updates[num]).where(
            ArchiveFileCopy.id == num + 1
        ).execute()

    # Init node I/O
    genericnode.io.set_queue(queue)

    return genericnode


def test_ioconfig(genericnode, have_lfs):
    """Test instantiating I/O without necessary ioconfig."""

    genericnode.io_class = "Nearline"

    with pytest.raises(KeyError):
        genericnode.io

    genericnode.io_config = '{"quota_group": "qgroup"}'

    with pytest.raises(KeyError):
        genericnode.io

    # Check bad release_check_count
    genericnode.io_config = (
        '{"quota_group": "qgroup", "fixed_quota": 300000, "release_check_count": -1}'
    )

    with pytest.raises(ValueError):
        genericnode.io

    genericnode.io_config = '{"quota_group": "qgroup", "fixed_quota": 300000}'

    # Now it works
    genericnode.io


@pytest.mark.lfs_quota_remaining(20000000)
def test_release_files_okay(queue, node):
    """Test running release_files when we're under headroom"""

    node.io.release_files()

    # Shouldn't be anything in the queue
    assert queue.qsize == 0


@pytest.mark.lfs_quota_remaining(10000000)
@pytest.mark.lfs_hsm_state(
    {
        "/node/genericacq/file1": "restored",
        "/node/genericacq/file2": "restored",
        "/node/genericacq/file3": "restored",
        "/node/genericacq/file4": "restored",
        "/node/genericacq/file5": "restored",
        "/node/genericacq/file6": "unarchived",
    }
)
def test_release_files(queue, mock_lfs, node):
    """Test running release_files."""

    node.io.release_files()

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
    assert not ArchiveFileCopy.get(id=2).ready
    assert not ArchiveFileCopy.get(id=3).ready
    assert ArchiveFileCopy.get(id=4).ready
    assert not ArchiveFileCopy.get(id=5).ready
    assert ArchiveFileCopy.get(id=6).ready

    # Check hsm_relase was actually called
    lfs = mock_lfs("")
    assert lfs.hsm_state("/node/genericacq/file1") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/genericacq/file2") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/genericacq/file3") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/genericacq/file4") == lfs.HSM_RESTORED
    assert lfs.hsm_state("/node/genericacq/file5") == lfs.HSM_RELEASED
    assert lfs.hsm_state("/node/genericacq/file6") == lfs.HSM_UNARCHIVED


@pytest.mark.lfs_quota_remaining(10000000)
def test_before_update(queue, node):
    """Test NearlineNodeIO.before_update()"""

    # When not idle, the release_files task is not run
    node.io.before_update(idle=False)

    assert queue.qsize == 0

    # But when idle it should run
    node.io.before_update(idle=True)

    assert queue.qsize == 1


def test_filesize(xfs, node):
    """Test NearlineNodeIO.filesize(), which always returns st_size"""

    xfs.create_file("/node/genericacq/file1", st_size=100000)
    assert node.io.filesize("genericacq/file1") == 100000


@pytest.mark.lfs_hsm_state(
    {
        "/node/genericacq/file1": "released",
    }
)
def test_check_released(queue, mock_lfs, node):
    """Test NearlineNodeIO.check() on a released file."""

    copy = ArchiveFileCopy.get(id=1)
    copy.ready = False

    node.io.check(copy)

    # Queue is empty
    assert queue.qsize == 0

    # File has been restored
    assert not mock_lfs("").hsm_released(copy.path)


@pytest.mark.lfs_hsm_state(
    {
        "/node/genericacq/file1": "restored",
    }
)
def test_check_restored(queue, mock_lfs, node):
    """Test NearlineNodeIO.check() on a restored file."""
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
        "/node/genericacq/file1": "restored",
    }
)
def test_auto_verify_restored(xfs, node):
    """Test auto_verification on a restored file."""

    # Mock the Default IO's check function which we don't need to test here
    from alpenhorn.io.Default import DefaultNodeIO

    DefaultNodeIO.check = MagicMock()

    xfs.create_file("/node/genericacq/file1")
    copy = ArchiveFileCopy.get(id=1)

    node.io.auto_verify(copy)
    DefaultNodeIO.check.assert_called_once_with(copy)


@pytest.mark.lfs_hsm_state(
    {
        "/node/genericacq/file1": "released",
    }
)
def test_auto_verify_released(xfs, queue, mock_lfs, node):
    """Test auto_verification on a released file."""

    # Mock the Default IO's check function which we don't need to test here
    from alpenhorn.io import _default_asyncs

    _default_asyncs.check_async = MagicMock()

    xfs.create_file("/node/genericacq/file1")

    copy = ArchiveFileCopy.get(id=1)
    copy.ready = False
    copy.save()
    node.io.auto_verify(copy)

    # Task in queue
    assert queue.qsize == 1

    # Run task
    task, key = queue.get()
    task()
    queue.task_done(key)

    _default_asyncs.check_async.assert_called_once()

    # File has been re-released
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RELEASED


@pytest.mark.lfs_hsm_state(
    {
        "/node/genericacq/file1": "released",
    }
)
def test_auto_verify_ready_released(xfs, queue, mock_lfs, node):
    """Test auto_verification on a released file that's ready."""

    # Mock the Default IO's check function which we don't need to test here
    from alpenhorn.io import _default_asyncs

    _default_asyncs.check_async = MagicMock()

    xfs.create_file("/node/genericacq/file1")

    copy = ArchiveFileCopy.get(id=1)
    node.io.auto_verify(copy)

    # Task in queue
    assert queue.qsize == 1

    # Run task
    task, key = queue.get()
    task()
    queue.task_done(key)

    _default_asyncs.check_async.assert_called_once()

    # File has _not_ been re-released
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORED


@pytest.mark.lfs_hsm_state(
    {
        "/node/genericacq/file1": "restored",
    }
)
def test_ready_restored(mock_lfs, node, archivefilecopyrequest):
    """Test NearlineNodeIO.ready on a restored file that isn't ready."""

    copy = ArchiveFileCopy.get(id=1)
    copy.ready = False
    copy.save()
    afcr = archivefilecopyrequest(file=copy.file, node_from=node, group_to=node.group)

    node.io.ready(afcr)

    # File is ready
    assert ArchiveFileCopy.get(id=1).ready

    # File is restored
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORED


@pytest.mark.lfs_hsm_state(
    {
        "/node/genericacq/file1": "released",
    }
)
def test_ready_restored(mock_lfs, node, archivefilecopyrequest):
    """Test NearlineNodeIO.ready on a restored file that isn't ready."""

    copy = ArchiveFileCopy.get(id=1)
    afcr = archivefilecopyrequest(file=copy.file, node_from=node, group_to=node.group)

    node.io.ready(afcr)

    # File is not ready (because ready is set before hsm_restore is called)
    assert not ArchiveFileCopy.get(id=1).ready

    # File is restored
    lfs = mock_lfs("")
    assert lfs.hsm_state(copy.path) == lfs.HSM_RESTORED


def test_idle_update_empty(queue, mock_lfs, node):
    """Test NearlineNodeIO.idle_update with no files."""

    # Delete all copies
    ArchiveFileCopy.update(has_file="N").execute()

    node.io.idle_update()

    # QW has not been initialised
    assert node.id not in NearlineNodeIO._release_qw

    # No item in queue
    assert queue.qsize == 0


@pytest.mark.lfs_hsm_state(
    {
        "/node/genericacq/file1": "released",
        "/node/genericacq/file2": "restored",
        "/node/genericacq/file3": "unarchived",
        "/node/genericacq/file4": "missing",
    }
)
def test_idle_update_ready(xfs, queue, mock_lfs, node):
    """Test NearlineNodeIO.idle_update with copies ready"""

    node.io.idle_update()

    # QW has been initialised
    assert node.id in NearlineNodeIO._release_qw

    # Item in queue
    assert queue.qsize == 1

    # Run the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # check readiness
    assert not ArchiveFileCopy.get(id=1).ready
    assert ArchiveFileCopy.get(id=2).ready
    assert ArchiveFileCopy.get(id=3).ready
    assert not ArchiveFileCopy.get(id=4).ready

    # Copy four is no longer on node
    assert ArchiveFileCopy.get(id=4).has_file == "N"


@pytest.mark.lfs_hsm_state(
    {
        "/node/genericacq/file1": "released",
        "/node/genericacq/file2": "restored",
        "/node/genericacq/file3": "unarchived",
        "/node/genericacq/file4": "missing",
    }
)
def test_idle_update_not_ready(xfs, queue, mock_lfs, node):
    """Test NearlineNodeIO.idle_update with copies not ready"""

    # Update all copies
    ArchiveFileCopy.update(ready=False).execute()

    node.io.idle_update()

    # QW has been initialised
    assert node.id in NearlineNodeIO._release_qw

    # Item in queue
    assert queue.qsize == 1

    # Run the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # check readiness
    assert not ArchiveFileCopy.get(id=1).ready
    assert ArchiveFileCopy.get(id=2).ready
    assert ArchiveFileCopy.get(id=3).ready
    assert not ArchiveFileCopy.get(id=4).ready

    # Copy four is no longer on node
    assert ArchiveFileCopy.get(id=4).has_file == "N"
