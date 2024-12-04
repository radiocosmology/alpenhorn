"""Test DefaultNodeIO.pull()"""

import os
import pathlib
from unittest.mock import MagicMock, patch

import pytest

from alpenhorn.daemon.update import UpdateableNode
from alpenhorn.db.archive import ArchiveFileCopy, ArchiveFileCopyRequest


@pytest.fixture
def have_bbcp(mock_run_command):
    """Pretend to have bbcp

    Mocks shutil.which to indicate bbcp is present.

    Also mocks util.run_command to emulate running bbcp.  Use the
    run_command_result marker to specify the result of running bbcp."""

    from shutil import which as real_which

    def _mocked_which(cmd, mode=os.F_OK | os.X_OK, path=None):
        nonlocal real_which
        if cmd == "bbcp":
            return "BBCP"
        return real_which(cmd, mode, path)

    with patch("shutil.which", _mocked_which):
        yield mock_run_command


@pytest.fixture
def have_rsync(mock_run_command):
    """Pretend to have rsync

    Mocks shutil.which to indicate rsync is present.

    Also mocks util.run_command to emulate running rsync.  Use the
    run_command_result marker to specify the result of running rsync."""

    from shutil import which as real_which

    def _mocked_which(cmd, mode=os.F_OK | os.X_OK, path=None):
        nonlocal real_which
        if cmd == "rsync":
            return "RSYNC"
        return real_which(cmd, mode, path)

    with patch("shutil.which", _mocked_which):
        yield mock_run_command


@pytest.fixture
def test_req(
    xfs,
    hostname,
    queue,
    storagegroup,
    storagenode,
    simplefile,
    archivefilecopy,
    archivefilecopyrequest,
):
    """Create a test ArchiveFileCopyRequest.

    Returns a two-element tuple: (node_to, copy_request)"""

    group_to = storagegroup(name="group_to")
    node_to = UpdateableNode(
        queue,
        storagenode(name="node_to", group=group_to, host=hostname, root="/node_to"),
    )
    node_from = storagenode(
        name="node_from",
        group=storagegroup(name="group_from"),
        root="/node_from",
        host=hostname,  # By default the transfer is local
        username="user",
        address="addr",
    )

    copy = archivefilecopy(file=simplefile, node=node_from, has_file="Y")

    # Create source file and dest root
    xfs.create_dir("/node_to")
    xfs.create_file(copy.path, st_size=copy.size_b)
    return (
        node_to,
        archivefilecopyrequest(file=simplefile, node_from=node_from, group_to=group_to),
    )


@pytest.fixture
def pull_async(dbtables, test_req):
    """Put a pull_async Task on the queue.

    Returns a two-element tuple: (node_to, copy_request)"""
    node, req = test_req
    node.io.pull(req)

    return test_req


def test_pull_sync_undermin(queue, test_req):
    """test DefaultNodeIO.pull synchronous under_min check"""

    node, req = test_req

    # set up node to fail under_min check
    node.db.avail_gb = 1.0
    node.db.min_avail_gb = 2.0
    node.io.pull(req)

    # No job should be queued and req isn't resolved.
    assert queue.qsize == 0
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False


def test_pull_sync_overmax(queue, test_req, archivefile, archivefilecopy):
    """test DefaultNodeIO.pull synchronous over_max check"""

    # Init I/O layer
    node, req = test_req

    # set up node to fail over_max check
    file = archivefile(name="file2", acq=req.file.acq, size_b=100000)
    archivefilecopy(file=file, node=node.db, has_file="Y")
    node.db.max_total_gb = file.size_b / 2**31  # i.e. half of file.size_b
    node.io.pull(req)

    # No job should be queued and req isn't resolved.
    assert queue.qsize == 0
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False


def test_pull_sync_fit(xfs, queue, test_req):
    """test DefaultNodeIO.pull synchronous fit check"""

    node, req = test_req

    # set up node to fail reserve_bytes check
    xfs.set_disk_usage(10000)
    node.io.reserve_bytes(4000)
    node.io.pull(req)

    # No job should be queued and req isn't resolved.
    assert queue.qsize == 0
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False


def test_pull_async_noroute(queue, pull_async):
    """Test no route for remote pull."""

    node, req = pull_async

    # Make the request non-local
    req.node_from.host = "other-host"

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Req isn't resolved.
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False

    # Source is not being re-checked
    assert ArchiveFileCopy.get(node=req.node_from, file=req.file).has_file != "M"


@pytest.mark.run_command_result(1, "", "bbcp_stderr")
def test_pull_async_bbcp_fail(queue, have_bbcp, pull_async):
    """Test an unsuccessful bbcp remote pull."""

    node, req = pull_async

    # Make the request non-local
    req.node_from.host = "other-host"

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Verify that bbcp ran
    assert "bbcp" in have_bbcp()["cmd"]

    # Req isn't resolved.
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False

    # Source is being re-checked
    assert ArchiveFileCopy.get(node=req.node_from, file=req.file).has_file == "M"


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
def test_pull_async_bbcp_succeed(queue, have_bbcp, mock_filesize, pull_async):
    """Test a successful bbcp remote pull."""

    node, req = pull_async

    # Make the request non-local
    req.node_from.host = "other-host"

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Req is complete.
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is True
    assert afcr.cancelled is False

    # Verify that bbcp ran
    assert "bbcp" in have_bbcp()["cmd"]

    # Target copy exists
    afc = ArchiveFileCopy.get(node=node.db, file=req.file)
    assert afc.has_file == "Y"

    # Source is not being re-checked
    assert ArchiveFileCopy.get(node=req.node_from, file=req.file).has_file != "M"


@pytest.mark.run_command_result(1, "", "rsync_stderr")
def test_pull_async_remote_rsync_fail(queue, have_rsync, pull_async):
    """Test an unsuccessful rsync remote pull."""

    node, req = pull_async

    # Make the request non-local
    req.node_from.host = "other-host"

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Verify that rsync ran
    assert "rsync" in have_rsync()["cmd"]

    # Req isn't resolved.
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False

    # Source is being re-checked
    assert ArchiveFileCopy.get(node=req.node_from, file=req.file).has_file == "M"


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
def test_pull_async_remote_rsync_succeed(queue, have_rsync, mock_filesize, pull_async):
    """Test a successful rsync remote pull."""

    node, req = pull_async

    # Make the request non-local
    req.node_from.host = "other-host"

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Verify that rsync ran
    assert "rsync" in have_rsync()["cmd"]

    # Req is complete.
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is True
    assert afcr.cancelled is False

    # Target copy exists
    afc = ArchiveFileCopy.get(node=node.db, file=req.file)
    assert afc.has_file == "Y"

    # Source is not being re-checked
    assert ArchiveFileCopy.get(node=req.node_from, file=req.file).has_file != "M"


def test_pull_async_remote_nomethod(queue, pull_async):
    """Test remote pull with no command available."""

    node, req = pull_async

    # Make the request non-local
    req.node_from.host = "other-host"

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Req isn't resolved.
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False

    # Source is not being re-checked
    assert ArchiveFileCopy.get(node=req.node_from, file=req.file).has_file != "M"


def test_pull_async_link(queue, pull_async):
    """Test creating hardlink."""

    node, req = pull_async

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Req is resolved after successful hardlink
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is True
    assert afcr.cancelled is False

    # Target copy exists
    afc = ArchiveFileCopy.get(node=node.db, file=req.file)
    assert afc.has_file == "Y"

    # Source is not being re-checked
    assert ArchiveFileCopy.get(node=req.node_from, file=req.file).has_file != "M"


def test_pull_async_link_arccontam(queue, pull_async):
    """Test not creating hardlinks between archive nodes."""

    node, req = pull_async

    # Make one node non-archival
    node.db.storage_type = "F"

    # Call the async
    task, key = queue.get()

    # Mock local_copy.  We'll end up falling back on this
    # due to no other copy method working
    mock = MagicMock()
    with patch("alpenhorn.io.ioutil.local_copy", mock):
        task()
    queue.task_done(key)

    # Local copy happened.
    mock.assert_called_once()


@pytest.mark.run_command_result(0, "", "stderr")
def test_pull_async_local_rsync_succeed(queue, have_rsync, mock_filesize, pull_async):
    """Test a successful rsync remote pull."""

    node, req = pull_async

    # Make one node non-archival to avoid hardlinking
    node.db.storage_type = "F"

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Verify that rsync ran
    assert "rsync" in have_rsync()["cmd"]

    # Req is complete.
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is True
    assert afcr.cancelled is False

    # Target copy exists
    afc = ArchiveFileCopy.get(node=node.db, file=req.file)
    assert afc.has_file == "Y"

    # Source is not being re-checked
    assert ArchiveFileCopy.get(node=req.node_from, file=req.file).has_file != "M"


@pytest.mark.run_command_result(1, "", "rsync_stderr")
def test_pull_async_local_rsync_fail(queue, have_rsync, pull_async):
    """Test an unsuccessful rsync local pull."""

    node, req = pull_async

    # Make one node non-archival to avoid hardlinking
    node.db.storage_type = "F"

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Verify that rsync ran
    assert "rsync" in have_rsync()["cmd"]

    # Req isn't resolved.
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False

    # Source is being re-checked
    assert ArchiveFileCopy.get(node=req.node_from, file=req.file).has_file == "M"


@pytest.mark.run_command_result(1, "", "rsync_stderr")
def test_pull_fail_unlink(xfs, queue, have_rsync, pull_async):
    """Test failure deleting the destination."""

    node, req = pull_async

    # Destination path
    path = pathlib.Path(node.db.root, req.file.path)

    # Create destination file
    xfs.create_file(path)

    # Verify
    assert path.exists()

    # Force hardlinking to fail
    node.db.storage_type = "T"

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Pull failure
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False

    # File is gone
    assert not path.exists()


def test_pull_already_done(xfs, queue, pull_async, archivefilecopy):
    """Test pulling a file that's already on the node."""

    node, req = pull_async

    # Create the archivefilecopy record
    archivefilecopy(file=req.file, node=node.db, has_file="Y", wants_file="Y")

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Pull cancelled
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is True
