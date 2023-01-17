"""
test_update
------------------

Tests for `alpenhorn.update` module.
"""

import os
import time
import pytest
from unittest.mock import patch, call, MagicMock

from alpenhorn import config, pool, update
from alpenhorn.queue import FairMultiFIFOQueue
from alpenhorn.storage import StorageNode
from alpenhorn.archive import ArchiveFileCopy, ArchiveFileCopyRequest


def make_afcr(
    name,
    filetype,
    acq,
    srcnode,
    srcstate,
    dstgroup,
    dstnode,
    dststate,
    archivefile,
    archivefilecopy,
    archivefilecopyrequest,
):
    """Create an ArchiveFileCopyRequest with ArchiveFileCopies on src and dest.

    Returns a 4-tuple: ArchiveFile, source-ArchiveFileCopy, dest-ArchiveFileCopy, Request
    """
    file = archivefile(name=name, type=filetype, acq=acq)
    srccopy = archivefilecopy(node=srcnode, file=file, has_file=srcstate)
    if dststate is None:
        dstcopy = None
    else:
        dstcopy = archivefilecopy(node=dstnode, file=file, has_file=dststate)
    afcr = archivefilecopyrequest(node_from=srcnode, group_to=dstgroup, file=file)

    return file, srccopy, dstcopy, afcr


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
def mocknode(storagenode, storagegroup):
    """A StorageNode fixture with a mocked io class.

    Access the mock via mocknode.mock."""

    group = storagegroup(name="mockgroup")
    node = storagenode(name="mocknode", group=group, root="/mocknode")
    node.active = True
    node.mock = MagicMock()
    node._io = node.mock

    yield node

    del node.mock
    del node._io


@pytest.fixture
def mockgroupandnode(mocknode):
    """A StorageGroup fixture with a mocked io class.  The
    group's node is mocknode.

    Yields the group and node."""

    group = mocknode.group
    group.mock = MagicMock()
    group._io = group.mock

    yield group, mocknode

    del group.mock
    del group._io


@pytest.fixture
def pull(
    mockgroupandnode,
    simplefiletype,
    simpleacq,
    simplenode,
    archivefile,
    archivefilecopy,
    archivefilecopyrequest,
):
    """A simple ArchiveFileCopyRequest for update_group."""

    # Make source active
    simplenode.active = True
    simplenode.save()

    group, node = mockgroupandnode
    group.mock.before_update = lambda nodes, idle: True

    # File doesn't exist on dest
    group.mock.exists = lambda path: None

    result = make_afcr(
        "file",
        simplefiletype,
        simpleacq,
        simplenode,
        "Y",
        group,
        node,
        None,
        archivefile,
        archivefilecopy,
        archivefilecopyrequest,
    )

    # Don't both returning the dstcopy, which is None
    return result[0], result[1], result[3]


@pytest.fixture
def loopmocks(dbtables):
    """Mock some function calls in the main loop.

    Also ensures the loop runs only once.
    """

    mocks = dict()
    patches = list()

    # Create the mocks
    for f in ["serial_io", "global_abort"]:
        mocks[f] = MagicMock()
        patches.append(patch(f"alpenhorn.update.{f}", mocks[f]))

    # Ensure the main loop only runs once
    waited = False

    def _wait(timeout=None):
        nonlocal waited
        waited = True

    def _is_set():
        nonlocal waited
        return waited

    mocks["global_abort"].wait = _wait
    mocks["global_abort"].is_set = _is_set

    # Start all the mocks
    for p in patches:
        p.start()

    yield mocks

    # Stop all the mocks
    for p in patches:
        p.stop()


def test_update_abort():
    """Test update_loop with global_abort set."""

    # Raise global abort
    pool.global_abort.set()

    # This should do nothing except exit, so passing
    # a bunch of Nones shouldn't be a problem
    update.update_loop(None, None, None)

    # Reset
    pool.global_abort.clear()


def test_update_no_nodes(hostname, dbtables, queue, emptypool, loopmocks):
    """Test update_loop with no active nodes."""

    update.update_loop(hostname, queue, emptypool)

    loopmocks["serial_io"].assert_called_once_with(queue)


def test_update_run(hostname, xfs, simplenode, queue, emptypool, loopmocks):
    """Test update_loop with one active node."""

    # Set up node
    simplenode.host = hostname
    simplenode.active = True
    simplenode.save()
    simplenode.io.set_queue(queue)

    xfs.create_file("/node/ALPENHORN_NODE", contents="simplenode")

    update.update_loop(hostname, queue, emptypool)

    loopmocks["serial_io"].assert_called_once_with(queue)


def test_serial_io(fastqueue):
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


def test_update_group_no_update(mockgroupandnode, hostname, queue):
    """Test update.update_group not running when before_update returns False."""
    group, node = mockgroupandnode

    group.mock.before_update = lambda nodes, idle: False

    # update should not have run
    assert not update.update_group(group, hostname, queue, True)


def test_update_group_inactive(mockgroupandnode, hostname, queue, pull, simplenode):
    """Test running update.update_group with source node inactive."""

    group, node = mockgroupandnode
    file, copy, afcr = pull

    # Source not active
    simplenode.active = False
    simplenode.save()

    # update ran
    assert update.update_group(group, hostname, queue, True)

    # afcr is not handled
    assert not ArchiveFileCopyRequest.get(file=file).completed
    assert not ArchiveFileCopyRequest.get(file=file).cancelled
    assert call.pull(file) not in group.mock.mock_calls


def test_update_group_nosrc(mockgroupandnode, hostname, queue, pull):
    """Test running update.update_group with source file missing."""

    group, node = mockgroupandnode
    file, copy, afcr = pull

    # Source not present
    copy.has_file = "N"
    copy.save()

    # update ran
    assert update.update_group(group, hostname, queue, True)

    # afcr is not handled
    assert not ArchiveFileCopyRequest.get(file=afcr.file).completed
    assert not ArchiveFileCopyRequest.get(file=afcr.file).cancelled
    assert call.pull(afcr) not in group.mock.mock_calls


def test_update_group_notready(mockgroupandnode, hostname, queue, pull):
    """Test running update.update_group with source file not ready."""

    group, node = mockgroupandnode
    file, copy, afcr = pull

    # Force load
    from alpenhorn.io.Default import DefaultNodeRemote

    # Force source not ready
    with patch(
        "alpenhorn.io.Default.DefaultNodeRemote.pull_ready", lambda self, file: False
    ):
        # update ran
        assert update.update_group(group, hostname, queue, True)

    # afcr is not handled
    assert not ArchiveFileCopyRequest.get(file=file).completed
    assert not ArchiveFileCopyRequest.get(file=file).cancelled
    assert call.pull(afcr) not in group.mock.mock_calls


def test_update_group_path_exists(mockgroupandnode, hostname, queue, pull):
    """Test running update.update_group with unexpected existing dest."""

    group, node = mockgroupandnode
    file, copy, afcr = pull

    # File exists on dest
    group.mock.exists = lambda path: node

    # This runs twice, to test two possibilities:
    # - no destination ArchiveFileCopy record
    # - desitination ArchiveFileCopy present with has_file='N'
    # It should behave the same both times
    for missing in [False, True]:
        # update ran
        assert update.update_group(group, hostname, queue, True)

        # Dest file copy needs checking
        dst = ArchiveFileCopy.get(file=file, node=node)
        assert dst.has_file == "M"

        # Request is still pending
        assert not ArchiveFileCopyRequest.get(file=file).completed
        assert not ArchiveFileCopyRequest.get(file=file).cancelled

        # Pull was not executed
        assert call.pull(afcr) not in group.mock.mock_calls

        # Only need to do this the first time
        if missing:
            # "Delete" dest
            dst.has_file = "N"
            dst.save()


def test_update_group_copy_state(
    mockgroupandnode,
    hostname,
    queue,
    simplenode,
    simpleacq,
    simplefiletype,
    archivefile,
    archivefilecopy,
    archivefilecopyrequest,
):
    """Test running update.update_group with different copy states."""

    group, node = mockgroupandnode

    group.mock.before_update = lambda nodes, idle: True

    # Source is active
    simplenode.active = True
    simplenode.save()

    # Make some pull requests
    commonargs = (simplefiletype, simpleacq, simplenode, "Y", group, node)
    factories = (archivefile, archivefilecopy, archivefilecopyrequest)
    fileY, srcY, dstY, afcrY = make_afcr("fileY", *commonargs, "Y", *factories)
    fileM, srcM, dstM, afcrM = make_afcr("fileM", *commonargs, "M", *factories)
    fileX, srcX, dstX, afcrX = make_afcr("fileX", *commonargs, "X", *factories)
    fileN, srcN, dstN, afcrN = make_afcr("fileN", *commonargs, "N", *factories)

    # Files don't exist on dest
    group.mock.exists = lambda path: None

    # update ran
    assert update.update_group(group, hostname, queue, True)

    # afcrY is cancelled
    assert not ArchiveFileCopyRequest.get(file=fileY).completed
    assert ArchiveFileCopyRequest.get(file=fileY).cancelled
    assert call.pull(afcrY) not in group.mock.mock_calls

    # afcrM is ongoing, but not handled
    assert not ArchiveFileCopyRequest.get(file=fileM).completed
    assert not ArchiveFileCopyRequest.get(file=fileM).cancelled
    assert call.pull(afcrM) not in group.mock.mock_calls

    # afcrX was executed
    assert not ArchiveFileCopyRequest.get(file=fileX).completed
    assert not ArchiveFileCopyRequest.get(file=fileX).cancelled
    assert call.pull(afcrX) in group.mock.mock_calls

    # afcrN was executed
    assert not ArchiveFileCopyRequest.get(file=fileX).completed
    assert not ArchiveFileCopyRequest.get(file=fileX).cancelled
    assert call.pull(afcrN) in group.mock.mock_calls


def test_update_node_not_idle(mocknode, queue):
    """Test update.update_node on non-idle node."""

    mocknode.mock.idle = False

    # update should not have run
    assert not update.update_node(mocknode, queue)

    # But avail_gb was updated
    assert call.update_avail_gb() in mocknode.mock.mock_calls


def test_update_node_no_update(mocknode, queue):
    """Test update.update_node with before_update returning False."""

    mocknode.mock.idle = True
    mocknode.mock.before_update = lambda idle: False

    # update should not have run
    assert not update.update_node(mocknode, queue)

    # But avail_gb was updated
    assert call.update_avail_gb() in mocknode.mock.mock_calls


def test_update_node_run(
    mocknode, queue, simplefile, archivefilecopy, archivefilecopyrequest
):
    """Test running update.update_node."""

    # Make something to check
    copy = archivefilecopy(node=mocknode, file=simplefile, has_file="M")

    # And something to pull
    afcr = archivefilecopyrequest(
        node_from=mocknode, group_to=mocknode.group, file=simplefile
    )

    mocknode.mock.idle = True
    mocknode.mock.before_update = lambda idle: True

    # update ran
    assert update.update_node(mocknode, queue)

    # Check I/O calls
    calls = list(mocknode.mock.mock_calls)
    assert call.update_avail_gb() in calls
    assert call.check(copy) in calls
    assert call.ready_pull(afcr) in calls


def test_update_node_active(simplenode):
    """Test update.update_node_active."""

    # Starts out active
    simplenode.active = True
    simplenode.save()
    assert simplenode.active

    # Pretend node is actually active
    with patch.object(simplenode.io, "check_active", lambda: True):
        assert update.update_node_active(simplenode)
    assert simplenode.active
    assert StorageNode.select(StorageNode.active).limit(1).scalar()

    # Pretend node is actually not active
    with patch.object(simplenode.io, "check_active", lambda: False):
        assert not update.update_node_active(simplenode)
    assert not simplenode.active
    assert not StorageNode.select(StorageNode.active).limit(1).scalar()


def test_update_node_delete_under_min(
    simplenode, simpleacq, simplefiletype, archivefile, archivefilecopy
):
    """Test update.update_node_delete() when not under min"""

    copyY = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileY", acq=simpleacq, type=simplefiletype),
        has_file="Y",
        wants_file="Y",
    )
    copyM = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileM", acq=simpleacq, type=simplefiletype),
        has_file="Y",
        wants_file="M",
    )
    copyN = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileN", acq=simpleacq, type=simplefiletype),
        has_file="Y",
        wants_file="N",
    )

    # Force under min and not archive
    simplenode.avail_gb = 5
    simplenode.min_avail_gb = 10
    simplenode.storage_type = "F"
    assert simplenode.under_min()
    assert not simplenode.archive

    mock_delete = MagicMock()
    with patch.object(simplenode.io, "delete", mock_delete):
        update.update_node_delete(simplenode)
    mock_delete.assert_called_once_with([copyM, copyN])


def test_update_node_delete_over_min(
    simplenode, simpleacq, simplefiletype, archivefile, archivefilecopy
):
    """Test update.update_node_delete() when not under min"""

    copyY = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileY", acq=simpleacq, type=simplefiletype),
        has_file="Y",
        wants_file="Y",
    )
    copyM = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileM", acq=simpleacq, type=simplefiletype),
        has_file="Y",
        wants_file="M",
    )
    copyN = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileN", acq=simpleacq, type=simplefiletype),
        has_file="Y",
        wants_file="N",
    )

    mock_delete = MagicMock()
    with patch.object(simplenode.io, "delete", mock_delete):
        update.update_node_delete(simplenode)
    mock_delete.assert_called_once_with([copyN])


@patch("alpenhorn.io.Default.DefaultNodeIO.auto_verify")
def test_auto_verify(
    mock_check, simplenode, simpleacq, simplefiletype, archivefile, archivefilecopy
):
    """Test update.auto_verify()"""

    # Enable auto_verify
    simplenode.auto_verify = 4

    # Make some files to verify
    copyY = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileY", acq=simpleacq, type=simplefiletype),
        has_file="Y",
    )
    copyN = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileN", acq=simpleacq, type=simplefiletype),
        has_file="N",
    )
    copyM = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileM", acq=simpleacq, type=simplefiletype),
        has_file="M",
    )
    copyX = archivefilecopy(
        node=simplenode,
        file=archivefile(name="fileX", acq=simpleacq, type=simplefiletype),
        has_file="X",
    )

    update.auto_verify(simplenode)
    calls = list(mock_check.mock_calls)

    # CopyN not checked
    assert call(copyY) in calls
    assert call(copyN) not in calls
    assert call(copyM) in calls
    assert call(copyX) in calls
