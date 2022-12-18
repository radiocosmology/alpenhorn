"""Test DefaultNodeIO.pull()"""

import pytest
import pathlib

from alpenhorn.archive import ArchiveFileCopyRequest


@pytest.fixture
def pull_async(queue, test_req):
    """Put a pull_async Task on the queue.

    Returns a two-element tuple: (node_to, copy_request)"""
    node, req = test_req
    node.io.set_queue(queue)
    node.io.pull(req)

    return test_req


@pytest.fixture
def test_req(
    xfs, storagegroup, storagenode, genericfile, archivefilecopy, archivefilecopyrequest
):
    """Create a test ArchiveFileCopyRequest.

    Returns a two-element tuple: (node_to, copy_request)"""

    group_to = storagegroup(name="group_to")
    node_to = storagenode(name="node_to", group=group_to, root="/node_to")
    node_from = storagenode(
        name="node_from", group=storagegroup(name="group_from"), root="/node_from"
    )

    copy = archivefilecopy(file=genericfile, node=node_from, has_file="Y")

    # Create source file and dest root
    xfs.create_dir("/node_to")
    xfs.create_file(copy.path, st_size=copy.size_b)
    return (
        node_to,
        archivefilecopyrequest(
            file=genericfile, node_from=node_from, group_to=group_to
        ),
    )


def test_pull_sync_undermin(queue, test_req):
    """test DefaultNodeIO.pull synchronous under_min check"""

    # Init I/O layer
    node, req = test_req
    node.io.set_queue(queue)

    # set up node to fail under_min check
    node.avail_gb = 1.0
    node.min_avail_gb = 2.0
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
    node.io.set_queue(queue)

    # set up node to fail over_max check
    file = archivefile(
        name="file2", acq=req.file.acq, type=req.file.type, size_b=100000
    )
    archivefilecopy(file=file, node=node, has_file="Y")
    node.max_total_gb = file.size_b / 2**21  # i.e. half of file.size_b
    node.io.pull(req)

    # No job should be queued and req isn't resolved.
    assert queue.qsize == 0
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False


def test_pull_sync_fit(xfs, queue, test_req):
    """test DefaultNodeIO.pull synchronous fit check"""

    # Init I/O layer
    node, req = test_req
    node.io.set_queue(queue)

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
    node.host = "here"
    req.node_from.host = "There"

    # Get the Task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Req isn't resolved.
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed is False
    assert afcr.cancelled is False
