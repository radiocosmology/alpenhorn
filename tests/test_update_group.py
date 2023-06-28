"""Tests for UpdateableGroup."""
import pytest
from unittest.mock import patch, call

from alpenhorn.archive import ArchiveFileCopy, ArchiveFileCopyRequest


def make_afcr(
    name,
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
    file = archivefile(name=name, acq=acq)
    srccopy = archivefilecopy(node=srcnode, file=file, has_file=srcstate)
    if dststate is None:
        dstcopy = None
    else:
        dstcopy = archivefilecopy(node=dstnode, file=file, has_file=dststate)
    afcr = archivefilecopyrequest(node_from=srcnode, group_to=dstgroup, file=file)

    return file, srccopy, dstcopy, afcr


@pytest.fixture
def pull(
    mockgroupandnode,
    simpleacq,
    simplenode,
    archivefile,
    archivefilecopy,
    archivefilecopyrequest,
):
    """A simple ArchiveFileCopyRequest for update_group.

    The group and node form `mockedgroupandnode` are the
    destination.  `simplenode` is the source.
    """

    # Make source active
    simplenode.active = True
    simplenode.save()

    mockio, group, node = mockgroupandnode

    mockio.group.before_update.return_value = True
    # File doesn't exist on dest
    mockio.group.exists.return_value = None

    result = make_afcr(
        "file",
        simpleacq,
        simplenode,
        "Y",
        group.db,
        node.db,
        None,
        archivefile,
        archivefilecopy,
        archivefilecopyrequest,
    )

    # Don't bother returning the dstcopy, which is None
    return result[0], result[1], result[3]


def test_update_group_inactive(mockgroupandnode, hostname, queue, pull, simplenode):
    """Test running update.update_group with source node inactive."""

    mockio, group, node = mockgroupandnode
    file, copy, afcr = pull

    # Source not active
    simplenode.active = False
    simplenode.save()

    # run update
    group.update()

    # afcr is not handled
    assert not ArchiveFileCopyRequest.get(file=file).completed
    assert not ArchiveFileCopyRequest.get(file=file).cancelled
    assert call.pull(file) not in mockio.group.mock_calls


def test_update_group_nosrc(mockgroupandnode, hostname, queue, pull):
    """Test running update.update_group with source file missing."""

    mockio, group, node = mockgroupandnode
    file, copy, afcr = pull

    # Source not present
    copy.has_file = "N"
    copy.save()

    # run update
    group.update()

    # afcr is not handled
    assert not ArchiveFileCopyRequest.get(file=afcr.file).completed
    assert not ArchiveFileCopyRequest.get(file=afcr.file).cancelled
    assert call.pull(afcr) not in mockio.group.mock_calls


def test_update_group_notready(mockgroupandnode, hostname, queue, pull):
    """Test running update.update_group with source file not ready."""

    mockio, group, node = mockgroupandnode
    file, copy, afcr = pull

    # Force source not ready
    with patch(
        "alpenhorn.io.default.DefaultNodeRemote.pull_ready", lambda self, file: False
    ):
        # run update
        group.update()

    # afcr is not handled
    assert not ArchiveFileCopyRequest.get(file=file).completed
    assert not ArchiveFileCopyRequest.get(file=file).cancelled
    assert call.pull(afcr) not in mockio.group.mock_calls


def test_update_group_path_exists(mockgroupandnode, hostname, queue, pull):
    """Test running update.update_group with unexpected existing dest."""

    mockio, group, node = mockgroupandnode
    file, copy, afcr = pull

    # File exists on dest
    mockio.group.exists.return_value = node

    # This runs twice, to test two possibilities:
    # - no destination ArchiveFileCopy record
    # - desitination ArchiveFileCopy present with has_file='N'
    # It should behave the same both times
    for missing in [True, False]:
        # run update
        group.update()

        # Dest file copy needs checking
        dst = ArchiveFileCopy.get(file=file, node=node.db)
        assert dst.has_file == "M"

        # Request is still pending
        assert not ArchiveFileCopyRequest.get(file=file).completed
        assert not ArchiveFileCopyRequest.get(file=file).cancelled

        # Pull was not executed
        assert call.pull(afcr) not in mockio.group.mock_calls

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
    archivefile,
    archivefilecopy,
    archivefilecopyrequest,
):
    """Test running update.update_group with different copy states."""

    mockio, group, node = mockgroupandnode

    mockio.group.before_update.return_value = True
    # Files don't exist on dest
    mockio.group.exists.return_value = None

    # Source is active
    simplenode.active = True
    simplenode.save()

    # Make some pull requests
    commonargs = (simpleacq, simplenode, "Y", group.db, node.db)
    factories = (archivefile, archivefilecopy, archivefilecopyrequest)
    fileY, srcY, dstY, afcrY = make_afcr("fileY", *commonargs, "Y", *factories)
    fileM, srcM, dstM, afcrM = make_afcr("fileM", *commonargs, "M", *factories)
    fileX, srcX, dstX, afcrX = make_afcr("fileX", *commonargs, "X", *factories)
    fileN, srcN, dstN, afcrN = make_afcr("fileN", *commonargs, "N", *factories)

    # run update
    group.update()

    # afcrY is cancelled
    assert not ArchiveFileCopyRequest.get(file=fileY).completed
    assert ArchiveFileCopyRequest.get(file=fileY).cancelled
    assert call.pull(afcrY) not in mockio.group.mock_calls

    # afcrM is ongoing, but not handled
    assert not ArchiveFileCopyRequest.get(file=fileM).completed
    assert not ArchiveFileCopyRequest.get(file=fileM).cancelled
    assert call.pull(afcrM) not in mockio.group.mock_calls

    # afcrX was executed
    assert not ArchiveFileCopyRequest.get(file=fileX).completed
    assert not ArchiveFileCopyRequest.get(file=fileX).cancelled
    assert call.pull(afcrX) in mockio.group.mock_calls

    # afcrN was executed
    assert not ArchiveFileCopyRequest.get(file=fileX).completed
    assert not ArchiveFileCopyRequest.get(file=fileX).cancelled
    assert call.pull(afcrN) in mockio.group.mock_calls


def test_group_idle(queue, mockgroupandnode):
    """Test DefaultGroupIO.idle."""
    mockio, group, node = mockgroupandnode

    # Currently idle
    assert group.idle is True

    # Enqueue something into this node's queue
    queue.put(None, node.name)

    # Now not idle
    assert group.idle is False

    # Dequeue it
    task, key = queue.get()

    # Still not idle, because task is in-progress
    assert group.idle is False

    # Finish the task
    queue.task_done(node.name)

    # Now idle again
    assert group.idle is True
