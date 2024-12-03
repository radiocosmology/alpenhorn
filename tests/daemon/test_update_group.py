"""Tests for UpdateableGroup."""

from unittest.mock import call, patch

import pytest

from alpenhorn.daemon.update import UpdateableGroup, UpdateableNode
from alpenhorn.db.archive import ArchiveFileCopyRequest
from alpenhorn.db.storage import StorageGroup


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

    Returns a 4-tuple:
    * ArchiveFile
    * source-ArchiveFileCopy
    * dest-ArchiveFileCopy
    * Request
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

    # afcr is cancelled
    assert not ArchiveFileCopyRequest.get(file=afcr.file).completed
    assert ArchiveFileCopyRequest.get(file=afcr.file).cancelled
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


def test_update_group_multicopy(
    mockgroupandnode,
    hostname,
    queue,
    pull,
    storagenode,
    archivefilecopy,
    archivefilecopyrequest,
):
    """update.update_group shouldn't queue simultaneous pulls for the same file."""

    mockio, group, node = mockgroupandnode
    file, copy, afcr = pull

    # Make a duplicate AFCR
    archivefilecopyrequest(node_from=afcr.node_from, group_to=afcr.group_to, file=file)

    # Make a third AFCR for the same file coming from a different source
    node2 = storagenode(name="node2", group=afcr.node_from.group, active=True)
    archivefilecopy(node=node2, file=file, has_file="Y")
    archivefilecopyrequest(node_from=node2, group_to=afcr.group_to, file=file)

    # Run update
    group.update()

    # only one pull request should have been submitted
    mockio.group.pull.assert_called_once()


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

    # afcrX called pull_force
    assert not ArchiveFileCopyRequest.get(file=fileX).completed
    assert not ArchiveFileCopyRequest.get(file=fileX).cancelled
    assert call.pull_force(afcrX) in mockio.group.mock_calls

    # afcrN called pull
    assert not ArchiveFileCopyRequest.get(file=fileN).completed
    assert not ArchiveFileCopyRequest.get(file=fileN).cancelled
    assert call.pull(afcrN) in mockio.group.mock_calls


def test_group_idle_group(queue, mockgroupandnode):
    """Test DefaultGroupIO.idle via group queue."""
    mockio, group, node = mockgroupandnode

    # Currently idle
    assert group.idle is True

    # Enqueue something into this node's queue
    queue.put(None, group.io.fifo)

    # Now not idle
    assert group.idle is False

    # Dequeue it
    task, key = queue.get()

    # Still not idle, because task is in-progress
    assert group.idle is False

    # Finish the task
    queue.task_done(key)

    # Now idle again
    assert group.idle is True


def test_group_idle_node(queue, mockgroupandnode):
    """Test DefaultGroupIO.idle via node queue."""
    mockio, group, node = mockgroupandnode

    # Currently idle
    assert group.idle is True

    # Enqueue something into this node's queue
    queue.put(None, node.io.fifo)

    # Now not idle
    assert group.idle is False

    # Dequeue it
    task, key = queue.get()

    # Still not idle, because task is in-progress
    assert group.idle is False

    # Finish the task
    queue.task_done(key)

    # Now idle again
    assert group.idle is True


def test_reinit(storagegroup, storagenode, queue):
    """Test UpdateableGroup.reinit."""

    # Create a group
    stgroup = storagegroup(name="group")
    stnode = storagenode(name="node", group=stgroup)
    node = UpdateableNode(queue, stnode)
    group = UpdateableGroup(queue=queue, group=stgroup, nodes=[node], idle=True)

    # No I/O re-init
    stgroup = StorageGroup.get(id=stgroup.id)
    assert stgroup is not group.db
    io = group.io
    group.reinit(group=stgroup, nodes=[node], idle=True)
    assert io is group.io

    # But storagegroup is updated
    assert stgroup is group.db

    # Also no I/O re-init
    stgroup = StorageGroup.get(id=stgroup.id)
    assert stgroup is not group.db
    stgroup.notes = "Updated"
    stgroup.save()
    io = group.io
    group.reinit(group=stgroup, nodes=[node], idle=True)
    assert io is group.io
    assert stgroup is group.db

    # Changing io_config forces re-init
    stgroup = StorageGroup.get(id=stgroup.id)
    assert stgroup is not group.db
    stgroup.io_config = "{}"
    stgroup.save()
    group.reinit(group=stgroup, nodes=[node], idle=True)
    assert io is not group.io
    assert stgroup is group.db

    # Changing io_class forces re-init
    stgroup = StorageGroup.get(id=stgroup.id)
    assert stgroup is not group.db
    stgroup.io_class = "Default"
    stgroup.save()
    io = group.io
    group.reinit(group=stgroup, nodes=[node], idle=True)
    assert io is not group.io
    assert stgroup is group.db

    # Changing id forces re-init
    #
    # Alpenhornd indexes UpdateabelGroups by group name, so this would happen
    # if StorageGroup records have their names swapped around somehow, though we
    # don't need to do that in this test
    stgroup = storagegroup(
        name="group2",
        io_class=stgroup.io_class,
        io_config=stgroup.io_config,
    )
    assert stgroup is not group.db
    io = group.io
    group.reinit(group=stgroup, nodes=[node], idle=True)
    assert io is not group.io
    assert stgroup is group.db
