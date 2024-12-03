"""Test DefaultGroupIO."""

from unittest.mock import MagicMock, patch

import pytest

from alpenhorn.daemon.update import UpdateableGroup, UpdateableNode
from alpenhorn.db import ArchiveFileCopy, ArchiveFileCopyRequest
from alpenhorn.io._default_asyncs import group_search_async


@pytest.fixture
def groupnode(xfs, dbtables, queue, storagegroup, storagenode):
    """Fixture setting up a default test group.

    Returns both the group and the node."""
    stgroup = storagegroup(name="group")
    node = UpdateableNode(
        queue, storagenode(name="node", group=stgroup, active=True, root="/node")
    )
    group = UpdateableGroup(queue=queue, group=stgroup, nodes=[node], idle=True)

    # Create the directory
    xfs.create_dir("/node")

    return group, node


def test_too_many_nodes(storagegroup, storagenode, queue):
    """Test DefaultGroupIO rejecting more than one node."""

    stgroup = storagegroup(name="group")
    node1 = UpdateableNode(None, storagenode(name="node1", group=stgroup, active=True))
    node2 = UpdateableNode(None, storagenode(name="node2", group=stgroup, active=True))

    group = UpdateableGroup(queue=queue, group=stgroup, nodes=[node1, node2], idle=True)
    assert group._nodes is None


def test_just_enough_nodes(storagegroup, storagenode, queue):
    """Test DefaultGroupIO accepting one node."""

    stgroup = storagegroup(name="group")
    node = UpdateableNode(None, storagenode(name="node1", group=stgroup, active=True))

    group = UpdateableGroup(queue=queue, group=stgroup, nodes=[node], idle=True)
    assert group._nodes == [node]


def test_exists(xfs, groupnode):
    """Test DefaultGroupIO.exists."""

    group, node = groupnode

    # Make something on the node
    xfs.create_file("/node/acq/file")

    # Check
    assert group.io.exists("acq/file") == node
    assert group.io.exists("acq/no-file") is None
    assert group.io.exists("no-acq/no-file") is None


def test_pull_force_handoff(groupnode, simplecopyrequest):
    """Test DefaultGroupIO.pull_force()."""

    group, node = groupnode
    node.io.pull = MagicMock(return_value=None)

    # Call pull_force
    group.io.pull_force(simplecopyrequest)

    # Check for hand-off to the node
    node.io.pull.assert_called_with(simplecopyrequest)


def test_pull(groupnode, simplecopyrequest, queue):
    """Test task submission in DefaultGroupIO.pull()."""

    group, node = groupnode

    with patch("alpenhorn.io.default.group_search_async") as mock:
        group.io.pull(simplecopyrequest)

        # Task is queued
        assert queue.qsize == 1

        # Dequeue
        task, key = queue.get()

        # Run the "task"
        task()

        # Clean up queue
        queue.task_done(key)

        assert key == group.io.fifo
        mock.assert_called_once()


def test_group_search_dispatch(groupnode, simplecopyrequest, queue):
    """Test group_search_async dispatch to pull_force"""

    group, node = groupnode

    mock = MagicMock()
    group.io.pull_force = mock

    # Run the async.  First argument is Task
    group_search_async(None, group.io, simplecopyrequest)

    # Check dispatch
    mock.assert_called_once_with(simplecopyrequest)


def test_group_search_in_db(
    groupnode, simplefile, archivefilecopy, archivefilecopyrequest, queue, xfs
):
    """Test group_search_async with record already in db."""

    group, node = groupnode

    mock = MagicMock()
    group.io.pull_force = mock

    # Create a file on the dest
    xfs.create_file(f"{node.db.root}/{simplefile.path}")

    # Create a file copy record
    archivefilecopy(file=simplefile, node=node.db, has_file="Y")

    # Create a copy request for the file.
    # Source here doesn't matter
    afcr = archivefilecopyrequest(file=simplefile, node_from=node.db, group_to=group.db)

    # Run the async.  First argument is Task
    group_search_async(None, group.io, afcr)

    # Check dispatch
    mock.assert_not_called()

    # Verify that the request has been cancelled
    assert ArchiveFileCopyRequest.get(id=afcr.id).cancelled == 1


def test_group_search_existing(
    groupnode, simplefile, archivefilecopyrequest, queue, xfs
):
    """Test group_search_async with existing file."""

    group, node = groupnode

    mock = MagicMock()
    group.io.pull_force = mock

    # Create a file on the dest
    xfs.create_file(f"{node.db.root}/{simplefile.path}")

    # Create a copy request for the file.
    # Source here doesn't matter
    afcr = archivefilecopyrequest(file=simplefile, node_from=node.db, group_to=group.db)

    # Run the async.  First argument is Task
    group_search_async(None, group.io, afcr)

    # Check dispatch
    mock.assert_not_called()

    # Check for an archivefilecopy record requesting a check
    afc = ArchiveFileCopy.get(file=afcr.file, node=node.db)
    assert afc.has_file == "M"


def test_group_search_hasN(
    groupnode, simplefile, archivefilecopyrequest, archivefilecopy, queue, xfs
):
    """Test group_search_async with existing file and has_file=N."""

    group, node = groupnode

    mock = MagicMock()
    group.io.pull_force = mock

    # Create a file on the dest
    xfs.create_file(f"{node.db.root}/{simplefile.path}")

    # Create the copy record
    archivefilecopy(file=simplefile, node=node.db, has_file="N", wants_file="N")

    # Create a copy request for the file.
    # Source here doesn't matter
    afcr = archivefilecopyrequest(file=simplefile, node_from=node.db, group_to=group.db)

    # Run the async.  First argument is Task
    group_search_async(None, group.io, afcr)

    # Check dispatch
    mock.assert_not_called()

    # Check for an archivefilecopy record requesting a check
    afc = ArchiveFileCopy.get(file=afcr.file, node=node.db)
    assert afc.has_file == "M"
