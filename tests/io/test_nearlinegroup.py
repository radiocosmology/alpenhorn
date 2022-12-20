"""Test NearlineGroupIO."""

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def group(xfs, mock_lfs, hostname, queue, storagegroup, storagenode):
    """Create a nearline group for testing."""

    group = storagegroup(name="group", io_class="Nearline")

    nearline = storagenode(
        name="nearline",
        io_class="Nearline",
        group=group,
        storage_type="A",
        root="/nearline",
        host=hostname,
        io_config='{"quota_group": "qgroup", "fixed_quota": 41000}',
    )
    smallfile = storagenode(
        name="smallfile",
        group=group,
        storage_type="A",
        root="/smallfile",
        host=hostname,
    )

    # Init I/O
    group.io.before_update([nearline, smallfile], True)
    nearline.io.set_queue(queue)
    smallfile.io.set_queue(queue)

    # All the node pull methods are mocked to avoid running them.
    nearline.io.pull = MagicMock(return_value=None)
    smallfile.io.pull = MagicMock(return_value=None)

    xfs.create_dir(nearline.root)
    xfs.create_dir(smallfile.root)

    return group, nearline, smallfile


@pytest.fixture
def req(
    group,
    genericgroup,
    storagenode,
    genericfile,
    archivefilecopyrequest,
):
    """Create an ArchiveFileCopyRequest targetting the nearline group."""
    group_to = group[0]
    node_from = storagenode(name="src", group=genericgroup)
    req = archivefilecopyrequest(
        file=genericfile, node_from=node_from, group_to=group_to
    )

    return req


def test_before_update(storagegroup, storagenode):
    group = storagegroup(name="group", io_class="Nearline")

    nearline = storagenode(name="nearline", group=group, io_class="Nearline")
    smallfile = storagenode(name="smallfile", group=group, io_class="Default")

    # These should work
    assert group.io.before_update([nearline, smallfile], True) is True
    assert group.io.before_update([smallfile, nearline], True) is True

    # But not these
    assert group.io.before_update([smallfile], True) is False
    assert group.io.before_update([smallfile, smallfile], True) is False


def test_idle(queue, group):
    """Test TransportGroupIO.idle."""
    group, nearline, smallfile = group

    # Currently idle
    assert group.io.idle is True

    for node in nearline, smallfile:
        # Enqueue something into the node's queue
        queue.put(None, node.name)

        # Now not idle
        assert group.io.idle is False

        # Dequeue it
        task, key = queue.get()
        queue.task_done(node.name)

        # Now idle again
        assert group.io.idle is True


def test_exists(xfs, group):
    """Test TransportGroupIO.exists()."""
    group, nearline, smallfile = group

    # make some files
    xfs.create_file("/nearline/test/one")
    xfs.create_file("/nearline/test/two")
    xfs.create_file("/smallfile/test/two")

    # Check
    assert group.io.exists("test/one") == nearline
    assert group.io.exists("test/two") == smallfile
    assert group.io.exists("test/three") == None


def test_pull_small(req, group):
    """Test TransportGroupIO.pull() hands off a small file to the smallfile node."""
    group, nearline, smallfile = group

    req.file.size_b = 1  # A very small file
    group.io.pull(req)

    # Sent to smallfile
    smallfile.io.pull.assert_called_once_with(req)
    nearline.io.pull.assert_not_called()


def test_pull_big(req, group):
    """Test TransportGroupIO.pull() hands off a large file to the nearline node."""
    group, nearline, smallfile = group

    req.file.size_b = 1e10
    group.io.pull(req)

    nearline.io.pull.assert_called_once_with(req)
    smallfile.io.pull.assert_not_called()
