"""Test LustreHSMGroupIO."""

import pytest
from unittest.mock import MagicMock

from alpenhorn.update import UpdateableGroup, UpdateableNode


@pytest.fixture
def group(xfs, mock_lfs, hostname, queue, storagegroup, storagenode):
    """Create a hsm group for testing."""

    stgroup = storagegroup(name="group", io_class="LustreHSM")

    hsm = UpdateableNode(
        queue,
        storagenode(
            name="hsm",
            io_class="LustreHSM",
            group=stgroup,
            storage_type="A",
            root="/hsm",
            host=hostname,
            io_config='{"quota_group": "qgroup", "headroom": 10250}',
        ),
    )
    smallfile = UpdateableNode(
        queue,
        storagenode(
            name="smallfile",
            group=stgroup,
            storage_type="A",
            root="/smallfile",
            host=hostname,
        ),
    )

    group = UpdateableGroup(group=stgroup, nodes=[hsm, smallfile], idle=True)

    # All the node pull methods are mocked to avoid running them.
    hsm.io.pull = MagicMock(return_value=None)
    smallfile.io.pull = MagicMock(return_value=None)

    xfs.create_dir(hsm.db.root)
    xfs.create_dir(smallfile.db.root)

    return group, hsm, smallfile


@pytest.fixture
def req(
    group,
    simplegroup,
    storagenode,
    simplefile,
    archivefilecopyrequest,
):
    """Create an ArchiveFileCopyRequest targetting the hsm group."""
    group_to = group[0].db
    node_from = storagenode(name="src", group=simplegroup)
    req = archivefilecopyrequest(
        file=simplefile, node_from=node_from, group_to=group_to
    )

    return req


def test_init(storagegroup, storagenode, mock_lfs):
    stgroup = storagegroup(name="group", io_class="LustreHSM")

    hsm = UpdateableNode(
        None,
        storagenode(
            name="hsm",
            group=stgroup,
            io_class="LustreHSM",
            io_config='{"quota_group": "qgroup", "headroom": 10250}',
        ),
    )
    smallfile = UpdateableNode(
        None, storagenode(name="smallfile", group=stgroup, io_class="Default")
    )

    # These should work
    group = UpdateableGroup(group=stgroup, nodes=[hsm, smallfile], idle=True)
    assert group._nodes is not None
    assert group.io._hsm is hsm
    assert group.io._smallfile is smallfile

    group = UpdateableGroup(group=stgroup, nodes=[smallfile, hsm], idle=True)
    assert group._nodes is not None
    assert group.io._hsm is hsm
    assert group.io._smallfile is smallfile

    # But not these
    group = UpdateableGroup(group=stgroup, nodes=[smallfile], idle=True)
    assert group._nodes is None
    group = UpdateableGroup(group=stgroup, nodes=[hsm], idle=True)
    assert group._nodes is None
    group = UpdateableGroup(group=stgroup, nodes=[smallfile, smallfile], idle=True)
    assert group._nodes is None


def test_idle(queue, group):
    """Test TransportGroupIO.idle."""
    group, hsm, smallfile = group

    # Currently idle
    assert group.idle is True

    for node in hsm, smallfile:
        # Enqueue something into the node's queue
        queue.put(None, node.name)

        # Now not idle
        assert group.idle is False

        # Dequeue it
        task, key = queue.get()
        queue.task_done(node.name)

        # Now idle again
        assert group.idle is True


def test_exists(xfs, group):
    """Test TransportGroupIO.exists()."""
    group, hsm, smallfile = group

    # make some files
    xfs.create_file("/hsm/test/one")
    xfs.create_file("/hsm/test/two")
    xfs.create_file("/smallfile/test/two")

    # Check
    assert group.io.exists("test/one") == hsm
    assert group.io.exists("test/two") == smallfile
    assert group.io.exists("test/three") is None


def test_pull_small(req, group):
    """Test TransportGroupIO.pull() hands off a small file to the smallfile node."""
    group, hsm, smallfile = group

    req.file.size_b = 1  # A very small file
    group.io.pull(req)

    # Sent to smallfile
    smallfile.io.pull.assert_called_once_with(req)
    hsm.io.pull.assert_not_called()


def test_pull_big(req, group):
    """Test TransportGroupIO.pull() hands off a large file to the hsm node."""
    group, hsm, smallfile = group

    req.file.size_b = 1e10
    group.io.pull(req)

    hsm.io.pull.assert_called_once_with(req)
    smallfile.io.pull.assert_not_called()
