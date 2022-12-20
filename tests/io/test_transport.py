"""Test TransportGroupIO."""

import pytest
from unittest.mock import MagicMock

from alpenhorn.storage import StorageNode
from alpenhorn.archive import ArchiveFileCopyRequest


@pytest.fixture
def transport_fleet(transport_fleet_no_init):
    """Create a Transport group for testing.

    Returns a tuple:
        - the Tranport StorageGroup
        - a list of the StorageNodes
    """

    # Do init
    group, nodes = transport_fleet_no_init
    group.io.before_update(nodes, True)

    return group, nodes


@pytest.fixture
def transport_fleet_no_init(xfs, hostname, queue, storagegroup, storagenode):
    """Like transport_fleet, but initialisation of the group
    is not done (so it can be tested)."""

    group = storagegroup(name="group", io_class="Transport")

    nodes = [
        storagenode(
            name="node1",
            group=group,
            storage_type="T",
            avail_gb=10,
            root="/node1",
            host=hostname,
        ),
        storagenode(
            name="node2",
            group=group,
            storage_type="T",
            avail_gb=20,
            root="/node2",
            host=hostname,
        ),
        storagenode(
            name="node3",
            group=group,
            storage_type="T",
            avail_gb=40,
            root="/node3",
            host=hostname,
        ),
        storagenode(
            name="node4",
            group=group,
            storage_type="T",
            avail_gb=None,
            root="/node4",
            host=hostname,
        ),
    ]

    for index, node in enumerate(nodes):
        node.io.set_queue(queue)
        # All the node pull methods are mocked to avoid running them.
        node.io.pull = MagicMock(return_value=None)
        # mock bytes_avail to simply return avail_gb to avoid having to mess about with pyfakefs
        node.io.bytes_avail = MagicMock(
            return_value=None if node.avail_gb is None else node.avail_gb * 2**30
        )
        xfs.create_dir(node.root)

    return group, nodes


@pytest.fixture
def remote_req(
    transport_fleet,
    genericgroup,
    storagenode,
    genericfile,
    archivefilecopyrequest,
):
    """Create a non-local ArchiveFileCopyRequest targetting the transport group."""
    group_to, _ = transport_fleet
    node_from = storagenode(name="src", group=genericgroup, host="other-host")
    req = archivefilecopyrequest(
        file=genericfile, node_from=node_from, group_to=group_to
    )

    return req


@pytest.fixture
def req(hostname, remote_req):
    """Create a local ArchiveFileCopyRequest targetting the transport group."""

    # Fix src node to be local
    remote_req.node_from.host = hostname

    return remote_req


def test_group_init(transport_fleet_no_init):
    """Test initialisation of TranportGroupIO with good nodes."""
    group, nodes = transport_fleet_no_init

    assert group.io.before_update(nodes, True) is True


def test_group_init_bad(transport_fleet_no_init):
    """Test initialisation of TranportGroupIO with bad nodes."""
    group, nodes = transport_fleet_no_init

    # Change all the nodes to not be transport nodes.
    for node in nodes:
        node.storage_type = "F"

    assert group.io.before_update(nodes, True) is False


def test_group_init_mixed(transport_fleet_no_init):
    """Test initialisation of TranportGroupIO with some bad nodes."""
    group, nodes = transport_fleet_no_init

    # Change _some_ of the nodes to non-trasmport
    nodes[0].storage_type = "A"
    nodes[3].storage_type = "F"

    assert group.io.before_update(nodes, True) is True


def test_idle(queue, transport_fleet):
    """Test TransportGroupIO.idle."""
    group, nodes = transport_fleet

    # Currently idle
    assert group.io.idle is True

    # Enqueue something into a node's queue
    queue.put(None, nodes[2].name)

    # Now not idle
    assert group.io.idle is False

    # Dequeue it
    task, key = queue.get()
    queue.task_done(nodes[2].name)

    # Now idle again
    assert group.io.idle is True


def test_exists(xfs, transport_fleet):
    """Test TransportGroupIO.exists()."""
    group, nodes = transport_fleet

    # make some files
    xfs.create_file("/node1/test/one")
    xfs.create_file("/node3/test/two")
    xfs.create_file("/node5/test/two")

    # Check
    assert group.io.exists("test/one") == nodes[0]
    assert group.io.exists("test/two") == nodes[2]
    assert group.io.exists("test/three") == None


def test_pull_remote_skip(remote_req, transport_fleet):
    """Test TransportGroupIO.pull() skips non-local requests."""
    group, nodes = transport_fleet

    group.io.pull(remote_req)

    # Request is not resolved
    afcr = ArchiveFileCopyRequest.get(id=remote_req.id)
    assert not afcr.completed
    assert not afcr.cancelled

    # Request did not get handed off
    for node in nodes:
        node.io.pull.assert_not_called()


def test_pull_local(req, transport_fleet):
    """Test TransportGroupIO.pull() hands off local requests."""
    group, nodes = transport_fleet

    group.io.pull(req)

    # Since the copy has no size, it will be put onto the first (smallest) node
    nodes[0].io.pull.assert_called_once_with(req)
    nodes[1].io.pull.assert_not_called()
    nodes[2].io.pull.assert_not_called()
    nodes[3].io.pull.assert_not_called()


def test_pull_size(req, transport_fleet):
    """Test TransportGroupIO.pull() finds the correct disk to fit the request."""
    group, nodes = transport_fleet

    # Set file size.  The node sizes in the transport fleet are (in GiB):
    # [10, 20, 40, None].  A file of size 6000 GiB will be sent to
    # nodes[1] due to the fudge factor of two in DefaultIO's reserve_bytes()
    req.file.size_b = 6 * 2**30

    group.io.pull(req)

    nodes[0].io.pull.assert_not_called()
    nodes[1].io.pull.assert_called_once_with(req)
    nodes[2].io.pull.assert_not_called()
    nodes[3].io.pull.assert_not_called()


def test_pull_minmax(req, archivefilecopy, transport_fleet):
    """Test TransportGroupIO.pull() correctly rejecting under-min and over-max nodes."""
    group, nodes = transport_fleet

    # Make nodes[0] under-min
    nodes[0].min_avail_gb = nodes[0].avail_gb * 2

    # Make nodes[1] over-max
    req.file.size_b = 10000
    archivefilecopy(file=req.file, node=nodes[1], has_file="Y")
    nodes[1].max_total_gb = 1e-3

    # Check
    group.io.pull(req)

    # Node 2 is the first node that can take the file
    nodes[0].io.pull.assert_not_called()
    nodes[1].io.pull.assert_not_called()
    nodes[2].io.pull.assert_called_once_with(req)
    nodes[3].io.pull.assert_not_called()


def test_pull_nonode(req, archivefilecopy, transport_fleet):
    """Test TransportGroupIO.pull() being okay with no node available."""
    group, nodes = transport_fleet

    # Give node[3] a size
    nodes[3].avail_gb = 3
    # Make all nodes under-min
    for node in nodes:
        node.min_avail_gb = node.avail_gb * 2

    # Check
    group.io.pull(req)

    nodes[0].io.pull.assert_not_called()
    nodes[1].io.pull.assert_not_called()
    nodes[2].io.pull.assert_not_called()
    nodes[3].io.pull.assert_not_called()
