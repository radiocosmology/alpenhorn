"""Test TransportGroupIO."""

from unittest.mock import MagicMock, patch

import pytest

from alpenhorn.daemon.update import UpdateableGroup, UpdateableNode
from alpenhorn.db import ArchiveFileCopy, ArchiveFileCopyRequest
from alpenhorn.io._default_asyncs import group_search_async


@pytest.fixture
def transport_fleet(transport_fleet_no_init, queue):
    """Create a Transport group for testing.

    Returns a tuple:
        - the Tranport StorageGroup
        - a list of the StorageNodes
    """

    # Do init
    stgroup, nodes = transport_fleet_no_init
    group = UpdateableGroup(queue=queue, group=stgroup, nodes=nodes, idle=True)

    return group, nodes


@pytest.fixture
def transport_fleet_no_init(xfs, hostname, queue, storagegroup, storagenode):
    """Like transport_fleet, but initialisation of the group
    is not done (so it can be tested)."""

    stgroup = storagegroup(name="group", io_class="Transport")

    nodes = [
        UpdateableNode(
            queue,
            storagenode(
                name="node1",
                group=stgroup,
                storage_type="T",
                avail_gb=10,
                root="/node1",
                host=hostname,
            ),
        ),
        UpdateableNode(
            queue,
            storagenode(
                name="node2",
                group=stgroup,
                storage_type="T",
                avail_gb=20,
                root="/node2",
                host=hostname,
            ),
        ),
        UpdateableNode(
            queue,
            storagenode(
                name="node3",
                group=stgroup,
                storage_type="T",
                avail_gb=40,
                root="/node3",
                host=hostname,
            ),
        ),
        UpdateableNode(
            queue,
            storagenode(
                name="node4",
                group=stgroup,
                storage_type="T",
                avail_gb=None,
                root="/node4",
                host=hostname,
            ),
        ),
    ]

    for index, node in enumerate(nodes):
        # All the node pull methods are mocked to avoid running them.
        node.io.pull = MagicMock(return_value=None)
        # mock bytes_avail to simply return avail_gb to avoid having to mess about
        # with pyfakefs
        node.io.bytes_avail = MagicMock(
            return_value=(
                None if node.db.avail_gb is None else node.db.avail_gb * 2**30
            )
        )
        xfs.create_dir(node.db.root)

    return stgroup, nodes


@pytest.fixture
def remote_req(
    transport_fleet,
    simplegroup,
    storagenode,
    simplefile,
    archivefilecopyrequest,
):
    """Create a non-local ArchiveFileCopyRequest targetting the transport group."""
    group_to, _ = transport_fleet
    node_from = storagenode(name="src", group=simplegroup, host="other-host")
    return archivefilecopyrequest(
        file=simplefile, node_from=node_from, group_to=group_to.db
    )


@pytest.fixture
def req(hostname, remote_req):
    """Create a local ArchiveFileCopyRequest targetting the transport group."""

    # Fix src node to be local
    remote_req.node_from.host = hostname

    return remote_req


def test_group_init(transport_fleet_no_init, queue):
    """Test initialisation of TranportGroupIO with good nodes."""
    stgroup, nodes = transport_fleet_no_init

    group = UpdateableGroup(queue=queue, group=stgroup, nodes=nodes, idle=True)
    assert group.io.nodes == nodes
    assert group.io.fifo is not None


def test_group_init_bad(transport_fleet_no_init, queue):
    """Test initialisation of TranportGroupIO with bad nodes."""
    stgroup, nodes = transport_fleet_no_init

    # Change all the nodes to not be transport nodes.
    for node in nodes:
        node.db.storage_type = "F"

    group = UpdateableGroup(queue=queue, group=stgroup, nodes=nodes, idle=True)
    assert group.io.nodes == []


def test_group_init_mixed(transport_fleet_no_init, queue):
    """Test initialisation of TranportGroupIO with some bad nodes."""
    stgroup, nodes = transport_fleet_no_init

    # Change _some_ of the nodes to non-transport
    nodes[0].db.storage_type = "A"
    nodes[3].db.storage_type = "F"

    group = UpdateableGroup(queue=queue, group=stgroup, nodes=nodes, idle=True)
    assert group.io.nodes == nodes[1:3]


def test_idle(queue, transport_fleet):
    """Test TransportGroupIO.idle."""
    group, nodes = transport_fleet

    # Currently idle
    assert group.idle is True

    # Enqueue something into a node's queue
    queue.put(None, nodes[2].io.fifo)

    # Now not idle
    assert group.idle is False

    # Dequeue it
    queue.get()
    queue.task_done(nodes[2].io.fifo)

    # Now idle again
    assert group.idle is True


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
    assert group.io.exists("test/three") is None


def test_pull_remote_skip(remote_req, transport_fleet):
    """Test TransportGroupIO.pull() skips non-local requests."""
    group, nodes = transport_fleet

    group.io.pull(remote_req, True)

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

    group.io.pull(req, True)

    # Since the copy has no size, it will be put onto the first (smallest) node
    nodes[0].io.pull.assert_called_once_with(req, True)
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

    group.io.pull(req, True)

    nodes[0].io.pull.assert_not_called()
    nodes[1].io.pull.assert_called_once_with(req, True)
    nodes[2].io.pull.assert_not_called()
    nodes[3].io.pull.assert_not_called()


def test_pull_minmax(req, archivefilecopy, transport_fleet):
    """Test TransportGroupIO.pull()
    correctly rejecting under-min and over-max nodes."""
    group, nodes = transport_fleet

    # Make nodes[0] under-min
    nodes[0].db.min_avail_gb = nodes[0].db.avail_gb * 2

    # Make nodes[1] over-max
    req.file.size_b = 10000
    archivefilecopy(file=req.file, node=nodes[1].db, has_file="Y")
    nodes[1].db.max_total_gb = 1e-3

    # Check
    group.io.pull(req, True)

    # Node 2 is the first node that can take the file
    nodes[0].io.pull.assert_not_called()
    nodes[1].io.pull.assert_not_called()
    nodes[2].io.pull.assert_called_once_with(req, True)
    nodes[3].io.pull.assert_not_called()


def test_pull_nonode(req, archivefilecopy, transport_fleet):
    """Test TransportGroupIO.pull() being okay with no node available."""
    group, nodes = transport_fleet

    # Give node[3] a size
    nodes[3].db.avail_gb = 3
    # Make all nodes under-min
    for node in nodes:
        node.db.min_avail_gb = node.db.avail_gb * 2

    # Check
    group.io.pull(req, True)

    nodes[0].io.pull.assert_not_called()
    nodes[1].io.pull.assert_not_called()
    nodes[2].io.pull.assert_not_called()
    nodes[3].io.pull.assert_not_called()


def test_pull_search(transport_fleet, simplecopyrequest, queue):
    """Test task submission in TransportGroupIO.pull_search()."""

    group, _ = transport_fleet

    # The patch is done in io.default, where the pull_search method
    # is actually defined.
    with patch("alpenhorn.io.default.group_search_async") as mock:
        group.io.pull_search(simplecopyrequest)

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


def test_group_search_dispatch(transport_fleet, dbtables, simplecopyrequest, queue):
    """Test group_search_async dispatch to pull"""

    group, _ = transport_fleet

    mock = MagicMock()
    group.io.pull = mock

    # Run the async.  First argument is Task
    group_search_async(None, group.io, simplecopyrequest)

    # Check dispatch
    mock.assert_called_once_with(simplecopyrequest, did_search=True)


def test_group_search_in_db(
    transport_fleet, simplefile, archivefilecopy, archivefilecopyrequest, queue, xfs
):
    """Test group_search_async with record already in db."""

    group, nodes = transport_fleet
    node = nodes[0]

    mock = MagicMock()
    group.io.pull = mock

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
    transport_fleet, dbtables, simplefile, archivefilecopyrequest, queue, xfs
):
    """Test group_search_async with existing file."""

    group, nodes = transport_fleet
    node = nodes[0]

    mock = MagicMock()
    group.io.pull = mock

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
    transport_fleet, simplefile, archivefilecopyrequest, archivefilecopy, queue, xfs
):
    """Test group_search_async with existing file and has_file=N."""

    group, nodes = transport_fleet
    node = nodes[0]

    mock = MagicMock()
    group.io.pull = mock

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


def test_group_search_corrupt(
    transport_fleet, simplefile, archivefilecopyrequest, archivefilecopy, queue, xfs
):
    """Test group_search_async with corrupt file.

    The corrupt file shouldn't mask another, surprise file
    on another node.
    """

    group, nodes = transport_fleet

    mock = MagicMock()
    group.io.pull = mock

    # Create a files on the first three nodes
    xfs.create_file(f"{nodes[0].db.root}/{simplefile.path}")
    xfs.create_file(f"{nodes[1].db.root}/{simplefile.path}")
    xfs.create_file(f"{nodes[2].db.root}/{simplefile.path}")

    # Mark the copy on the second node as corrupt.
    archivefilecopy(file=simplefile, node=nodes[1].db, has_file="X", wants_file="Y")

    # Create a copy request for the file.
    # Source here doesn't matter
    afcr = archivefilecopyrequest(
        file=simplefile, node_from=nodes[0].db, group_to=group.db
    )

    # Run the async.  First argument is Task
    group_search_async(None, group.io, afcr)

    # Check dispatch
    mock.assert_not_called()

    # State of the file on nodes[1] hasn't changed, but files have been
    # found on both nodes[0] and nodes[2]
    assert ArchiveFileCopy.get(file=afcr.file, node=nodes[0].db).has_file == "M"
    assert ArchiveFileCopy.get(file=afcr.file, node=nodes[1].db).has_file == "X"
    assert ArchiveFileCopy.get(file=afcr.file, node=nodes[2].db).has_file == "M"
