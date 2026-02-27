"""Test CLI: alpenhorn host show"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageHost,
    StorageNode,
)


def test_no_show(clidb, cli):
    """Test showing nothing."""

    cli(1, ["host", "show", "TEST"])


def test_show_defaults(clidb, cli):
    """Test show with default parameters and no nodes."""

    StorageHost.create(name="SHost")

    result = cli(0, ["host", "show", "SHost"])

    assert "SHost" in result.output
    assert "Notes" in result.output
    assert "Nodes" in result.output


def test_show(clidb, cli):
    """Test show with some config and nodes."""

    group = StorageGroup.create(name="SGroup")
    host = StorageHost.create(
        name="SHost", address="SAddr", username="SUser", notes="Comment"
    )
    StorageNode.create(name="Node1", group=group, host=host)
    StorageNode.create(name="Node2", group=group, host=host)

    result = cli(0, ["host", "show", "SHost"])

    assert "SHost" in result.output
    assert "Comment" in result.output
    assert "SAddr" in result.output
    assert "SUser" in result.output
    assert "Node1" in result.output
    assert "Node2" in result.output


def test_show_node_details(clidb, cli, assert_row_present):
    """Test show --node-details."""

    group = StorageGroup.create(name="SGroup")
    host = StorageHost.create(name="SHost")
    StorageNode.create(name="Node1", group=group, active=True, host=host)
    StorageNode.create(
        name="Node2",
        group=group,
        active=False,
        host=host,
        io_class="NodeClass",
        max_total_gb=1,
    )

    result = cli(0, ["host", "show", "SHost", "--node-details"])

    assert_row_present(result.output, "Node1", "Yes", "Default")
    assert_row_present(result.output, "Node2", "No", "NodeClass")


def test_show_node_stats(clidb, cli, assert_row_present):
    """Test show --node-stats."""

    group = StorageGroup.create(name="SGroup")
    host = StorageHost.create(name="SHost")
    node1 = StorageNode.create(name="Node1", group=group, active=True, host=host)
    node2 = StorageNode.create(
        name="Node2",
        group=group,
        active=False,
        host=host,
        io_class="NodeClass",
        max_total_gb=1,
    )

    # And some files
    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="X", wants_file="Y")

    file = ArchiveFile.create(name="File2", acq=acq, size_b=2345)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="File3", acq=acq, size_b=3456)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = cli(0, ["host", "show", "SHost", "--node-stats"])

    assert_row_present(result.output, "Node1", 2, "4.580 kiB", "-")
    assert_row_present(result.output, "Node2", 2, "5.665 kiB", "0.00")


def test_show_node_details_stats(clidb, cli, assert_row_present):
    """Test show --node-details --node-stats."""

    group = StorageGroup.create(name="SGroup")
    host = StorageHost.create(name="SHost")
    node1 = StorageNode.create(name="Node1", group=group, active=True, host=host)
    node2 = StorageNode.create(
        name="Node2",
        group=group,
        active=False,
        host=host,
        io_class="NodeClass",
        max_total_gb=1,
    )

    # And some files
    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="X", wants_file="Y")

    file = ArchiveFile.create(name="File2", acq=acq, size_b=2345)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="File3", acq=acq, size_b=3456)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = cli(0, ["host", "show", "SHost", "--node-stats", "--node-details"])

    assert_row_present(result.output, "Node1", "Yes", "Default", 2, "4.580 kiB", "-")
    assert_row_present(
        result.output, "Node2", "No", "NodeClass", 2, "5.665 kiB", "0.00"
    )
