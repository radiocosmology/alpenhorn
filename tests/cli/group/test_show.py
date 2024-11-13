"""Test CLI: alpenhorn group show"""

import pytest
from alpenhorn.db import (
    StorageGroup,
    StorageNode,
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
)


def test_no_show(clidb, cli):
    """Test showing nothing."""

    cli(1, ["group", "show", "TEST"])


def test_show_defaults(clidb, cli):
    """Test show with default parameters and no nodes."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup")

    result = cli(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Notes" in result.output
    assert "Default" in result.output
    assert "Nodes" in result.output


def test_show_no_io_config(clidb, cli):
    """Test show with no I/O config."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", notes="Comment", io_class="IOClass")
    StorageNode.create(name="Node1", group=group)
    StorageNode.create(name="Node2", group=group)

    result = cli(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "I/O Config" in result.output


def test_show_empty_io_config(clidb, cli):
    """Test show with empty I/O config."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(
        name="SGroup", notes="Comment", io_class="IOClass", io_config="{}"
    )
    StorageNode.create(name="Node1", group=group)
    StorageNode.create(name="Node2", group=group)

    result = cli(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "I/O Config" in result.output
    assert "Node1" in result.output
    assert "Node2" in result.output


def test_show_io_config(clidb, cli):
    """Test show with I/O config."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(
        name="SGroup",
        notes="Comment",
        io_class="IOClass",
        io_config='{"Param1": 1, "Param2": 2}',
    )

    result = cli(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "Param1" in result.output
    assert "Param2" in result.output


def test_show_node_details(clidb, cli, assert_row_present):
    """Test show --node-details."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", io_class="IOClass")
    StorageNode.create(name="Node1", group=group, active=True, host="over_here")
    StorageNode.create(
        name="Node2", group=group, active=False, host="over_there", io_class="NodeClass"
    )

    result = cli(0, ["group", "show", "SGroup", "--node-details"])

    assert_row_present(result.output, "Node1", "over_here", "Yes", "Default")
    assert_row_present(result.output, "Node2", "over_there", "No", "NodeClass")


def test_show_node_stats(clidb, cli, assert_row_present):
    """Test show --node-stats."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", io_class="IOClass")
    node1 = StorageNode.create(name="Node1", group=group, active=True, host="over_here")
    node2 = StorageNode.create(
        name="Node2",
        group=group,
        active=False,
        host="over_there",
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

    result = cli(0, ["group", "show", "SGroup", "--node-stats"])

    assert_row_present(result.output, "Node1", 2, "4.580 kiB", "-")
    assert_row_present(result.output, "Node2", 2, "5.665 kiB", "0.00")


def test_show_node_details_stats(clidb, cli, assert_row_present):
    """Test show --node-details --node-stats."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", io_class="IOClass")
    node1 = StorageNode.create(name="Node1", group=group, active=True, host="over_here")
    node2 = StorageNode.create(
        name="Node2",
        group=group,
        active=False,
        host="over_there",
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

    result = cli(0, ["group", "show", "SGroup", "--node-stats", "--node-details"])

    assert_row_present(
        result.output, "Node1", "over_here", "Yes", "Default", 2, "4.580 kiB", "-"
    )
    assert_row_present(
        result.output, "Node2", "over_there", "No", "NodeClass", 2, "5.665 kiB", "0.00"
    )
