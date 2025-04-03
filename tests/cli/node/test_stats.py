"""Test CLI: alpenhorn node stats"""

import pytest

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


@pytest.fixture
def some_nodes(clidb):
    """Define some nodes to list"""

    group = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(
        name="Node1", group=group, max_total_gb=10, active=True, host="Host1"
    )
    node2 = StorageNode.create(name="Node2", group=group, active=False, host="Host1")
    StorageNode.create(name="Node3", group=group, active=False, host="Host2")
    group = StorageGroup.create(name="Group2")
    node4 = StorageNode.create(
        name="Node4", group=group, max_total_gb=20, active=True, host="Host2"
    )

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1 * 2**30)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node4, has_file="Y", wants_file="Y")
    file = ArchiveFile.create(name="File2", acq=acq, size_b=2 * 2**30)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node4, has_file="X", wants_file="Y")
    file = ArchiveFile.create(name="File3", acq=acq, size_b=4 * 2**30)
    ArchiveFileCopy.create(file=file, node=node1, has_file="M", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="N", wants_file="Y")
    file = ArchiveFile.create(name="File4", acq=acq, size_b=8 * 2**30)
    ArchiveFileCopy.create(file=file, node=node1, has_file="M", wants_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="N", wants_file="N")


def test_no_list(clidb, cli):
    """Test listing no nodes."""

    result = cli(0, ["node", "stats"])

    # i.e. the header hasn't been printed
    assert "Count" not in result.output


def test_list(some_nodes, cli, assert_row_present):
    """Test listing node stats."""

    result = cli(0, ["node", "stats"])

    assert_row_present(result.output, "Node1", 2, "3.000 GiB", "30.00")
    assert_row_present(result.output, "Node2", 0, "-", "-")
    assert_row_present(result.output, "Node3", 0, "-", "-")
    assert_row_present(result.output, "Node4", 1, "1.000 GiB", "5.00")


def test_negative_total(some_nodes, cli, assert_row_present):
    """Test listing nodes with negative max_total_gb."""

    # Set Node1's total to -1
    StorageNode.update(max_total_gb=-1).where(StorageNode.name == "Node1").execute()

    result = cli(0, ["node", "stats"])

    assert_row_present(result.output, "Node1", 2, "3.000 GiB", "-")


def test_active(some_nodes, cli, assert_row_present):
    """Test limit --active."""

    result = cli(0, ["node", "stats", "--active"])
    assert_row_present(result.output, "Node1", 2, "3.000 GiB", "30.00")
    assert "Node2" not in result.output
    assert "Node3" not in result.output
    assert_row_present(result.output, "Node4", 1, "1.000 GiB", "5.00")


def test_inactive(some_nodes, cli, assert_row_present):
    """Test limit --inactive."""

    result = cli(0, ["node", "stats", "--inactive"])
    assert "Node1" not in result.output
    assert_row_present(result.output, "Node2", 0, "-", "-")
    assert_row_present(result.output, "Node3", 0, "-", "-")
    assert "Node4" not in result.output


def test_host(some_nodes, cli, assert_row_present):
    """Test limit --host."""

    result = cli(0, ["node", "stats", "--host=Host2"])
    assert "Node1" not in result.output
    assert "Node2" not in result.output
    assert_row_present(result.output, "Node3", 0, "-", "-")
    assert_row_present(result.output, "Node4", 1, "1.000 GiB", "5.00")


def test_group(some_nodes, cli, assert_row_present):
    """Test limit --group."""

    result = cli(0, ["node", "stats", "--group=Group1"])
    assert_row_present(result.output, "Node1", 2, "3.000 GiB", "30.00")
    assert_row_present(result.output, "Node2", 0, "-", "-")
    assert_row_present(result.output, "Node3", 0, "-", "-")
    assert "Node4" not in result.output


def test_limit_bad_group(some_nodes, cli):
    """Test limit matching to a non-existent group."""

    result = cli(1, ["node", "stats", "--group=Group3"])

    # i.e. the header hasn't been printed
    assert "I/O class" not in result.output
    assert "Node1" not in result.output
    assert "Node2" not in result.output
    assert "Node3" not in result.output


def test_limit_nothing(some_nodes, cli):
    """Test limit matching nothing."""

    result = cli(0, ["node", "stats", "--host=Host3"])

    # i.e. the header hasn't been printed
    assert "I/O class" not in result.output
    assert "Node1" not in result.output
    assert "Node2" not in result.output
    assert "Node3" not in result.output


def test_extra(some_nodes, cli, assert_row_present):
    """Test --extra-stats."""

    result = cli(0, ["node", "stats", "--extra-stats"])

    assert_row_present(result.output, "Node1", 2, "3.000 GiB", "30.00", "-", "1", "-")
    assert_row_present(result.output, "Node2", 0, "-", "-", "-", "-", "1")
    assert_row_present(result.output, "Node3", 0, "-", "-", "-", "-", "-")
    assert_row_present(result.output, "Node4", 1, "1.000 GiB", "5.00", "1", "-", "-")
