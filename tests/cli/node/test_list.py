"""Test CLI: alpenhorn node list"""

import pytest

from alpenhorn.db import StorageGroup, StorageNode


@pytest.fixture
def some_nodes(clidb):
    """Define some nodes to list"""

    group = StorageGroup.create(name="Group1")
    StorageNode.create(name="Node1", group=group, storage_type="F")
    StorageNode.create(
        name="Node2",
        group=group,
        storage_type="A",
        io_class="Class2",
        notes="Note2",
        active=True,
        host="Host2",
        root="/Node2",
    )
    group = StorageGroup.create(name="Group2", notes="Note2", io_class="Class2")
    StorageNode.create(
        name="Node3",
        group=group,
        storage_type="T",
        active=False,
        host="Host2",
        root="/Node3",
    )


def test_schema_mismatch(clidb, cli, cli_wrong_schema):
    """Test schema mismatch."""

    cli(1, ["node", "list"])


def test_no_list(clidb, cli):
    """Test listing no nodes."""

    result = cli(0, ["node", "list"])

    # i.e. the header hasn't been printed
    assert "I/O class" not in result.output


def test_list(some_nodes, cli, assert_row_present):
    """Test listing nodes."""

    result = cli(0, ["node", "list"])
    assert_row_present(result.output, "Node1", "Group1", "-", "", "", "No", "", "")
    assert_row_present(
        result.output,
        "Node2",
        "Group1",
        "archive",
        "Class2",
        "Host2",
        "Yes",
        "/Node2",
        "Note2",
    )
    assert_row_present(
        result.output, "Node3", "Group2", "transport", "", "Host2", "No", "/Node3", ""
    )


def test_active(some_nodes, cli, assert_row_present):
    """Test limit --active."""

    result = cli(0, ["node", "list", "--active"])
    assert "Node1" not in result.output
    assert_row_present(
        result.output,
        "Node2",
        "Group1",
        "archive",
        "Class2",
        "Host2",
        "Yes",
        "/Node2",
        "Note2",
    )
    assert "Node3" not in result.output


def test_inactive(some_nodes, cli, assert_row_present):
    """Test limit --inactive."""

    result = cli(0, ["node", "list", "--inactive"])
    assert_row_present(result.output, "Node1", "Group1", "-", "", "", "No", "", "")
    assert "Node2" not in result.output
    assert_row_present(
        result.output, "Node3", "Group2", "transport", "", "Host2", "No", "/Node3", ""
    )


def test_host(some_nodes, cli, assert_row_present):
    """Test limit --host."""

    result = cli(0, ["node", "list", "--host=Host2"])
    assert "Node1" not in result.output
    assert_row_present(
        result.output,
        "Node2",
        "Group1",
        "archive",
        "Class2",
        "Host2",
        "Yes",
        "/Node2",
        "Note2",
    )
    assert_row_present(
        result.output, "Node3", "Group2", "transport", "", "Host2", "No", "/Node3", ""
    )


def test_group(some_nodes, cli, assert_row_present):
    """Test limit --group."""

    result = cli(0, ["node", "list", "--group=Group1"])
    assert_row_present(result.output, "Node1", "Group1", "-", "", "", "No", "", "")
    assert_row_present(
        result.output,
        "Node2",
        "Group1",
        "archive",
        "Class2",
        "Host2",
        "Yes",
        "/Node2",
        "Note2",
    )
    assert "Node3" not in result.output


def test_limit_bad_group(some_nodes, cli):
    """Test limit matching to a non-existent group."""

    result = cli(1, ["node", "list", "--group=Group3"])

    # i.e. the header hasn't been printed
    assert "I/O class" not in result.output
    assert "Node1" not in result.output
    assert "Node2" not in result.output
    assert "Node3" not in result.output


def test_limit_nothing(some_nodes, cli):
    """Test limit matching nothing."""

    result = cli(0, ["node", "list", "--host=Host3"])

    # i.e. the header hasn't been printed
    assert "I/O class" not in result.output
    assert "Node1" not in result.output
    assert "Node2" not in result.output
    assert "Node3" not in result.output
