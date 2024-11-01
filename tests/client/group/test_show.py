"""Test CLI: alpenhorn group show"""

import pytest
from alpenhorn.db import StorageGroup, StorageNode


def test_no_show(clidb, client):
    """Test showing nothing."""

    client(1, ["group", "show", "TEST"])


def test_show_defaults(clidb, client):
    """Test show with default parameters and no nodes."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup")

    result = client(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Notes" in result.output
    assert "Default" in result.output
    assert "Nodes" in result.output


def test_show_no_io_config(clidb, client):
    """Test show with no I/O config."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", notes="Comment", io_class="IOClass")
    StorageNode.create(name="Node1", group=group)
    StorageNode.create(name="Node2", group=group)

    result = client(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "I/O Config" in result.output


def test_show_empty_io_config(clidb, client):
    """Test show with empty I/O config."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(
        name="SGroup", notes="Comment", io_class="IOClass", io_config="{}"
    )
    StorageNode.create(name="Node1", group=group)
    StorageNode.create(name="Node2", group=group)

    result = client(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "I/O Config" in result.output
    assert "Node1" in result.output
    assert "Node2" in result.output


def test_show_io_config(clidb, client):
    """Test show with I/O config."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(
        name="SGroup",
        notes="Comment",
        io_class="IOClass",
        io_config='{"Param1": 1, "Param2": 2}',
    )

    result = client(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "Param1" in result.output
    assert "Param2" in result.output


def test_show_node_details(clidb, client):
    """Test show --node_details."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", io_class="IOClass")
    StorageNode.create(name="Node1", group=group, active=True, host="over_here")
    StorageNode.create(
        name="Node2", group=group, active=False, host="over_there", io_class="NodeClass"
    )

    result = client(0, ["group", "show", "SGroup", "--node-details"])

    assert "Node1" in result.output
    assert "Yes" in result.output
    assert "over_here" in result.output
    assert "Default" in result.output

    assert "Node1" in result.output
    assert "No" in result.output
    assert "over_there" in result.output
    assert "NodeClass" in result.output
