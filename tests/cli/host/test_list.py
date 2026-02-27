"""Test CLI: alpenhorn host list"""

from alpenhorn.db import StorageGroup, StorageHost, StorageNode


def test_list(clidb, cli, assert_row_present):
    """Test listing hosts."""

    # Make some StorageHosts to list
    host = StorageHost.create(name="Host1", notes="Note1")
    StorageHost.create(name="Host2", notes="Note2", address="Addr2", username="User2")

    # Add some nodes
    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node1", group=group, host=host)
    StorageNode.create(name="Node2", group=group, host=host)

    result = cli(0, ["host", "list"])
    assert_row_present(result.output, "Host1", 2, "Note1")
    assert_row_present(result.output, "Host2", "User2", "Addr2", 0, "Note2")


def test_no_list(clidb, cli):
    """Test listing no hosts."""

    result = cli(0, ["host", "list"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output
