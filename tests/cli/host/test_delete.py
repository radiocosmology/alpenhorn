"""Test CLI: alpenhorn host delete"""

from alpenhorn.db import StorageGroup, StorageHost, StorageNode


def test_no_host(clidb, cli):
    """Test deleting a non-existent host."""

    cli(1, ["host", "delete", "TEST"], input="Y\n")


def test_check_force(clidb, cli):
    """Test using both --check and --force."""

    StorageHost.create(name="TEST")

    cli(2, ["host", "delete", "TEST", "--check", "--force"])

    # Record still exists
    assert StorageHost.select().count() == 1


def test_check(clidb, cli):
    """Test using --check."""

    StorageHost.create(name="TEST")

    cli(0, ["host", "delete", "TEST", "--check"])

    # Record still exists
    assert StorageHost.select().count() == 1


def test_force(clidb, cli):
    """Test using --force."""

    StorageHost.create(name="TEST")

    cli(0, ["host", "delete", "TEST", "--force"])

    # Record no longer exists
    assert StorageHost.select().count() == 0


def test_delete(clidb, cli):
    """Test deleting a host."""

    StorageHost.create(name="TEST")
    StorageHost.create(name="NOT-TEST")

    cli(0, ["host", "delete", "TEST"], input="Y\n")

    # Record no longer exists
    assert StorageHost.get_or_none(name="TEST") is None

    # But the other one still does
    assert StorageHost.get_or_none(name="NOT-TEST") is not None


def test_nodes(clidb, cli):
    """Test deleting a host with nodes."""

    host = StorageHost.create(name="TEST")
    StorageNode.create(name="NODE", group=StorageGroup.create(name="GROUP"), host=host)

    cli(0, ["host", "delete", "TEST"], input="Y\n")

    # Record still exists
    assert StorageHost.select().count() == 1


def test_nodes_remove(clidb, cli):
    """Test removing nodes before delete."""

    host = StorageHost.create(name="TEST")
    StorageNode.create(name="NODE", group=StorageGroup.create(name="GROUP"), host=host)

    cli(0, ["host", "delete", "TEST", "--remove-nodes"], input="Y\n")

    # Record no longer exists
    assert StorageHost.select().count() == 0
    # Node has no host now.
    assert StorageNode.get(name="NODE").host is None
