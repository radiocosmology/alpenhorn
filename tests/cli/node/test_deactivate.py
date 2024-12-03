"""Test CLI: alpenhorn node deactivate"""

from alpenhorn.db import StorageGroup, StorageNode


def test_no_node(clidb, cli):
    """Test deactivating a non-existent node."""

    cli(1, ["node", "deactivate", "TEST"])


def test_do_deactivate(clidb, cli):
    """Test deactivating an active node."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group, active=True)

    cli(0, ["node", "deactivate", "TEST"])

    # Check
    node = StorageNode.get(name="TEST")
    assert not node.active


def test_already_inactive(clidb, cli):
    """Test deactivating a node that is already inactive."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group, active=True)

    cli(0, ["node", "deactivate", "TEST"])

    node = StorageNode.get(name="TEST")
    assert not node.active
