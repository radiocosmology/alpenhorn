"""Test CLI: alpenhorn node init"""

from alpenhorn.db import ArchiveFileImportRequest, StorageGroup, StorageNode


def test_no_node(clidb, cli):
    """Test a missing NODE name"""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["node", "init"])


def test_bad_node(clidb, cli):
    """Test a bad node name"""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["node", "init", "MISSING"])


def test_init(clidb, cli):
    """Test the default invocation."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, root="/root")

    cli(0, ["node", "init", "Node"])

    # Request created
    afir = ArchiveFileImportRequest.get(id=1)

    assert afir.node == node
    assert afir.path == "ALPENHORN_NODE"
