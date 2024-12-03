"""Test CLI: alpenhorn node autoclean"""

from alpenhorn.db import StorageGroup, StorageNode, StorageTransferAction


def test_no_node(clidb, cli):
    """Test autoclean with a bad node name."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["node", "autoclean", "MISSING", "Group"])


def test_no_group(clidb, cli):
    """Test autoclean with a bad group name."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["node", "autoclean", "Node", "MISSING"])


def test_node_in_group(clidb, cli):
    """Can't add autoclean with NODE in GROUP."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["node", "autoclean", "Node", "Group"])


def test_remove_node_in_group(clidb, cli):
    """But we can remove autoclean with NODE in GROUP."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(0, ["node", "autoclean", "Node", "Group", "--remove"])


def test_add_noop(clidb, cli):
    """Test autoclean already on."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    StorageTransferAction.create(node_from=node, group_to=group, autoclean=1)

    cli(0, ["node", "autoclean", "Node", "Group"])


def test_remove_noop(clidb, cli):
    """Test removing autoclean already explicity off."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    StorageTransferAction.create(node_from=node, group_to=group, autoclean=0)

    cli(0, ["node", "autoclean", "Node", "Group", "--remove"])

    assert not StorageTransferAction.get(node_from=node, group_to=group).autoclean


def test_add_from_remove(clidb, cli):
    """Test adding autoclean already explicitly off."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    StorageTransferAction.create(node_from=node, group_to=group, autoclean=0)

    cli(0, ["node", "autoclean", "Node", "Group"])

    assert StorageTransferAction.get(node_from=node, group_to=group).autoclean


def test_remove_from_add(clidb, cli):
    """Test adding autoclean already explicitly off."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    StorageTransferAction.create(node_from=node, group_to=group, autoclean=1)

    cli(0, ["node", "autoclean", "Node", "Group", "--remove"])

    assert not StorageTransferAction.get(node_from=node, group_to=group).autoclean


def test_add_create(clidb, cli):
    """Test adding autoclean through record creation."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    cli(0, ["node", "autoclean", "Node", "Group"])

    assert StorageTransferAction.get(node_from=node, group_to=group).autoclean
