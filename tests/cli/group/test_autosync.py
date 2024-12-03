"""Test CLI: alpenhorn group autosync"""

from alpenhorn.db import StorageGroup, StorageNode, StorageTransferAction


def test_no_group(clidb, cli):
    """Test autosync with a bad group name."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["group", "autosync", "MISSING", "Node"])


def test_no_node(clidb, cli):
    """Test autosync with a bad node name."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["group", "autosync", "Group", "Node"])


def test_node_in_group(clidb, cli):
    """Can't start autosync with NODE in GROUP."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["group", "autosync", "Group", "Node"])


def test_stop_node_in_group(clidb, cli):
    """But we can stop autosync with NODE in GROUP."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(0, ["group", "autosync", "Group", "Node", "--remove"])


def test_start_noop(clidb, cli):
    """Test autosync already on."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    StorageTransferAction.create(node_from=node, group_to=group, autosync=1)

    cli(0, ["group", "autosync", "Group", "Node"])


def test_stop_noop(clidb, cli):
    """Test stopping autosync already explicity off."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    StorageTransferAction.create(node_from=node, group_to=group, autosync=0)

    cli(0, ["group", "autosync", "Group", "Node", "--remove"])

    assert not StorageTransferAction.get(node_from=node, group_to=group).autosync


def test_start_from_stop(clidb, cli):
    """Test starting autosync already explicitly off."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    StorageTransferAction.create(node_from=node, group_to=group, autosync=0)

    cli(0, ["group", "autosync", "Group", "Node"])

    assert StorageTransferAction.get(node_from=node, group_to=group).autosync


def test_stop_from_start(clidb, cli):
    """Test starting autosync already explicitly off."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    StorageTransferAction.create(node_from=node, group_to=group, autosync=1)

    cli(0, ["group", "autosync", "Group", "Node", "--remove"])

    assert not StorageTransferAction.get(node_from=node, group_to=group).autosync


def test_start_create(clidb, cli):
    """Test starting autosync through record creation."""

    group = StorageGroup.create(name="NodeGroup")
    node = StorageNode.create(name="Node", group=group)
    group = StorageGroup.create(name="Group")

    cli(0, ["group", "autosync", "Group", "Node"])

    assert StorageTransferAction.get(node_from=node, group_to=group).autosync
