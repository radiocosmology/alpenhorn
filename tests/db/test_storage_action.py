"""test alpenhorn.storage."""

import peewee as pw
import pytest

from alpenhorn.db.storage import StorageTransferAction


def test_schema(dbproxy, simplenode, storagetransferaction):
    # Force table creation
    storagetransferaction(node_from=simplenode, group_to=simplenode.group)

    assert set(dbproxy.get_tables()) == {
        "storagegroup",
        "storagenode",
        "storagetransferaction",
    }


def test_edge_model(storagetransferaction, storagenode, storagegroup):
    group1 = storagegroup(name="group1")
    node1 = storagenode(name="node1", group=group1)

    group2 = storagegroup(name="group2")
    node2 = storagenode(name="node2", group=group2)

    storagetransferaction(node_from=node1, group_to=group2)
    storagetransferaction(
        node_from=node2, group_to=group1, autosync=True, autoclean=True
    )

    # (node_from, group_to) is unique
    with pytest.raises(pw.IntegrityError):
        storagetransferaction(node_from=node1, group_to=group2)

    # Check records in DB
    assert StorageTransferAction.select().where(
        StorageTransferAction.node_from == node1,
        StorageTransferAction.group_to == group2,
    ).dicts().get() == {
        "id": 1,
        "node_from": node1.id,
        "group_to": group2.id,
        "autosync": False,
        "autoclean": False,
    }
    assert StorageTransferAction.select().where(
        StorageTransferAction.node_from == node2,
        StorageTransferAction.group_to == group1,
    ).dicts().get() == {
        "id": 2,
        "node_from": node2.id,
        "group_to": group1.id,
        "autosync": True,
        "autoclean": True,
    }


def test_edge_self_loop(storagetransferaction, storagenode, storagegroup):
    """StorageTransferAction.self_loop is True when node_from.group == group_to"""

    group1 = storagegroup(name="group1")
    node1 = storagenode(name="node1", group=group1)

    group2 = storagegroup(name="group2")
    storagenode(name="node2", group=group2)

    # Not a loop
    storagetransferaction(node_from=node1, group_to=group2)

    # Loop
    storagetransferaction(node_from=node1, group_to=group1)

    assert not StorageTransferAction.get(id=1).self_loop
    assert StorageTransferAction.get(id=2).self_loop
