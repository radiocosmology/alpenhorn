"""
test_storage_model
------------------

Tests for `alpenhorn.storage` module.
"""

import pytest

from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.io.base import BaseNodeIO, BaseGroupIO, BaseNodeRemote


def _storagenode_dict(name, nodes):
    """Given the loaded YAML node list "nodes", return a full
    StorageNode dict for node "name"."""
    _node_default = {
        "active": False,
        "address": None,
        "auto_import": False,
        "auto_verify": 0,
        "avail_gb": None,
        "avail_gb_last_checked": None,
        "host": None,
        "io_class": None,
        "io_config": None,
        "max_total_gb": -1.0,
        "min_avail_gb": None,
        "notes": None,
        "root": None,
        "storage_type": "A",
        "username": None,
    }
    for id_, node in enumerate(nodes):
        if node["name"] == name:
            node["id"] = 1 + id_
            for key, value in _node_default.items():
                node.setdefault(key, value)
            return node

    return None


def test_schema(dbproxy, storage_data):
    assert set(dbproxy.get_tables()) == {"storagegroup", "storagenode"}


def test_model(storage_data):
    groups = set(
        [tuple[0] for tuple in StorageGroup.select(StorageGroup.name).tuples()]
    )
    assert groups == set([group["name"] for group in storage_data["groups"]])
    assert StorageGroup.get(StorageGroup.name == "bar").notes == "Some bar!"

    nearline = StorageNode.get(name="nearline")

    for node in StorageNode.select().dicts():
        assert node == _storagenode_dict(node["name"], storage_data["nodes"])


def test_ioload(lfs, storage_data):
    """Test instantiation of the I/O classes"""

    for node in StorageNode.select().execute():
        io = node.io
        assert isinstance(io, BaseNodeIO)
        remote = node.remote
        assert isinstance(remote, BaseNodeRemote)

    for group in StorageGroup.select().execute():
        io = group.io
        assert isinstance(io, BaseGroupIO)
