"""
test_storage_model
------------------

Tests for `alpenhorn.storage` module.
"""

import yaml
import pytest
from os import path

from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.io.base import BaseNodeIO, BaseGroupIO, BaseNodeRemote

tests_path = path.abspath(path.dirname(__file__))


@pytest.fixture
def load_data(dbproxy):
    """Loads data from tests/fixtures into the connected database"""
    dbproxy.create_tables([StorageGroup, StorageNode])

    # Check we're starting from a clean slate
    assert StorageGroup.select().count() == 0
    assert StorageNode.select().count() == 0

    with open(path.join(tests_path, "fixtures/storage.yml")) as f:
        fixtures = yaml.safe_load(f)

    StorageGroup.insert_many(fixtures["groups"]).execute()
    groups = {group["name"]: group["id"] for group in fixtures["groups"]}

    # fixup foreign keys for the nodes
    for node in fixtures["nodes"]:
        node["group"] = groups[node["group"]]

    # bulk load the nodes
    StorageNode.insert_many(fixtures["nodes"]).execute()

    return fixtures


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


def test_schema(dbproxy, load_data):
    assert set(dbproxy.get_tables()) == {"storagegroup", "storagenode"}


def test_model(load_data):
    groups = set(
        [tuple[0] for tuple in StorageGroup.select(StorageGroup.name).tuples()]
    )
    assert groups == set([group["name"] for group in load_data["groups"]])
    assert StorageGroup.get(StorageGroup.name == "bar").notes == "Some bar!"

    nearline = StorageNode.get(name="nearline")

    for node in StorageNode.select().dicts():
        assert node == _storagenode_dict(node["name"], load_data["nodes"])


def test_ioload(lfs, load_data):
    """Test instantiation of the I/O classes"""

    for node in StorageNode.select().execute():
        io = node.io
        assert isinstance(io, BaseNodeIO)
        remote = node.remote
        assert isinstance(remote, BaseNodeRemote)

    for group in StorageGroup.select().execute():
        io = group.io
        assert isinstance(io, BaseGroupIO)
