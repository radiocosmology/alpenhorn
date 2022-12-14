"""
test_storage_model
------------------

Tests for `alpenhorn.storage` module.
"""

from os import path

import pytest
import yaml

from alpenhorn.storage import StorageGroup, StorageNode

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
    groups = dict(StorageGroup.select(StorageGroup.name, StorageGroup.id).tuples())

    # fixup foreign keys for the nodes
    for node in fixtures["nodes"]:
        node["group"] = groups[node["group"]]

    # bulk load the nodes
    StorageNode.insert_many(fixtures["nodes"]).execute()
    nodes = dict(StorageNode.select(StorageNode.name, StorageNode.id).tuples())

    return {"groups": groups, "nodes": nodes}


def test_schema(dbproxy, load_data):
    assert set(dbproxy.get_tables()) == {"storagegroup", "storagenode"}


def test_model(load_data):
    groups = set(StorageGroup.select(StorageGroup.name).tuples())
    assert groups == {("foo",), ("bar",), ("transport",)}
    assert StorageGroup.get(StorageGroup.name == "bar").notes == "Some bar!"

    nodes = list(StorageNode.select().dicts())
    assert nodes == [
        {
            "id": 1,
            "name": "x",
            "group": 1,
            "host": "foo.example.com",
            "address": None,
            "io_class": None,
            "io_config": None,
            "auto_import": False,
            "auto_verify": 0,
            "storage_type": "A",
            "active": True,
            "root": None,
            "username": None,
            "notes": None,
            "max_total_gb": 10.0,
            "min_avail_gb": 1.0,
            "avail_gb": None,
            "avail_gb_last_checked": None,
        },
        {
            "id": 2,
            "name": "z",
            "group": 2,
            "host": "bar.example.com",
            "address": None,
            "io_class": None,
            "io_config": None,
            "auto_import": False,
            "auto_verify": 0,
            "storage_type": "A",
            "active": False,
            "root": None,
            "username": None,
            "notes": None,
            "max_total_gb": 10.0,
            "min_avail_gb": 1.0,
            "avail_gb": None,
            "avail_gb_last_checked": None,
        },
    ]
