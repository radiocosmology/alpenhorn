"""
test_storage_model
------------------

Tests for `alpenhorn.storage` module.
"""

import pytest
import yaml
from os import path

import alpenhorn.db as db
from alpenhorn.storage import *


tests_path = path.abspath(path.dirname(__file__))

def load_fixtures():
    """Loads data from tests/fixtures into the connected database"""
    db.database_proxy.create_tables([StorageGroup, StorageNode])

    # Check we're starting from a clean slate
    assert StorageGroup.select().count() == 0
    assert StorageNode.select().count() == 0

    with open(path.join(tests_path, 'fixtures/storage.yml')) as f:
        fixtures = yaml.safe_load(f)

    StorageGroup.insert_many(fixtures['groups']).execute()
    groups = dict(StorageGroup.select(StorageGroup.name, StorageGroup.id).tuples())

    # fixup foreign keys for the nodes
    for node in fixtures['nodes']:
        node['group']= groups[node['group']]

    # bulk load the nodes
    StorageNode.insert_many(fixtures['nodes']).execute()
    nodes = dict(StorageNode.select(StorageNode.name, StorageNode.id).tuples())

    return {'groups': groups, 'nodes': nodes}


@pytest.fixture
def fixtures():
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db._connect()

    yield load_fixtures()

    db.database_proxy.close()


def test_schema(fixtures):
    assert set(db.database_proxy.get_tables() )== { u'storagegroup', u'storagenode' }


def test_model(fixtures):
    groups = set(StorageGroup.select(StorageGroup.name).tuples())
    assert groups == { ( 'foo', ), ( 'bar', ), ('transport', ) }
    assert StorageGroup.get(StorageGroup.name == 'bar').notes == 'Some bar!'

    nodes = list(StorageNode.select().dicts())
    assert nodes == [
        {
            'id': 1, 'name': 'x', 'group': 1,
            'host': 'foo.example.com', 'address': None,
            'auto_import': False, 'storage_type': 'A',
            'active': True, 'root': None, 'suspect': False,
            'username': None, 'notes': None,
            'max_total_gb': 10, 'min_avail_gb': 1,
            'avail_gb': None, 'avail_gb_last_checked': None,
            'min_delete_age_days': 30,
        },
        {
            'id': 2, 'name': 'z', 'group': 2,
            'host': 'bar.example.com', 'address': None,
            'auto_import': False, 'storage_type': 'A',
            'active': False, 'root': None, 'suspect': False,
            'username': None, 'notes': None,
            'max_total_gb': 10, 'min_avail_gb': 1,
            'avail_gb': None, 'avail_gb_last_checked': None,
            'min_delete_age_days': 30,
        }
    ]
