"""
test_acquisition_model
------------------

Tests for `alpenhorn.acquisition` module.
"""

import pytest
import yaml
from os import path

import alpenhorn.db as db
from alpenhorn.acquisition import *

tests_path = path.abspath(path.dirname(__file__))

@pytest.fixture
def fixtures():
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db.connect()
    db.database_proxy.create_tables([ArchiveAcq, ArchiveInst, AcqType])

    # Check we're starting from a clean slate
    assert ArchiveAcq.select().count() == 0
    assert ArchiveInst.select().count() == 0
    assert AcqType.select().count() == 0

    with open(path.join(tests_path, 'fixtures/acquisition.yml')) as f:
        fixtures = yaml.load(f)

    ArchiveInst.insert_many(fixtures['instruments']).execute()
    instruments = {}
    for inst in ArchiveInst.select(ArchiveInst.name, ArchiveInst.id).dicts():
        instruments[inst['name']] = inst['id']

    AcqType.insert_many(fixtures['types']).execute()
    types = {}
    for type in AcqType.select(AcqType.name, AcqType.id).dicts():
        types[type['name']] = type['id']

    # fixup foreign keys for the acquisitions
    for ack in fixtures['acquisitions']:
        ack['inst']= instruments[ack['inst']]
        ack['type']= types[ack['type']]

    # bulk load the acquisitions
    ArchiveAcq.insert_many(fixtures['acquisitions']).execute()

    yield

    # cleanup
    db.database_proxy.close()


def test_schema(fixtures):
    assert set(db.database_proxy.get_tables()) == { u'acqtype', u'archiveinst', u'archiveacq' }


def test_model(fixtures):
    instruments = ArchiveInst.select(ArchiveInst.name)
    assert instruments == ['foo', 'bar']
    assert ArchiveInst.get(ArchiveInst.name == 'foo').notes is None
    
    assert [ ArchiveAcq.select() ] == [ ArchiveAcq.get(ArchiveAcq.name ==  'x')]
