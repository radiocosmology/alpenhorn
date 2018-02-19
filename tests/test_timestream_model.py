"""
test_timestream_model
------------------

Tests for `alpenhorn.timestream` module.
"""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import pytest
import yaml
import os
from os import path


import alpenhorn.db as db
from alpenhorn.acquisition import *
from alpenhorn.timestream import *
# from alpenhorn.db_tables import *

tests_path = path.abspath(path.dirname(__file__))

@pytest.fixture
def fixtures(clear_db=True):
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    if (clear_db):
        db._connect()
    db.database_proxy.create_tables([ArchiveAcq, AcqType, FileType, ArchiveFile, TimestreamFileInfo, TimestreamAcqInfo], safe=not clear_db)

    # Check we're starting from a clean slate
    assert ArchiveAcq.select().count() == 0
    assert AcqType.select().count() == 0

    with open(path.join(tests_path, 'fixtures/timestream.yml')) as f:
        fixtures = yaml.load(f)
    print(fixtures)

    AcqType.insert_many(fixtures['types']).execute()
    types = dict(AcqType.select(AcqType.name, AcqType.id).tuples())

    # fixup foreign keys for the acquisitions
    for ack in fixtures['acquisitions']:
        ack['type'] = types[ack['type']]

    ArchiveAcq.insert_many(fixtures['acquisitions']).execute()
    acqs = dict(ArchiveAcq.select(ArchiveAcq.name, ArchiveAcq.id).tuples())

    FileType.insert_many(fixtures['file_types']).execute()
    file_types = dict(FileType.select(FileType.name, FileType.id).tuples())

    # fixup foreign keys for the files
    for file in fixtures['files']:
        file['acq'] = acqs[file['acq']]
        file['type'] = file_types[file['type']]

    ArchiveFile.insert_many(fixtures['files']).execute()
    files = dict(ArchiveFile.select(ArchiveFile.name, ArchiveFile.id).tuples())
    print(files['apple'])

    # fixup foreign keys for timestreamfileinfo
    for tsfileinfo in fixtures['timestreamfileinfo']:
        print(tsfileinfo['file'])
        # tsfileinfo['file'] = files[tsfileinfo['name']]
        #tsfileinfo['name'] = files[tsfileinfo['name']]
        #tsfileinfo['acq'] = acqs[tsfileinfo['acq']]
        #tsfileinfo['type'] = types[tsfileinfo['type']]
        #['file'] = files[tsfileinfo['file']]

    TimestreamFileInfo.insert_many(fixtures['timestreamfileinfo']).execute()

    # fixup foreign keys for timestreamfileinfo
    for tsacqinfo in fixtures['timestreamacqinfo']:
        tsacqinfo['acq'] = acqs[tsacqinfo['acq']]

    TimestreamAcqInfo.insert_many(fixtures['timestreamacqinfo']).execute()

    yield {'types': types, 'file_types': file_types, 'files': files}

    # cleanup
    if clear_db:
        db.database_proxy.close()


def test_schema(fixtures):
    assert set(db.database_proxy.get_tables()) == {
        u'acqtype', u'archiveacq',
        u'filetype', u'archivefile', u'timestreamfileinfo', u'timestreamacqinfo'
    }


def test_model(fixtures):

    assert list(ArchiveAcq.select()) == [
        ArchiveAcq.get(ArchiveAcq.name == 'x')
    ]

    files = set(ArchiveFile.select(ArchiveFile.name).tuples())
    assert files == { ( 'fred', ), ( 'jim', ), ( 'sheila', ) }

    freds = list(ArchiveFile.select().where(ArchiveFile.name == 'fred').dicts())
    assert freds == [
        { 'id': 1, 'name': 'fred', 'acq': 1, 'type': 1, 'md5sum': None, 'size_b': None}
    ]
