"""
test_archive_model
------------------

Tests for `alpenhorn.archive` module.
"""

import pytest
import yaml
import os
from os import path

# TODO: use Pytest's directory used for tmpdir/basedir, not '/tmp'
os.environ['ALPENHORN_LOG_FILE'] = '/tmp' + '/alpenhornd.log'

import alpenhorn.db as db
from alpenhorn.archive import *
import alpenhorn.storage as storage
import alpenhorn.acquisition as acquisition

import test_storage_model as ts
import test_acquisition_model as ta

class SqliteEnumField(pw.CharField):
    """Implements an enum field for the ORM.

    Why doesn't peewee support enums? That's dumb. We should make one."""

    def __init__(self, choices, *args, **kwargs):
        super(SqliteEnumField, self).__init__(*args, **kwargs)
        self.choices = choices

    def coerce(self, val):
        if val is None:
            return str(self.default)
        if val not in self.choices:
            raise ValueError("Invalid enum value '%s'" % val)
        return str(val)


# Use Sqlite-compatible EnumField
SqliteEnumField(['N', 'Y', 'M', 'X'], default='N').add_to_class(ArchiveFileCopy, 'has_file')
SqliteEnumField(['Y', 'M', 'N'], default='Y').add_to_class(ArchiveFileCopy, 'wants_file')

tests_path = path.abspath(path.dirname(__file__))


@pytest.fixture
def fixtures():
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""

    db.connect()

    fs = ts.fixtures(False).next()
    fa = ta.fixtures(False).next()

    db.database_proxy.create_tables([ArchiveFileCopy, ArchiveFileCopyRequest ])

    # Check we're starting from a clean slate
    assert ArchiveFileCopy.select().count() == 0
    assert ArchiveFileCopyRequest.select().count() == 0
    assert StorageNode.select().count() != 0

    with open(path.join(tests_path, 'fixtures/archive.yml')) as f:
        fixtures = yaml.load(f)

    # fixup foreign keys for the file copies
    for copy in fixtures['file_copies']:
        copy['file']= fa['files'][copy['file']]
        copy['node']= fs['nodes'][copy['node']]

    # bulk load the file copies
    ArchiveFileCopy.insert_many(fixtures['file_copies']).execute()
    file_copies = dict(ArchiveFileCopy.select(ArchiveFileCopy.file, ArchiveFileCopy.id).tuples())

    # fixup foreign keys for the copy requests
    for req in fixtures['copy_requests']:
        req['file']= fa['files'][req['file']]
        req['node_from']= fs['nodes'][req['node_from']]
        req['group_to']= fs['groups'][req['group_to']]

    # bulk load the file copies
    ArchiveFileCopyRequest.insert_many(fixtures['copy_requests']).execute()
    copy_requests = list(ArchiveFileCopyRequest.select(ArchiveFileCopyRequest.file,
                                                       ArchiveFileCopyRequest.node_from,
                                                       ArchiveFileCopyRequest.group_to).tuples())

    yield {
        'file_copies': file_copies,
        'copy_requests': copy_requests
    }

    # cleanup
    db.database_proxy.close()


def test_schema(fixtures):
    assert set(db.database_proxy.get_tables() )== {
        u'storagegroup', u'storagenode',
        u'acqtype', u'archiveinst', u'archiveacq',
        u'filetype', u'archivefile',
        u'archivefilecopyrequest', u'archivefilecopy'
    }


def test_model(fixtures):
    copies = set(ArchiveFileCopy
                 .select(ArchiveFile.name, StorageNode.name)
                 .join(ArchiveFile)
                 .switch(ArchiveFileCopy)
                 .join(StorageNode).tuples())
    assert copies == { ('fred', 'x'), ('sheila', 'x')}

    reqs = set(ArchiveFileCopyRequest
               .select(ArchiveFile.name, StorageNode.name, StorageGroup.name)
               .join(ArchiveFile)
               .switch(ArchiveFileCopyRequest)
               .join(StorageNode)
               .switch(ArchiveFileCopyRequest)
               .join(StorageGroup)
               .tuples())
    assert reqs == { ('jim', 'x', 'bar') }

    assert ArchiveFileCopy.select().join(ArchiveFile).where(ArchiveFile.name == 'sheila').get().wants_file == 'M'
