"""
test_acquisition_model
------------------

Tests for `alpenhorn.acquisition` module.
"""

import datetime as dt
import pytest
import yaml
from os import path
import peewee as pw

import alpenhorn.db as db
from alpenhorn.acquisition import ArchiveAcq, AcqType, FileType, ArchiveFile

tests_path = path.abspath(path.dirname(__file__))


def load_fixtures():
    """Loads data from tests/fixtures into the connected database"""
    db.database_proxy.create_tables([ArchiveAcq, AcqType, FileType, ArchiveFile])

    # Check we're starting from a clean slate
    assert ArchiveAcq.select().count() == 0
    assert AcqType.select().count() == 0

    with open(path.join(tests_path, "fixtures/acquisition.yml")) as f:
        fixtures = yaml.safe_load(f)

    AcqType.insert_many(fixtures["types"]).execute()
    types = dict(AcqType.select(AcqType.name, AcqType.id).tuples())

    # fixup foreign keys for the acquisitions
    for ack in fixtures["acquisitions"]:
        ack["type"] = types[ack["type"]]

    ArchiveAcq.insert_many(fixtures["acquisitions"]).execute()
    acqs = dict(ArchiveAcq.select(ArchiveAcq.name, ArchiveAcq.id).tuples())

    FileType.insert_many(fixtures["file_types"]).execute()
    file_types = dict(FileType.select(FileType.name, FileType.id).tuples())

    # fixup foreign keys for the files
    for file in fixtures["files"]:
        file["acq"] = acqs[file["acq"]]
        file["type"] = file_types[file["type"]]

    ArchiveFile.insert_many(fixtures["files"]).execute()
    files = dict(ArchiveFile.select(ArchiveFile.name, ArchiveFile.id).tuples())

    return {"types": types, "file_types": file_types, "files": files}


@pytest.fixture
def fixtures():
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db._connect()

    yield load_fixtures()

    db.database_proxy.close()


def test_schema(fixtures):
    assert set(db.database_proxy.get_tables()) == {
        u"acqtype",
        u"archiveacq",
        u"filetype",
        u"archivefile",
    }


def test_model(fixtures):

    assert list(ArchiveAcq.select()) == [ArchiveAcq.get(ArchiveAcq.name == "x")]

    files = set(ArchiveFile.select(ArchiveFile.name).tuples())
    assert files == {("fred",), ("jim",), ("sheila",)}

    freds = list(ArchiveFile.select().where(ArchiveFile.name == "fred").dicts())

    # the archive file should be registered in the last 5 seconds
    now = dt.datetime.now()
    for f in freds:
        diff = now - f["registered"]
        assert diff.days == 0 and diff.seconds <= 5
        # we don't need to check the registered time in the rest of the test
        del f["registered"]

    assert freds == [
        {"id": 1, "name": "fred", "acq": 1, "type": 1, "md5sum": None, "size_b": None}
    ]


def test_registered(fixtures):
    """Verifies that registered times are unique per each ArchiveFile instance"""
    assert (
        ArchiveFile.select(pw.fn.Count(pw.fn.Distinct(ArchiveFile.registered))).scalar()
    ) > 1
