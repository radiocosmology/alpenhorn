"""
test_archive_model
------------------

Tests for `alpenhorn.archive` module.
"""

import pytest
import yaml
import os
from os import path


import alpenhorn.db as db
from alpenhorn.archive import *
import alpenhorn.storage as storage
import alpenhorn.acquisition as acquisition

import test_storage_model as ts
import test_acquisition_model as ta


tests_path = path.abspath(path.dirname(__file__))


def load_fixtures():
    """Loads data from tests/fixtures into the connected database"""
    fs = ts.load_fixtures()
    fa = ta.load_fixtures()

    db.database_proxy.create_tables([ArchiveFileCopy, ArchiveFileCopyRequest])

    # Check we're starting from a clean slate
    assert ArchiveFileCopy.select().count() == 0
    assert ArchiveFileCopyRequest.select().count() == 0
    assert StorageNode.select().count() != 0

    with open(path.join(tests_path, "fixtures/archive.yml")) as f:
        fixtures = yaml.safe_load(f)

    # fixup foreign keys for the file copies
    for copy in fixtures["file_copies"]:
        copy["file"] = fa["files"][copy["file"]]
        copy["node"] = fs["nodes"][copy["node"]]
        copy["size_b"] = 512

    # bulk load the file copies
    ArchiveFileCopy.insert_many(fixtures["file_copies"]).execute()
    file_copies = dict(
        ArchiveFileCopy.select(ArchiveFileCopy.file, ArchiveFileCopy.id).tuples()
    )

    # fixup foreign keys for the copy requests
    for req in fixtures["copy_requests"]:
        req["file"] = fa["files"][req["file"]]
        req["node_from"] = fs["nodes"][req["node_from"]]
        req["group_to"] = fs["groups"][req["group_to"]]

    # bulk load the file copies
    ArchiveFileCopyRequest.insert_many(fixtures["copy_requests"]).execute()
    copy_requests = list(
        ArchiveFileCopyRequest.select(
            ArchiveFileCopyRequest.file,
            ArchiveFileCopyRequest.node_from,
            ArchiveFileCopyRequest.group_to,
        ).tuples()
    )

    return {"file_copies": file_copies, "copy_requests": copy_requests}


@pytest.fixture
def fixtures():
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db._connect()

    yield load_fixtures()

    db.database_proxy.close()


def test_schema(fixtures):
    assert set(db.database_proxy.get_tables()) == {
        u"storagegroup",
        u"storagenode",
        u"acqtype",
        u"archiveacq",
        u"filetype",
        u"archivefile",
        u"archivefilecopyrequest",
        u"archivefilecopy",
    }


def test_model(fixtures):
    copies = set(
        ArchiveFileCopy.select(ArchiveFile.name, StorageNode.name)
        .join(ArchiveFile)
        .switch(ArchiveFileCopy)
        .join(StorageNode)
        .tuples()
    )
    assert copies == {("fred", "x"), ("sheila", "x")}

    reqs = set(
        ArchiveFileCopyRequest.select(
            ArchiveFile.name, StorageNode.name, StorageGroup.name
        )
        .join(ArchiveFile)
        .switch(ArchiveFileCopyRequest)
        .join(StorageNode)
        .switch(ArchiveFileCopyRequest)
        .join(StorageGroup)
        .tuples()
    )
    assert reqs == {("jim", "x", "bar")}

    assert list(
        ArchiveFileCopy.select(
            ArchiveFile.name, ArchiveFileCopy.has_file, ArchiveFileCopy.wants_file
        )
        .join(ArchiveFile)
        .where(ArchiveFile.name == "fred")
        .dicts()
    ) == [{"name": "fred", "has_file": "N", "wants_file": "Y"}]
    assert (
        ArchiveFileCopy.select()
        .join(ArchiveFile)
        .where(ArchiveFile.name == "sheila")
        .get()
        .wants_file
    ) == "M"


def test_unique_copy_constraint(fixtures):
    f = ArchiveFile.get(name="fred")
    assert f.name == "fred"
    n = StorageNode.get(name="x")
    assert n.name == "x"

    # we have one copy
    assert [fc for fc in f.copies] == [ArchiveFileCopy.get(file=f, node=n)]

    # but can't insert a second with identical file and node:
    with pytest.raises(pw.IntegrityError):
        file_copy = ArchiveFileCopy.create(file=f, node=n)
        file_copy.save()
