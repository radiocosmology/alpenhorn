"""
test_archive_model
------------------

Tests for `alpenhorn.archive` module.
"""

import pytest
import peewee as pw

from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.archive import ArchiveFile, ArchiveFileCopy, ArchiveFileCopyRequest


def test_schema(dbproxy, archive_data):
    assert set(dbproxy.get_tables()) == {
        "storagegroup",
        "storagenode",
        "acqtype",
        "archiveacq",
        "filetype",
        "archivefile",
        "archivefilecopyrequest",
        "archivefilecopy",
    }


def test_model(archive_data):
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


def test_unique_copy_constraint(archive_data):
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
