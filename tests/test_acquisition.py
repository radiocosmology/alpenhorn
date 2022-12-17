"""
test_acquisition_model
------------------

Tests for `alpenhorn.acquisition` module.
"""

import pathlib
import pytest
import datetime
import peewee as pw

from alpenhorn.acquisition import AcqType, ArchiveAcq, ArchiveFile, FileType


def test_schema(dbproxy, genericfile):
    assert set(dbproxy.get_tables()) == {
        "acqtype",
        "archiveacq",
        "filetype",
        "archivefile",
    }


def test_acqtype_model(acqtype):
    acqtype(name="min")
    acqtype(name="max", notes="Note")

    # name is unique
    with pytest.raises(pw.IntegrityError):
        acqtype(name="min")

    # Check records in DB
    assert AcqType.select().where(AcqType.name == "min").dicts().get() == {
        "id": 1,
        "notes": None,
        "name": "min",
    }
    assert AcqType.select().where(AcqType.name == "max").dicts().get() == {
        "id": 2,
        "notes": "Note",
        "name": "max",
    }


def test_acq_model(acqtype, archiveacq):
    at = acqtype(name="type")
    archiveacq(name="min", type=at)
    archiveacq(name="max", type=at, comment="Comment, apparently, is not a note")

    # name is unique
    with pytest.raises(pw.IntegrityError):
        archiveacq(name="min", type=at)

    # Check records in DB
    assert ArchiveAcq.select().where(ArchiveAcq.name == "min").dicts().get() == {
        "id": 1,
        "name": "min",
        "type": at.id,
        "comment": None,
    }
    assert ArchiveAcq.select().where(ArchiveAcq.name == "max").dicts().get() == {
        "id": 2,
        "name": "max",
        "type": at.id,
        "comment": "Comment, apparently, is not a note",
    }


def test_filetype_model(filetype):
    filetype(name="min")
    filetype(name="max", notes="Note")

    # name is unique
    with pytest.raises(pw.IntegrityError):
        filetype(name="min")

    # Check records in DB
    assert FileType.select().where(FileType.name == "min").dicts().get() == {
        "id": 1,
        "notes": None,
        "name": "min",
    }
    assert FileType.select().where(FileType.name == "max").dicts().get() == {
        "id": 2,
        "notes": "Note",
        "name": "max",
    }


def test_file_model(acqtype, archiveacq, filetype, archivefile):
    at = acqtype(name="type")
    acq1 = archiveacq(name="acq1", type=at)
    ft = filetype(name="type")
    before = datetime.datetime.now().replace(microsecond=0)
    archivefile(name="min", acq=acq1, type=ft)
    after = datetime.datetime.now()
    archivefile(
        name="max", acq=acq1, type=ft, md5sum="123456789", registered=after, size_b=45
    )

    # Check records in DB
    af = ArchiveFile.select().where(ArchiveFile.name == "min").dicts().get()

    # Registered should be the time of record creation
    assert af["registered"] >= before
    assert af["registered"] <= after
    del af["registered"]  # Don't bother matching exactly

    assert af == {
        "id": 1,
        "acq": acq1.id,
        "name": "min",
        "type": at.id,
        "md5sum": None,
        "size_b": None,
    }
    assert ArchiveFile.select().where(ArchiveFile.name == "max").dicts().get() == {
        "id": 2,
        "acq": acq1.id,
        "name": "max",
        "type": at.id,
        "md5sum": "123456789",
        "registered": after,
        "size_b": 45,
    }

    # (acq, name) is unique
    with pytest.raises(pw.IntegrityError):
        archivefile(name="min", acq=acq1, type=ft)
    # But this should work
    acq2 = archiveacq(name="acq2", type=at)
    archivefile(name="min", acq=acq2, type=ft)


def test_file_path(genericacq, archivefile, filetype):
    """Test ArchiveFile.path."""
    ft = filetype(name="type")
    file = archivefile(name="file", acq=genericacq, type=ft)

    assert file.path == pathlib.PurePath(genericacq.name, "file")


def test_archive_count(genericgroup, storagenode, genericfile, archivefilecopy):
    """Test ArchiveFile.archive_count()"""

    # Create a bunch of copies in various states on various node types
    archivefilecopy(
        file=genericfile,
        node=storagenode(name="n1", group=genericgroup, storage_type="A"),
        has_file="N",
    )
    assert genericfile.archive_count() == 0
    archivefilecopy(
        file=genericfile,
        node=storagenode(name="n2", group=genericgroup, storage_type="F"),
        has_file="Y",
    )
    assert genericfile.archive_count() == 0
    archivefilecopy(
        file=genericfile,
        node=storagenode(name="n3", group=genericgroup, storage_type="T"),
        has_file="Y",
    )
    assert genericfile.archive_count() == 0
    archivefilecopy(
        file=genericfile,
        node=storagenode(name="n4", group=genericgroup, storage_type="F"),
        has_file="Y",
    )
    assert genericfile.archive_count() == 0
    archivefilecopy(
        file=genericfile,
        node=storagenode(name="n5", group=genericgroup, storage_type="A"),
        has_file="Y",
    )
    assert genericfile.archive_count() == 1
    archivefilecopy(
        file=genericfile,
        node=storagenode(name="n6", group=genericgroup, storage_type="A"),
        has_file="X",
    )
    assert genericfile.archive_count() == 1
    archivefilecopy(
        file=genericfile,
        node=storagenode(name="n7", group=genericgroup, storage_type="A"),
        has_file="M",
    )
    assert genericfile.archive_count() == 1
    archivefilecopy(
        file=genericfile,
        node=storagenode(name="n8", group=genericgroup, storage_type="A"),
        has_file="Y",
    )
    assert genericfile.archive_count() == 2
