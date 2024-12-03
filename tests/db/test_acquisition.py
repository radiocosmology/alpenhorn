"""Tests for `alpenhorn.acquisition` module."""

import pathlib

import peewee as pw
import pytest

from alpenhorn.db.acquisition import ArchiveAcq, ArchiveFile


def test_schema(dbproxy, simplefile):
    assert set(dbproxy.get_tables()) == {
        "archiveacq",
        "archivefile",
    }


def test_acq_model(archiveacq):
    """Test the ArchiveAcq table model."""
    archiveacq(name="min")
    archiveacq(name="max", comment="Comment, apparently, is not a note")

    # name is unique
    with pytest.raises(pw.IntegrityError):
        archiveacq(name="min")

    # Check records in DB
    assert ArchiveAcq.select().where(ArchiveAcq.name == "min").dicts().get() == {
        "id": 1,
        "name": "min",
        "comment": None,
    }
    assert ArchiveAcq.select().where(ArchiveAcq.name == "max").dicts().get() == {
        "id": 2,
        "name": "max",
        "comment": "Comment, apparently, is not a note",
    }


def test_file_model(archiveacq, archivefile):
    acq1 = archiveacq(name="acq1")
    before = pw.utcnow().replace(microsecond=0)
    archivefile(name="min", acq=acq1)
    after = pw.utcnow()
    archivefile(
        name="max",
        acq=acq1,
        md5sum="123456789",
        registered=after,
        size_b=45,
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
        "md5sum": None,
        "size_b": None,
    }
    assert ArchiveFile.select().where(ArchiveFile.name == "max").dicts().get() == {
        "id": 2,
        "acq": acq1.id,
        "name": "max",
        "md5sum": "123456789",
        "registered": after,
        "size_b": 45,
    }

    # (acq, name) is unique
    with pytest.raises(pw.IntegrityError):
        archivefile(name="min", acq=acq1)
    # But this should work
    acq2 = archiveacq(name="acq2")
    archivefile(name="min", acq=acq2)


def test_file_path(simpleacq, archivefile):
    """Test ArchiveFile.path."""
    file = archivefile(name="file", acq=simpleacq)

    assert file.path == pathlib.PurePath(simpleacq.name, "file")


def test_archive_count(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test ArchiveFile.archive_count"""

    # Create a bunch of copies in various states on various node types
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n1", group=simplegroup, storage_type="A"),
        has_file="N",
    )
    assert simplefile.archive_count == 0
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n2", group=simplegroup, storage_type="F"),
        has_file="Y",
    )
    assert simplefile.archive_count == 0
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n3", group=simplegroup, storage_type="T"),
        has_file="Y",
    )
    assert simplefile.archive_count == 0
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n4", group=simplegroup, storage_type="F"),
        has_file="Y",
    )
    assert simplefile.archive_count == 0
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n5", group=simplegroup, storage_type="A"),
        has_file="Y",
    )
    assert simplefile.archive_count == 1
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n6", group=simplegroup, storage_type="A"),
        has_file="X",
    )
    assert simplefile.archive_count == 1
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n7", group=simplegroup, storage_type="A"),
        has_file="M",
    )
    assert simplefile.archive_count == 1
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n8", group=simplegroup, storage_type="A"),
        has_file="Y",
    )
    assert simplefile.archive_count == 2
