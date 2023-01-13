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
from alpenhorn.info_base import _NoInfo


def test_schema(dbproxy, simplefile):
    assert set(dbproxy.get_tables()) == {
        "acqfiletypes",
        "acqtype",
        "archiveacq",
        "filetype",
        "archivefile",
    }


def test_acqtype_model(acqtype):
    acqtype(name="min")
    acqtype(
        name="max",
        priority=-1,
        notes="Note",
        info_class="InfoClass",
        info_config='{"info": "config"}',
    )

    # name is unique
    with pytest.raises(pw.IntegrityError):
        acqtype(name="min")

    # Check records in DB
    assert AcqType.select().where(AcqType.name == "min").dicts().get() == {
        "id": 1,
        "info_class": None,
        "info_config": None,
        "notes": None,
        "name": "min",
        "priority": 0,
    }
    assert AcqType.select().where(AcqType.name == "max").dicts().get() == {
        "id": 2,
        "info_class": "InfoClass",
        "info_config": '{"info": "config"}',
        "notes": "Note",
        "name": "max",
        "priority": -1,
    }


def test_acqtype_acqinfo_importerror(acqtype):
    """Test AcqType.info() raising ImportError."""

    ats = [
        acqtype(name="at1", info_class="NoClass"),
        acqtype(name="at2", info_class="NoModule.Class"),
        acqtype(name="at3", info_class="alpenhorn.acquisition.NoClass"),
    ]

    for at in ats:
        with pytest.raises(ImportError):
            at.info()


@pytest.mark.alpenhorn_config({"model": {"acq_info_errors": "skip"}})
def test_acqtype_acqinfo_skip(set_config, acqtype):
    """Test AcqType.info() with info_errors skip"""

    at = acqtype(name="name", info_class="NoClass")

    info = at.info()
    assert issubclass(info, _NoInfo)


@pytest.mark.alpenhorn_config({"model": {"acq_info_errors": "ignore"}})
def test_acqtype_acqinfo_ignore(set_config, acqtype):
    """Test AcqType.info() with info_errors skip"""

    at = acqtype(name="name", info_class="NoClass")

    info = at.info()
    assert issubclass(info, _NoInfo)


def test_acqtype_filetypes_empty(simpleacqtype, simplefiletype):
    """Test AcqType.file_types() returning no matches."""
    assert list(simpleacqtype.file_types) == list()


def test_acqtype_filetypes(simpleacqtype, acqfiletypes, filetype):
    """Test AcqType.file_types()."""

    # Add a bunch of filetypes to the acqtype
    filetypes = [
        filetype(name="1"),
        filetype(name="2"),
        filetype(name="3"),
    ]
    for ft in filetypes:
        acqfiletypes(acq_type=simpleacqtype, file_type=ft)

    assert list(simpleacqtype.file_types) == filetypes


def test_acq_model(simpleacqtype, archiveacq):
    archiveacq(name="min", type=simpleacqtype)
    archiveacq(
        name="max", type=simpleacqtype, comment="Comment, apparently, is not a note"
    )

    # name is unique
    with pytest.raises(pw.IntegrityError):
        archiveacq(name="min", type=simpleacqtype)

    # Check records in DB
    assert ArchiveAcq.select().where(ArchiveAcq.name == "min").dicts().get() == {
        "id": 1,
        "name": "min",
        "type": simpleacqtype.id,
        "comment": None,
    }
    assert ArchiveAcq.select().where(ArchiveAcq.name == "max").dicts().get() == {
        "id": 2,
        "name": "max",
        "type": simpleacqtype.id,
        "comment": "Comment, apparently, is not a note",
    }


def test_filetype_model(filetype):
    filetype(name="min")
    filetype(
        name="max",
        notes="Note",
        info_class="InfoClass",
        info_config='{"info": "config"}',
        priority=-1,
    )

    # name is unique
    with pytest.raises(pw.IntegrityError):
        filetype(name="min")

    # Check records in DB
    assert FileType.select().where(FileType.name == "min").dicts().get() == {
        "id": 1,
        "info_class": None,
        "info_config": None,
        "notes": None,
        "name": "min",
        "priority": 0,
    }
    assert FileType.select().where(FileType.name == "max").dicts().get() == {
        "id": 2,
        "info_class": "InfoClass",
        "info_config": '{"info": "config"}',
        "notes": "Note",
        "name": "max",
        "priority": -1,
    }


def test_filetype_info_importerror(filetype):
    """Test FileType.info() raising ImportError."""

    fts = [
        filetype(name="ft1", info_class="NoClass"),
        filetype(name="ft2", info_class="NoModule.Class"),
        filetype(name="ft3", info_class="alpenhorn.acquisition.NoClass"),
    ]

    for ft in fts:
        with pytest.raises(ImportError):
            ft.info()


@pytest.mark.alpenhorn_config({"model": {"file_info_errors": "skip"}})
def test_filetype_info_skip(set_config, filetype):
    """Test FileType.info() with info_errors skip"""

    ft = filetype(name="name", info_class="NoClass")

    info = ft.info()
    assert issubclass(info, _NoInfo)


@pytest.mark.alpenhorn_config({"model": {"file_info_errors": "ignore"}})
def test_filetype_info_ignore(set_config, filetype):
    """Test FileType.info() with info_errors skip"""

    ft = filetype(name="name", info_class="NoClass")

    info = ft.info()
    assert issubclass(info, _NoInfo)


def test_file_model(simpleacqtype, archiveacq, simplefiletype, archivefile):
    acq1 = archiveacq(name="acq1", type=simpleacqtype)
    before = datetime.datetime.now().replace(microsecond=0)
    archivefile(name="min", acq=acq1, type=simplefiletype)
    after = datetime.datetime.now()
    archivefile(
        name="max",
        acq=acq1,
        type=simplefiletype,
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
        "type": simpleacqtype.id,
        "md5sum": None,
        "size_b": None,
    }
    assert ArchiveFile.select().where(ArchiveFile.name == "max").dicts().get() == {
        "id": 2,
        "acq": acq1.id,
        "name": "max",
        "type": simpleacqtype.id,
        "md5sum": "123456789",
        "registered": after,
        "size_b": 45,
    }

    # (acq, name) is unique
    with pytest.raises(pw.IntegrityError):
        archivefile(name="min", acq=acq1, type=simplefiletype)
    # But this should work
    acq2 = archiveacq(name="acq2", type=simpleacqtype)
    archivefile(name="min", acq=acq2, type=simplefiletype)


def test_file_path(simpleacq, archivefile, simplefiletype):
    """Test ArchiveFile.path."""
    file = archivefile(name="file", acq=simpleacq, type=simplefiletype)

    assert file.path == pathlib.PurePath(simpleacq.name, "file")


def test_archive_count(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test ArchiveFile.archive_count()"""

    # Create a bunch of copies in various states on various node types
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n1", group=simplegroup, storage_type="A"),
        has_file="N",
    )
    assert simplefile.archive_count() == 0
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n2", group=simplegroup, storage_type="F"),
        has_file="Y",
    )
    assert simplefile.archive_count() == 0
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n3", group=simplegroup, storage_type="T"),
        has_file="Y",
    )
    assert simplefile.archive_count() == 0
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n4", group=simplegroup, storage_type="F"),
        has_file="Y",
    )
    assert simplefile.archive_count() == 0
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n5", group=simplegroup, storage_type="A"),
        has_file="Y",
    )
    assert simplefile.archive_count() == 1
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n6", group=simplegroup, storage_type="A"),
        has_file="X",
    )
    assert simplefile.archive_count() == 1
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n7", group=simplegroup, storage_type="A"),
        has_file="M",
    )
    assert simplefile.archive_count() == 1
    archivefilecopy(
        file=simplefile,
        node=storagenode(name="n8", group=simplegroup, storage_type="A"),
        has_file="Y",
    )
    assert simplefile.archive_count() == 2
