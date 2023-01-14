"""Test auto_import.py"""

import pytest
import pathlib
import peewee as pw
from unittest.mock import patch

from alpenhorn import auto_import
from alpenhorn.archive import ArchiveFileCopy
from alpenhorn.acquisition import ArchiveAcq, ArchiveFile, AcqType


def test_import_file_bad_paths(queue, simplenode):
    """Test that bad paths are rejected by import_file."""

    # Path outside root
    auto_import.import_file(simplenode, queue, pathlib.Path("/bad/path"))

    # no job queued
    assert queue.qsize == 0

    # node root is ignored
    auto_import.import_file(simplenode, queue, pathlib.Path(simplenode.root))

    # no job queued
    assert queue.qsize == 0


def test_import_file_queue(queue, simplenode):
    """Test import_file()"""

    auto_import.import_file(simplenode, queue, pathlib.Path("/node/acq/file"))

    # job in queue
    assert queue.qsize == 1


def test_import_file_bad_acq(dbtables, simplefiletype, simplenode):
    """Test bad acq in _import_file()"""

    auto_import._import_file(simplenode, pathlib.Path("acq/file"))

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


@patch("alpenhorn.acquisition.FileType.detect")
def test_import_file_copy_exists(mock_detect, simplenode, simplefile, archivefilecopy):
    """Test copy exists in _import_file()"""
    from alpenhorn.acquisition import FileType

    # Create the file copy
    archivefilecopy(file=simplefile, node=simplenode, has_file="Y")

    auto_import._import_file(simplenode, pathlib.Path("simplefile_acq/simplefile"))

    # The copy exists check happens before filetype detection.
    mock_detect.assert_not_called()


def test_import_file_bad_file(dbtables, simplefiletype, simplenode):
    """Test bad file in _import_file()"""

    auto_import._import_file(simplenode, pathlib.Path("simplefile_acq/file"))

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


def test_import_file_locked(xfs, dbtables, simplefiletype, simplenode):
    """Test bad file in _import_file()"""

    # Create lockfile
    xfs.create_file("/node/simplefile_acq/.simplefile.lock")

    auto_import._import_file(simplenode, pathlib.Path("simplefile_acq/simplefile"))

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


def test_import_file_create(xfs, dbtables, simplefiletype, simplenode):
    """Test acq, file, copy creation in _import_file()"""

    xfs.create_file("/node/simplefile_acq/simplefile")
    auto_import._import_file(simplenode, pathlib.Path("simplefile_acq/simplefile"))

    # Check DB
    acq = ArchiveAcq.get(name="simplefile_acq")
    assert acq.__data__ == {
        "id": 1,
        "name": "simplefile_acq",
        "comment": None,
        "type": 1,
    }

    file = ArchiveFile.get(name="simplefile", acq=acq)
    file_data = file.__data__
    del file_data["registered"]
    assert file_data == {
        "id": 1,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "name": "simplefile",
        "acq": 1,
        "size_b": 0,
        "type": 1,
    }

    copy = ArchiveFileCopy.get(file=file, node=simplenode)
    copy_data = copy.__data__
    del copy_data["last_update"]
    assert copy_data == {
        "id": 1,
        "node": 1,
        "file": 1,
        "has_file": "Y",
        "wants_file": "Y",
        "ready": True,
        "size_b": 0,
    }


def test_import_file_exists(xfs, dbtables, simplenode, simplefile, archivefilecopy):
    """Test _import_file() with pre-existing acq, file, copy"""

    # Create the file copy
    archivefilecopy(
        file=simplefile, node=simplenode, has_file="N", wants_file="N", ready=False
    )
    xfs.create_file("/node/simplefile_acq/simplefile")

    auto_import._import_file(simplenode, pathlib.Path("simplefile_acq/simplefile"))

    # Check DB
    acq = ArchiveAcq.get(name="simplefile_acq")
    assert acq == simplefile.acq

    file = ArchiveFile.get(name="simplefile", acq=acq)
    assert file == simplefile

    copy = ArchiveFileCopy.get(file=file, node=simplenode)
    copy_data = copy.__data__
    del copy_data["last_update"]
    assert copy_data == {
        "id": 1,
        "node": 1,
        "file": 1,
        "has_file": "M",
        "wants_file": "Y",
        "ready": True,
        "size_b": None,
    }
