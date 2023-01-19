"""Test auto_import.py"""

import pytest
import pathlib
import peewee as pw
from unittest.mock import call, patch
from watchdog.observers.api import BaseObserver, ObservedWatch

from alpenhorn import auto_import
from alpenhorn.archive import ArchiveFileCopy
from alpenhorn.acquisition import ArchiveAcq, ArchiveFile, AcqType, FileType
from alpenhorn.info_base import GenericAcqInfo, GenericFileInfo


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


@pytest.mark.lfs_hsm_state(
    {
        "/node/acq/file": "released",
    }
)
def test_import_file_not_ready(dbtables, simplenode, mock_lfs):
    """Test _import_file() on unready file."""

    # Set up node for NearlineIO
    simplenode.io_class = "Nearline"
    simplenode.io_config = '{"quota_group": "qgroup", "fixed_quota": 300000}'

    # _import_file is a generator function, so it needs to be interated to run.
    assert (
        next(auto_import._import_file(None, simplenode, pathlib.Path("acq/file"))) == 60
    )

    # File has been restored
    assert not mock_lfs("").hsm_released("/node/acq/file")


def test_import_file_bad_acq(dbtables, simplefiletype, simplenode):
    """Test bad acq in _import_file()"""

    # _import_file is a generator function, so it needs to be interated to run.
    with pytest.raises(StopIteration):
        next(auto_import._import_file(None, simplenode, pathlib.Path("acq/file")))

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


@patch("alpenhorn.acquisition.FileType.detect")
def test_import_file_copy_exists(mock_detect, simplenode, simplefile, archivefilecopy):
    """Test copy exists in _import_file()"""
    # Create the file copy
    archivefilecopy(file=simplefile, node=simplenode, has_file="Y")

    with pytest.raises(StopIteration):
        next(
            auto_import._import_file(
                None, simplenode, pathlib.Path("simplefile_acq/simplefile")
            )
        )

    # The copy exists check happens before filetype detection.
    mock_detect.assert_not_called()


def test_import_file_bad_file(dbtables, simplefiletype, simplenode):
    """Test bad file in _import_file()"""

    with pytest.raises(StopIteration):
        next(
            auto_import._import_file(
                None, simplenode, pathlib.Path("simplefile_acq/file")
            )
        )

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


def test_import_file_locked(xfs, dbtables, simplefiletype, simplenode):
    """Test bad file in _import_file()"""

    # Create lockfile
    xfs.create_file("/node/simplefile_acq/.simplefile.lock")

    with pytest.raises(StopIteration):
        next(
            auto_import._import_file(
                None, simplenode, pathlib.Path("simplefile_acq/simplefile")
            )
        )

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


def test_import_file_create(xfs, dbproxy, dbtables, simplefiletype, simplenode):
    """Test acq, file, copy creation in _import_file()"""

    xfs.create_file("/node/simplefile_acq/simplefile")

    # Shoe-horn in some info classes
    class AcqInfoTest(GenericAcqInfo):
        datum = pw.CharField()

        def _set_info(self, path, node, acqtype, acqname):
            return {"datum": "acq_datum"}

    class FileInfoTest(GenericFileInfo):
        datum = pw.CharField()

        def _set_info(self, path, node, acqtype, acqname):
            return {"datum": "file_datum"}

    acqtype = AcqType.get(id=1)
    AcqType._info_classes[acqtype.name] = AcqInfoTest
    FileType._info_classes[simplefiletype.name] = FileInfoTest
    dbproxy.create_tables([AcqInfoTest, FileInfoTest])

    AcqInfoTest.set_config(acqtype)
    FileInfoTest.set_config(simplefiletype)

    with pytest.raises(StopIteration):
        next(
            auto_import._import_file(
                None, simplenode, pathlib.Path("simplefile_acq/simplefile")
            )
        )

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

    ai = AcqInfoTest.get(id=1)
    assert ai.__data__ == {"id": 1, "acq": acq.id, "datum": "acq_datum"}

    fi = FileInfoTest.get(id=1)
    assert fi.__data__ == {"id": 1, "file": file.id, "datum": "file_datum"}


def test_import_file_exists(xfs, dbtables, simplenode, simplefile, archivefilecopy):
    """Test _import_file() with pre-existing acq, file, copy"""

    # Create the file copy
    archivefilecopy(
        file=simplefile, node=simplenode, has_file="N", wants_file="N", ready=False
    )
    xfs.create_file("/node/simplefile_acq/simplefile")

    with pytest.raises(StopIteration):
        next(
            auto_import._import_file(
                None, simplenode, pathlib.Path("simplefile_acq/simplefile")
            )
        )

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


def test_empty_stop_observers():
    """Test calling stop_observers with nothing running."""
    assert len(auto_import._observers) == 0
    assert len(auto_import._watchers) == 0
    auto_import.stop_observers()
    assert len(auto_import._observers) == 0
    assert len(auto_import._watchers) == 0


def test_update_observers_start(xfs, dbtables, simplenode, queue):
    """Test starting an observer via update_observers()."""

    xfs.create_file("/node/acq/file", contents="")
    simplenode.auto_import = True
    auto_import.update_observer(simplenode, queue)

    assert isinstance(auto_import._observers["Default"], BaseObserver)
    assert len(auto_import._observers["Default"].emitters) == 1
    assert isinstance(auto_import._watchers["simplenode"], ObservedWatch)
    assert auto_import._watchers["simplenode"].path == simplenode.root

    auto_import.stop_observers()
    assert len(auto_import._observers) == 0
    assert len(auto_import._watchers) == 0


def test_update_observers_stop(xfs, dbtables, simplenode, queue):
    """Test stopping a watcher via update_observers()."""

    # First start
    xfs.create_dir(simplenode.root)
    simplenode.auto_import = True
    auto_import.update_observer(simplenode, queue)
    assert len(auto_import._observers["Default"].emitters) == 1
    assert "simplenode" in auto_import._watchers

    # Now stop
    simplenode.auto_import = False
    auto_import.update_observer(simplenode, queue)
    assert len(auto_import._observers["Default"].emitters) == 0
    assert "simplenode" not in auto_import._watchers

    auto_import.stop_observers()


@patch("alpenhorn.auto_import.import_file")
def test_catchup_new(mocked_import, xfs, dbtables, simplenode, queue):
    """Test auto_import.catchup with new files."""

    # Make some files to "import"
    xfs.create_file("/node/acq1/file1")
    xfs.create_file("/node/acq1/file2")
    xfs.create_file("/node/acq2/file1")

    auto_import.catchup(simplenode, queue)
    mocked_import.assert_has_calls(
        [
            call(simplenode, queue, pathlib.PurePath("acq1/file1")),
            call(simplenode, queue, pathlib.PurePath("acq1/file2")),
            call(simplenode, queue, pathlib.PurePath("acq2/file1")),
        ],
    )


@patch("alpenhorn.auto_import.import_file")
def test_catchup_exists(
    mocked_import,
    xfs,
    queue,
    simplenode,
    simpleacq,
    simplefiletype,
    archivefile,
    archivefilecopy,
):
    """Test auto_import.catchup skips existing copies."""

    # Make files in DB
    af1 = archivefile(name="file1", acq=simpleacq, type=simplefiletype)
    af2 = archivefile(name="file2", acq=simpleacq, type=simplefiletype)
    af3 = archivefile(name="file3", acq=simpleacq, type=simplefiletype)
    af4 = archivefile(name="file4", acq=simpleacq, type=simplefiletype)
    af5 = archivefile(name="file5", acq=simpleacq, type=simplefiletype)

    # Make copies in DB
    archivefilecopy(node=simplenode, file=af1, has_file="Y")
    archivefilecopy(node=simplenode, file=af2, has_file="M")
    archivefilecopy(node=simplenode, file=af3, has_file="X")
    archivefilecopy(node=simplenode, file=af4, has_file="N")

    # Create files to crawl
    xfs.create_file("/node/simpleacq/file1")  # has_file == 'Y'
    xfs.create_file("/node/simpleacq/file2")  # has_file == 'M'
    xfs.create_file("/node/simpleacq/file3")  # has_file == 'X'
    xfs.create_file("/node/simpleacq/file4")  # has_file == 'N'
    xfs.create_file("/node/simpleacq/file5")  # no copy but ArchiveFile
    xfs.create_file("/node/simpleacq/file6")  # no copy or ArchiveFile

    auto_import.catchup(simplenode, queue)

    # Only file1 should be skipped
    assert mocked_import.mock_calls == [
        call(simplenode, queue, pathlib.PurePath("simpleacq/file2")),
        call(simplenode, queue, pathlib.PurePath("simpleacq/file3")),
        call(simplenode, queue, pathlib.PurePath("simpleacq/file4")),
        call(simplenode, queue, pathlib.PurePath("simpleacq/file5")),
        call(simplenode, queue, pathlib.PurePath("simpleacq/file6")),
    ]
