"""Test auto_import.py"""

import pytest
import pathlib
import datetime
import peewee as pw
from unittest.mock import call, patch
from watchdog.observers.api import BaseObserver, ObservedWatch

from alpenhorn import auto_import
from alpenhorn.archive import ArchiveFileCopy
from alpenhorn.acquisition import ArchiveAcq, ArchiveFile
from alpenhorn.update import UpdateableNode


def test_import_file_bad_paths(queue, unode):
    """Test that bad paths are rejected by import_file."""

    # Path outside root
    auto_import.import_file(unode, queue, pathlib.PurePath("/bad/path"))

    # no job queued
    assert queue.qsize == 0

    # node root is ignored
    auto_import.import_file(unode, queue, pathlib.PurePath(unode.db.root))

    # no job queued
    assert queue.qsize == 0


def test_import_file_queue(queue, unode):
    """Test job queueing in import_file()"""

    auto_import.import_file(unode, queue, pathlib.PurePath("/node/acq/file"))

    # job in queue
    assert queue.qsize == 1


@pytest.mark.lfs_hsm_state(
    {
        "/node/acq/file": "released",
    }
)
def test_import_file_not_ready(dbtables, queue, simplenode, mock_lfs):
    """Test _import_file() on unready file."""

    # Set up node for LustreHSMIO
    simplenode.io_class = "LustreHSM"
    simplenode.io_config = '{"quota_group": "qgroup", "headroom": 300000}'
    unode = UpdateableNode(queue, simplenode)

    # _import_file is a generator function, so it needs to be interated to run.
    assert next(auto_import._import_file(None, unode, pathlib.PurePath("acq/file"))) > 0

    # File has been restored
    assert not mock_lfs("").hsm_released("/node/acq/file")


def test_import_file_no_ext(dbtables, unode):
    """Test no import_detect extensions during _import_file()"""

    # _import_file is a generator function, so it needs to be interated to run.
    with pytest.raises(StopIteration):
        next(auto_import._import_file(None, unode, pathlib.PurePath("acq/file")))

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


def test_import_file_no_detect(dbtables, unode):
    """Test no detection from import_detect."""

    with patch("alpenhorn.extensions._id_ext", [lambda path, node: (None, None)]):
        with pytest.raises(StopIteration):
            next(auto_import._import_file(None, unode, pathlib.PurePath("acq/file")))

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


def test_import_file_locked(xfs, dbtables, unode):
    """Test bad file in _import_file()"""

    # Create lockfile
    xfs.create_file("/node/simplefile_acq/.simplefile.lock")

    with pytest.raises(StopIteration):
        next(
            auto_import._import_file(
                None, unode, pathlib.PurePath("simplefile_acq/simplefile")
            )
        )

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


def test_import_file_create(xfs, dbtables, unode):
    """Test acq, file, copy creation in _import_file()"""

    xfs.create_file("/node/simplefile_acq/simplefile")

    before = (datetime.datetime.now() - datetime.timedelta(seconds=1)).replace(
        microsecond=0
    )

    with patch(
        "alpenhorn.extensions._id_ext", [lambda path, node: ("simplefile_acq", None)]
    ):
        with pytest.raises(StopIteration):
            next(
                auto_import._import_file(
                    None, unode, pathlib.PurePath("simplefile_acq/simplefile")
                )
            )

    after = datetime.datetime.now() + datetime.timedelta(seconds=1)

    # Check DB
    acq = ArchiveAcq.get(name="simplefile_acq")
    assert acq.__data__ == {
        "id": 1,
        "name": "simplefile_acq",
        "comment": None,
    }

    file = ArchiveFile.get(name="simplefile", acq=acq)
    file_data = file.__data__

    assert file_data["registered"] >= before
    assert file_data["registered"] <= after
    del file_data["registered"]
    assert file_data == {
        "id": 1,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "name": "simplefile",
        "acq": 1,
        "size_b": 0,
    }

    copy = ArchiveFileCopy.get(file=file, node=unode.db)
    copy_data = copy.__data__

    assert copy_data["last_update"] >= before
    assert copy_data["last_update"] <= after
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


def test_import_file_callback(xfs, dbtables, unode):
    """Test _import_file() with callback"""

    xfs.create_file("/node/simplefile_acq/simplefile")

    callback_executed = False
    callback_args = None

    def callback(copy, file_, acq, node):
        nonlocal callback_executed, callback_args
        callback_executed = True
        callback_args = [copy, file_, acq, node]

    with patch(
        "alpenhorn.extensions._id_ext",
        [lambda path, node: ("simplefile_acq", callback)],
    ):
        with pytest.raises(StopIteration):
            next(
                auto_import._import_file(
                    None, unode, pathlib.PurePath("simplefile_acq/simplefile")
                )
            )

    # Get records from DB
    acq = ArchiveAcq.get(name="simplefile_acq")
    file = ArchiveFile.get(name="simplefile", acq=acq)
    copy = ArchiveFileCopy.get(file=file, node=unode.db)

    # Did callback run?
    assert callback_executed
    assert callback_args == [copy, file, acq, unode]


def test_import_file_exists(xfs, dbtables, unode, simplefile, archivefilecopy):
    """Test _import_file() with pre-existing acq, file, copy"""

    # Create the file copy
    archivefilecopy(
        file=simplefile,
        node=unode.db,
        has_file="N",
        wants_file="N",
        ready=False,
        last_update=datetime.datetime(2000, 1, 1, 0, 0, 0),
    )
    xfs.create_file("/node/simplefile_acq/simplefile")

    before = (datetime.datetime.now() - datetime.timedelta(seconds=1)).replace(
        microsecond=0
    )

    with patch(
        "alpenhorn.extensions._id_ext", [lambda path, node: ("simplefile_acq", None)]
    ):
        with pytest.raises(StopIteration):
            next(
                auto_import._import_file(
                    None, unode, pathlib.PurePath("simplefile_acq/simplefile")
                )
            )

    after = datetime.datetime.now() + datetime.timedelta(seconds=1)

    # Check DB
    acq = ArchiveAcq.get(name="simplefile_acq")
    assert acq == simplefile.acq

    file = ArchiveFile.get(name="simplefile", acq=acq)
    assert file == simplefile

    copy = ArchiveFileCopy.get(file=file, node=unode.db)
    copy_data = copy.__data__

    assert copy_data["last_update"] >= before
    assert copy_data["last_update"] <= after
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


def test_update_observers_start(xfs, dbtables, unode, queue):
    """Test starting an observer via update_observers()."""

    xfs.create_file("/node/acq/file", contents="")
    unode.db.auto_import = True
    auto_import.update_observer(unode, queue)

    assert isinstance(auto_import._observers[unode.io_class], BaseObserver)
    assert len(auto_import._observers[unode.io_class].emitters) == 1
    assert isinstance(auto_import._watchers["simplenode"], ObservedWatch)
    assert auto_import._watchers["simplenode"].path == unode.db.root

    auto_import.stop_observers()
    assert len(auto_import._observers) == 0
    assert len(auto_import._watchers) == 0


def test_update_observers_stop(xfs, dbtables, unode, queue):
    """Test stopping a watcher via update_observers()."""

    # First start
    xfs.create_dir(unode.db.root)
    unode.db.auto_import = True
    auto_import.update_observer(unode, queue)
    assert len(auto_import._observers[unode.io_class].emitters) == 1
    assert "simplenode" in auto_import._watchers

    # Now stop
    unode.db.auto_import = False
    auto_import.update_observer(unode, queue)
    assert len(auto_import._observers[unode.io_class].emitters) == 0
    assert "simplenode" not in auto_import._watchers

    auto_import.stop_observers()


def test_update_observers_force_stop(xfs, dbtables, unode, queue):
    """Test force-stopping a watcher via update_observers()."""

    # First start
    xfs.create_dir(unode.db.root)
    unode.db.auto_import = True
    auto_import.update_observer(unode, queue)
    assert len(auto_import._observers[unode.io_class].emitters) == 1
    assert "simplenode" in auto_import._watchers

    # Now force stop
    auto_import.update_observer(unode, queue, force_stop=True)
    assert len(auto_import._observers[unode.io_class].emitters) == 0
    assert "simplenode" not in auto_import._watchers

    auto_import.stop_observers()


@patch("alpenhorn.auto_import.import_file")
def test_catchup_new(mocked_import, xfs, dbtables, unode, queue):
    """Test auto_import.catchup with new files."""

    # Make some files to "import"
    xfs.create_file("/node/acq1/file1")
    xfs.create_file("/node/acq1/file2")
    xfs.create_file("/node/acq2/file1")

    auto_import.catchup(None, unode, queue)
    mocked_import.assert_has_calls(
        [
            call(unode, queue, pathlib.PurePath("acq1/file1")),
            call(unode, queue, pathlib.PurePath("acq1/file2")),
            call(unode, queue, pathlib.PurePath("acq2/file1")),
        ],
    )


@patch("alpenhorn.auto_import.import_file")
def test_catchup_exists(
    mocked_import,
    xfs,
    queue,
    unode,
    simpleacq,
    archivefile,
    archivefilecopy,
):
    """Test auto_import.catchup skips existing copies."""

    # Make files in DB
    af1 = archivefile(name="file1", acq=simpleacq)
    af2 = archivefile(name="file2", acq=simpleacq)
    af3 = archivefile(name="file3", acq=simpleacq)
    af4 = archivefile(name="file4", acq=simpleacq)
    archivefile(name="file5", acq=simpleacq)

    # Make copies in DB
    archivefilecopy(node=unode.db, file=af1, has_file="Y")
    archivefilecopy(node=unode.db, file=af2, has_file="M")
    archivefilecopy(node=unode.db, file=af3, has_file="X")
    archivefilecopy(node=unode.db, file=af4, has_file="N")

    # Create files to crawl
    xfs.create_file("/node/simpleacq/file1")  # has_file == 'Y'
    xfs.create_file("/node/simpleacq/file2")  # has_file == 'M'
    xfs.create_file("/node/simpleacq/file3")  # has_file == 'X'
    xfs.create_file("/node/simpleacq/file4")  # has_file == 'N'
    xfs.create_file("/node/simpleacq/file5")  # no copy but ArchiveFile
    xfs.create_file("/node/simpleacq/file6")  # no copy or ArchiveFile

    auto_import.catchup(None, unode, queue)

    # Only files4 through 6 should be imported
    assert mocked_import.mock_calls == [
        call(unode, queue, pathlib.PurePath("simpleacq/file4")),
        call(unode, queue, pathlib.PurePath("simpleacq/file5")),
        call(unode, queue, pathlib.PurePath("simpleacq/file6")),
    ]
