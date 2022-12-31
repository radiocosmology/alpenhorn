"""Test auto_import.py"""

import pytest
import pathlib
import peewee as pw

from alpenhorn import auto_import
from alpenhorn.acquisition import ArchiveAcq


def test_import_file_bad_paths(queue, simplenode):
    """Test that a bad paths are rejected by import_file."""

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


def test_import_file_bad_acq(dbtables, simpleacqinfo, simpleacqtype, simplenode):
    """Test bad acq in _import_file()"""

    simpleacqinfo.patterns = ["not-a-match"]
    auto_import._import_file(simplenode, pathlib.Path("acq/file"))

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")


def test_import_file_bad_file(
    dbtables, simpleacqinfo, simplefileinfo, simpleacqtype, simplenode
):
    """Test bad acq in _import_file()"""

    auto_import._import_file(simplenode, pathlib.Path("acq/file"))

    # No acq has been added
    with pytest.raises(pw.DoesNotExist):
        ArchiveAcq.get(name="acq")
