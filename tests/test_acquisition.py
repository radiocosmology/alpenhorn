"""
test_acquisition_model
------------------

Tests for `alpenhorn.acquisition` module.
"""

import datetime as dt
import peewee as pw
import pytest

from alpenhorn.acquisition import AcqType, ArchiveAcq, ArchiveFile, FileType



def test_schema(dbproxy, acq_data):
    assert set(dbproxy.get_tables()) == {
        "acqtype",
        "archiveacq",
        "filetype",
        "archivefile",
    }


def test_model(acq_data):

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


def test_registered(acq_data):
    """Verifies that registered times are unique per each ArchiveFile instance"""
    assert (
        ArchiveFile.select(pw.fn.Count(pw.fn.Distinct(ArchiveFile.registered))).scalar()
    ) > 1
