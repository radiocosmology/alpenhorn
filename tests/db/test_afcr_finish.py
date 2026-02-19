"""Test ArchiveFileCopyRequest.finish()."""

import datetime
import time
from unittest.mock import MagicMock, patch

import pytest

from alpenhorn.daemon.update import UpdateableNode
from alpenhorn.db import utcfromtimestamp, utcnow
from alpenhorn.db.archive import ArchiveFileCopy, ArchiveFileCopyRequest


@pytest.fixture
def mock_autoactions():
    """Mocks ArchiveFileCopy.trigger_autoactions."""

    mock = MagicMock()
    with patch("alpenhorn.db.archive.ArchiveFileCopy.trigger_autoactions", mock):
        yield mock


@pytest.fixture
def db_setup(
    queue,
    mock_filesize,
    mock_autoactions,
    storagegroup,
    storagenode,
    simplefile,
    archivefilecopy,
    archivefilecopyrequest,
):
    """DB setup for checks with no pre-existing dest copy."""

    group_to = storagegroup(name="group_to")
    node_to = storagenode(name="node_to", group=group_to, root="/node_to")
    node_from = storagenode(
        name="node_from", group=storagegroup(name="group_from"), root="/node_from"
    )

    return (
        UpdateableNode(queue, node_to).io,
        archivefilecopy(file=simplefile, node=node_from, has_file="Y"),
        archivefilecopyrequest(file=simplefile, node_from=node_from, group_to=group_to),
        time.time() - 2,  # start_time
        mock_autoactions,
    )


@pytest.fixture
def db_setup_with_copy(db_setup, archivefilecopy):
    """DB setup for checks with pre-existing dest copy."""
    node_to = db_setup[0].node
    req = db_setup[1]

    dstcopy = archivefilecopy(file=req.file, node=node_to, has_file="N", ready=False)

    # We re-get to launder the record through the DB
    return *db_setup, ArchiveFileCopy.get(id=dstcopy.id)


def test_fail_chksrc(db_setup):
    """Test failed transfer with check_src==True"""

    io, copy, req, start_time, trigger_autoactions = db_setup

    assert (
        req.finish(
            io.node,
            io.storage_used,
            success=False,
            md5ok=False,
            start_time=start_time,
            check_src=True,
        )
        is False
    )

    # request is not resolved
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert not afcr.completed
    assert not afcr.cancelled

    # reqeust to re-check src has been made
    assert ArchiveFileCopy.get(id=copy.id).has_file == "M"

    trigger_autoactions.assert_not_called()


def test_fail_nochksrc(db_setup):
    """Test failed transfer with check_src==False"""

    io, copy, req, start_time, trigger_autoactions = db_setup

    assert (
        req.finish(
            io.node,
            io.storage_used,
            success=False,
            md5ok=False,
            start_time=start_time,
            check_src=False,
        )
        is False
    )

    # request is not resolved
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert not afcr.completed
    assert not afcr.cancelled

    # reqeust to re-check src has not been made
    assert ArchiveFileCopy.get(id=copy.id).has_file != "M"

    trigger_autoactions.assert_not_called()


def test_md5ok_false(db_setup):
    """Test successful transfer with md5ok==False"""

    io, copy, req, start_time, trigger_autoactions = db_setup

    assert (
        req.finish(
            io.node,
            io.storage_used,
            success=True,
            md5ok=False,
            start_time=start_time,
        )
        is False
    )

    # request is not resolved
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert not afcr.completed
    assert not afcr.cancelled

    # reqeust to re-check src has been made
    assert ArchiveFileCopy.get(id=copy.id).has_file == "M"

    trigger_autoactions.assert_not_called()


def test_md5ok_bad(db_setup):
    """Test successful transfer with a non-matching md5ok string"""

    io, copy, req, start_time, trigger_autoactions = db_setup

    assert (
        req.finish(
            io.node,
            io.storage_used,
            success=True,
            md5ok="incorrect-md5sum",
            start_time=start_time,
        )
        is False
    )

    # request is not resolved
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert not afcr.completed
    assert not afcr.cancelled

    # reqeust to re-check src has been made
    assert ArchiveFileCopy.get(id=copy.id).has_file == "M"

    trigger_autoactions.assert_not_called()


def test_md5ok_true(db_setup):
    """Test successful transfer with a md5ok==True"""

    io, copy, req, start_time, trigger_autoactions = db_setup

    before = utcnow() - datetime.timedelta(seconds=2)
    assert (
        req.finish(
            io.node,
            io.storage_used,
            success=True,
            md5ok=True,
            start_time=start_time,
        )
        is True
    )
    after = utcnow()

    # request is resolved
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed
    assert not afcr.cancelled
    assert afcr.transfer_started == utcfromtimestamp(start_time)
    assert afcr.transfer_completed >= before
    assert afcr.transfer_completed <= after

    # reqeust to re-check src has not been made
    assert ArchiveFileCopy.get(id=copy.id).has_file != "M"

    # Check copy on dest
    dstcopy = ArchiveFileCopy.get(
        ArchiveFileCopy.node == io.node, ArchiveFileCopy.file == req.file
    )
    assert dstcopy.has_file == "Y"
    assert dstcopy.wants_file == "Y"
    assert dstcopy.ready is True
    assert dstcopy.size_b == 512 * 3
    assert dstcopy.last_update >= before
    assert dstcopy.last_update <= after

    trigger_autoactions.assert_called_once()


def test_md5ok_str(db_setup):
    """Test successful transfer with a matching md5ok string"""

    io, copy, req, start_time, trigger_autoactions = db_setup

    assert (
        req.finish(
            io.node,
            io.storage_used,
            success=True,
            md5ok="d41d8cd98f00b204e9800998ecf8427e",
            start_time=start_time,
        )
        is True
    )

    # request is resolved
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed
    assert not afcr.cancelled

    # reqeust to re-check src has been made
    assert ArchiveFileCopy.get(id=copy.id).has_file != "M"

    # dest copy exists
    assert (
        ArchiveFileCopy.get(
            ArchiveFileCopy.node == io.node, ArchiveFileCopy.file == req.file
        ).has_file
        == "Y"
    )

    trigger_autoactions.assert_called_once()


def test_dstcopy(db_setup_with_copy):
    """Test successful transfer with existing destination copy record."""

    io, _, req, start_time, trigger_autoactions, dstcopy = db_setup_with_copy

    assert (
        req.finish(
            io.node,
            io.storage_used,
            success=True,
            md5ok=True,
            start_time=start_time,
        )
        is True
    )

    # Verify update-in-place of ArchiveFileCopy
    newcopy = ArchiveFileCopy.get(
        ArchiveFileCopy.node == io.node, ArchiveFileCopy.file == req.file
    )
    assert newcopy.id == dstcopy.id
    assert newcopy.has_file == "Y"
    assert newcopy.wants_file == "Y"
    assert newcopy.ready is True
    assert newcopy.size_b == 512 * 3

    trigger_autoactions.assert_called_once()
