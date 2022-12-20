"""Test alpenhorn.io.ioutil.copy_request_done()."""

import time
import pytest
import datetime


from alpenhorn.archive import ArchiveFileCopy, ArchiveFileCopyRequest
from alpenhorn.io.ioutil import copy_request_done


@pytest.fixture
def db_setup(
    mock_filesize,
    storagegroup,
    storagenode,
    simplefile,
    archivefilecopy,
    archivefilecopyrequest,
):
    """DB setup for copy_request_done checks with no pre-existing dest copy."""

    group_to = storagegroup(name="group_to")
    node_to = storagenode(name="node_to", group=group_to, root="/node_to")
    node_from = storagenode(
        name="node_from", group=storagegroup(name="group_from"), root="/node_from"
    )

    return (
        node_to,
        archivefilecopy(file=simplefile, node=node_from, has_file="Y"),
        archivefilecopyrequest(
            file=simplefile, node_from=node_from, group_to=group_to
        ),
        time.time() - 2,  # start_time
    )


@pytest.fixture
def db_setup_with_copy(db_setup, archivefilecopy):
    """DB setup for copy_request_done checks with pre-existing dest copy."""
    node_to = db_setup[0]
    req = db_setup[1]

    dstcopy = archivefilecopy(file=req.file, node=node_to, has_file="N", ready=False)

    # We re-get to launder the record through the DB
    return *db_setup, ArchiveFileCopy.get(id=dstcopy.id)


def test_fail_chksrc(db_setup):
    """Test failed transfer with check_src==True"""

    node, copy, req, start_time = db_setup

    assert (
        copy_request_done(
            req,
            node,
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


def test_fail_nochksrc(db_setup):
    """Test failed transfer with check_src==False"""

    node, copy, req, start_time = db_setup

    assert (
        copy_request_done(
            req,
            node,
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


def test_md5ok_false(db_setup):
    """Test successful transfer with md5ok==False"""

    node, copy, req, start_time = db_setup

    assert (
        copy_request_done(
            req,
            node,
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


def test_md5ok_bad(db_setup):
    """Test successful transfer with a non-matching md5ok string"""

    node, copy, req, start_time = db_setup

    assert (
        copy_request_done(
            req,
            node,
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


def test_md5ok_true(db_setup):
    """Test successful transfer with a md5ok==True"""

    node, copy, req, start_time = db_setup

    before = datetime.datetime.now() - datetime.timedelta(seconds=2)
    assert (
        copy_request_done(
            req,
            node,
            success=True,
            md5ok=True,
            start_time=start_time,
        )
        is True
    )
    after = datetime.datetime.now()

    # request is resolved
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed
    assert not afcr.cancelled
    assert afcr.transfer_started == datetime.datetime.fromtimestamp(start_time)
    assert afcr.transfer_completed >= before
    assert afcr.transfer_completed <= after

    # reqeust to re-check src has not been made
    assert ArchiveFileCopy.get(id=copy.id).has_file != "M"

    # Check copy on dest
    dstcopy = ArchiveFileCopy.get(
        ArchiveFileCopy.node == node, ArchiveFileCopy.file == req.file
    )
    assert dstcopy.has_file == "Y"
    assert dstcopy.wants_file == "Y"
    assert dstcopy.ready is True
    assert dstcopy.size_b == 512 * 3
    assert dstcopy.last_update >= before
    assert dstcopy.last_update <= after


def test_md5ok_str(db_setup):
    """Test successful transfer with a matching md5ok string"""

    node, copy, req, start_time = db_setup

    before = datetime.datetime.now() - datetime.timedelta(seconds=2)
    assert (
        copy_request_done(
            req,
            node,
            success=True,
            md5ok="d41d8cd98f00b204e9800998ecf8427e",
            start_time=start_time,
        )
        is True
    )
    after = datetime.datetime.now()

    # request is resolved
    afcr = ArchiveFileCopyRequest.get(id=req.id)
    assert afcr.completed
    assert not afcr.cancelled

    # reqeust to re-check src has been made
    assert ArchiveFileCopy.get(id=copy.id).has_file != "M"

    # dest copy exists
    assert (
        ArchiveFileCopy.get(
            ArchiveFileCopy.node == node, ArchiveFileCopy.file == req.file
        ).has_file
        == "Y"
    )


def test_dstcopy(db_setup_with_copy):
    """Test successful transfer with existing destination copy record."""

    node, copy, req, start_time, dstcopy = db_setup_with_copy

    before = datetime.datetime.now() - datetime.timedelta(seconds=2)
    assert (
        copy_request_done(
            req,
            node,
            success=True,
            md5ok=True,
            start_time=start_time,
        )
        is True
    )
    after = datetime.datetime.now()

    # Verify update-in-place of ArchiveFileCopy
    newcopy = ArchiveFileCopy.get(
        ArchiveFileCopy.node == node, ArchiveFileCopy.file == req.file
    )
    assert newcopy.id == dstcopy.id
    assert newcopy.has_file == "Y"
    assert newcopy.wants_file == "Y"
    assert newcopy.ready is True
    assert newcopy.size_b == 512 * 3
