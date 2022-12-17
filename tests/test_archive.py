"""
test_archive_model
------------------

Tests for `alpenhorn.archive` module.
"""

import pytest
import pathlib
import datetime
import peewee as pw

from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.archive import ArchiveFile, ArchiveFileCopy, ArchiveFileCopyRequest


def test_schema(dbproxy, genericcopy, genericrequest):
    assert set(dbproxy.get_tables()) == {
        "storagegroup",
        "storagenode",
        "acqtype",
        "archiveacq",
        "filetype",
        "archivefile",
        "archivefilecopyrequest",
        "archivefilecopy",
    }


def test_archivefilecopy_model(
    genericgroup, storagenode, genericacq, filetype, archivefile, archivefilecopy
):
    """Test ArchiveFileCopy table model."""
    node = storagenode(name="n1", group=genericgroup)
    ft = filetype(name="name")
    minfile = archivefile(name="min", acq=genericacq, type=ft)
    maxfile = archivefile(name="max", acq=genericacq, type=ft)

    # Deal with round-off
    before = (datetime.datetime.now() - datetime.timedelta(seconds=1)).replace(
        microsecond=0
    )
    archivefilecopy(file=minfile, node=node)
    archivefilecopy(
        file=maxfile,
        node=node,
        has_file="X",
        wants_file="M",
        ready=True,
        size_b=300,
        last_update=before,
    )
    after = datetime.datetime.now() + datetime.timedelta(seconds=1)

    afc = ArchiveFileCopy.select().where(ArchiveFileCopy.file == minfile).dicts().get()

    assert afc["last_update"] >= before
    assert afc["last_update"] <= after
    del afc["last_update"]

    assert afc == {
        "file": minfile.id,
        "node": node.id,
        "id": 1,
        "has_file": "N",
        "wants_file": "Y",
        "ready": False,
        "size_b": None,
    }
    assert ArchiveFileCopy.select().where(
        ArchiveFileCopy.file == maxfile
    ).dicts().get() == {
        "file": maxfile.id,
        "node": node.id,
        "id": 2,
        "has_file": "X",
        "wants_file": "M",
        "ready": True,
        "size_b": 300,
        "last_update": before,
    }

    # (node, file) is unique
    with pytest.raises(pw.IntegrityError):
        archivefilecopy(file=minfile, node=node)
    # But this should work
    node2 = storagenode(name="new", group=genericgroup)
    archivefilecopy(file=minfile, node=node2)


def test_archivefilecopyrequest_model(
    genericgroup, storagenode, genericfile, archivefilecopyrequest
):
    """Test ArchiveFileCopyRequest model"""
    minnode = storagenode(name="min", group=genericgroup)
    maxnode = storagenode(name="max", group=genericgroup)
    before = (datetime.datetime.now() - datetime.timedelta(seconds=1)).replace(
        microsecond=0
    )
    archivefilecopyrequest(file=genericfile, node_from=minnode, group_to=genericgroup)
    after = datetime.datetime.now() + datetime.timedelta(seconds=1)
    archivefilecopyrequest(
        file=genericfile,
        node_from=maxnode,
        group_to=genericgroup,
        cancelled=True,
        completed=True,
        timestamp=before,
        transfer_completed=before,
        transfer_started=after,
    )

    afcr = (
        ArchiveFileCopyRequest.select()
        .where(ArchiveFileCopyRequest.node_from == minnode)
        .dicts()
        .get()
    )
    assert afcr["timestamp"] >= before
    assert afcr["timestamp"] <= after
    del afcr["timestamp"]
    assert afcr == {
        "file": genericfile.id,
        "node_from": minnode.id,
        "group_to": genericgroup.id,
        "id": 1,
        "cancelled": False,
        "completed": False,
        "transfer_completed": None,
        "transfer_started": None,
    }
    assert ArchiveFileCopyRequest.select().where(
        ArchiveFileCopyRequest.node_from == maxnode
    ).dicts().get() == {
        "file": genericfile.id,
        "node_from": maxnode.id,
        "group_to": genericgroup.id,
        "id": 2,
        "cancelled": True,
        "completed": True,
        "timestamp": before,
        "transfer_completed": before,
        "transfer_started": after,
    }

    # Not unique
    archivefilecopyrequest(file=genericfile, node_from=minnode, group_to=genericgroup)


def test_copy_path(genericfile, genericnode, archivefilecopy):
    """Test ArchiveFileCopy.path."""

    copy = archivefilecopy(file=genericfile, node=genericnode)

    assert copy.path == pathlib.PurePath(
        genericnode.root, genericfile.acq.name, genericfile.name
    )
