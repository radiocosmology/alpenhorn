"""Test the alpenhorn.archive module"""

import pytest
import pathlib
import datetime
import peewee as pw

from alpenhorn.archive import ArchiveFileCopy, ArchiveFileCopyRequest


def test_schema(dbproxy, simplecopy, simplerequest):
    assert set(dbproxy.get_tables()) == {
        "storagegroup",
        "storagenode",
        "archiveacq",
        "archivefile",
        "archivefilecopyrequest",
        "archivefilecopy",
    }


def test_archivefilecopy_model(
    simplegroup, storagenode, simpleacq, archivefile, archivefilecopy
):
    """Test ArchiveFileCopy table model."""
    node = storagenode(name="n1", group=simplegroup)
    minfile = archivefile(name="min", acq=simpleacq)
    maxfile = archivefile(name="max", acq=simpleacq)

    archivefilecopy(file=minfile, node=node)
    archivefilecopy(
        file=maxfile,
        node=node,
        has_file="X",
        wants_file="M",
        ready=True,
        size_b=300,
    )

    afc = ArchiveFileCopy.select().where(ArchiveFileCopy.file == minfile).dicts().get()

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

    afc = ArchiveFileCopy.select().where(ArchiveFileCopy.file == maxfile).dicts().get()

    del afc["last_update"]

    assert afc == {
        "file": maxfile.id,
        "node": node.id,
        "id": 2,
        "has_file": "X",
        "wants_file": "M",
        "ready": True,
        "size_b": 300,
    }

    # (node, file) is unique
    with pytest.raises(pw.IntegrityError):
        archivefilecopy(file=minfile, node=node)
    # But this should work
    node2 = storagenode(name="new", group=simplegroup)
    archivefilecopy(file=minfile, node=node2)


def test_archivefilecopyrequest_model(
    simplegroup, storagenode, simplefile, archivefilecopyrequest
):
    """Test ArchiveFileCopyRequest model"""
    minnode = storagenode(name="min", group=simplegroup)
    maxnode = storagenode(name="max", group=simplegroup)
    before = (datetime.datetime.now() - datetime.timedelta(seconds=1)).replace(
        microsecond=0
    )
    archivefilecopyrequest(file=simplefile, node_from=minnode, group_to=simplegroup)
    after = datetime.datetime.now() + datetime.timedelta(seconds=1)
    archivefilecopyrequest(
        file=simplefile,
        node_from=maxnode,
        group_to=simplegroup,
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
        "file": simplefile.id,
        "node_from": minnode.id,
        "group_to": simplegroup.id,
        "id": 1,
        "cancelled": False,
        "completed": False,
        "transfer_completed": None,
        "transfer_started": None,
    }
    assert ArchiveFileCopyRequest.select().where(
        ArchiveFileCopyRequest.node_from == maxnode
    ).dicts().get() == {
        "file": simplefile.id,
        "node_from": maxnode.id,
        "group_to": simplegroup.id,
        "id": 2,
        "cancelled": True,
        "completed": True,
        "timestamp": before,
        "transfer_completed": before,
        "transfer_started": after,
    }

    # Not unique
    archivefilecopyrequest(file=simplefile, node_from=minnode, group_to=simplegroup)


def test_copy_path(simplefile, simplenode, archivefilecopy):
    """Test ArchiveFileCopy.path."""

    copy = archivefilecopy(file=simplefile, node=simplenode)

    assert copy.path == pathlib.PurePath(
        simplenode.root, simplefile.acq.name, simplefile.name
    )
