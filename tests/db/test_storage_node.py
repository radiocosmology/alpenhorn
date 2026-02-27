"""test alpenhorn.storage."""

import pathlib

import peewee as pw
import pytest

from alpenhorn.db.archive import ArchiveFileCopy
from alpenhorn.db.storage import StorageNode


def test_schema(dbproxy, simplegroup, storagehost, storagenode):
    # Force table creation
    storagehost(name="host")
    storagenode(name="node", group=simplegroup)

    assert set(dbproxy.get_tables()) == {
        "storagegroup",
        "storagehost",
        "storagenode",
    }


def test_node_model(storagegroup, storagehost, storagenode):
    group = storagegroup(name="group")
    host = storagehost(name="host")
    storagenode(name="min", group=group)
    storagenode(
        name="max",
        group=group,
        active=True,
        archive=True,
        auto_import=True,
        auto_verify=1,
        avail_gb=2.2,
        avail_gb_last_checked=3.3,
        host=host,
        io_class="IOClass",
        io_config="{ioconfig}",
        max_total_gb=4.4,
        min_avail_gb=5.5,
        notes="Notes",
        root="/root",
    )

    # name is unique
    with pytest.raises(pw.IntegrityError):
        storagenode(name="min", group=group)

    # Check records in DB
    assert StorageNode.select().where(StorageNode.name == "min").dicts().get() == {
        "id": 1,
        "name": "min",
        "group": group.id,
        "active": False,
        "archive": False,
        "auto_import": False,
        "auto_verify": 0,
        "avail_gb": None,
        "avail_gb_last_checked": None,
        "host": None,
        "io_class": None,
        "io_config": None,
        "max_total_gb": None,
        "min_avail_gb": 0,
        "notes": None,
        "root": None,
    }

    assert StorageNode.select().where(StorageNode.name == "max").dicts().get() == {
        "id": 2,
        "name": "max",
        "group": group.id,
        "active": True,
        "archive": True,
        "auto_import": True,
        "auto_verify": 1,
        "avail_gb": 2.2,
        "avail_gb_last_checked": 3.3,
        "host": host.id,
        "io_class": "IOClass",
        "io_config": "{ioconfig}",
        "max_total_gb": 4.4,
        "min_avail_gb": 5.5,
        "notes": "Notes",
        "root": "/root",
    }


def test_local(simplenode, daemon_host, storagehost):
    """Test StorageNode.local"""

    simplenode.host = daemon_host
    assert simplenode.local

    other_host = storagehost(name="other-host")
    simplenode.host = other_host
    assert not simplenode.local


def test_undermin(simplegroup, storagenode):
    """Test StorageNode.under_min."""

    node = storagenode(name="anone", group=simplegroup, min_avail_gb=2.0)
    assert node.under_min is False  # avail_gb is None

    node = storagenode(name="mzero", group=simplegroup, avail_gb=1.0, min_avail_gb=0.0)
    assert node.under_min is False  # min_avail_gb is zero

    node = storagenode(name="false", group=simplegroup, avail_gb=3.0, min_avail_gb=2.0)
    assert node.under_min is False

    node = storagenode(name="true", group=simplegroup, avail_gb=1.0, min_avail_gb=2.0)
    assert node.under_min is True


def test_totalgb(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test StorageNode.get_total_gb()."""

    # Node with a copy (simplefile has size_b==1GiB)
    node = storagenode(name="good", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.get_total_gb() == 1.0

    # Node with suspect copy
    node = storagenode(name="suspect", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="M")
    assert node.get_total_gb() == 1.0

    # Node with bad copy
    node = storagenode(name="bad", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="X")
    assert node.get_total_gb() == 0.0

    # Node with no file copies
    node = storagenode(name="empty", group=simplegroup)
    assert node.get_total_gb() == 0.0


def test_overmax(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test StorageNode.check_over_max()."""

    # simplefile has size_b==1GiB
    node = storagenode(name="toofull", group=simplegroup, max_total_gb=0.1)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.check_over_max() is True

    node = storagenode(name="notfull", group=simplegroup, max_total_gb=2.0)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.check_over_max() is False

    node = storagenode(name="zero", group=simplegroup, max_total_gb=0)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.check_over_max() is False

    # This is the old default
    node = storagenode(name="-1", group=simplegroup, max_total_gb=-1.0)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.check_over_max() is False

    node = storagenode(name="none", group=simplegroup, max_total_gb=None)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.check_over_max() is False


def test_namedcopytracked(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test StorageNode.named_copy_tracked()."""
    acqname = simplefile.acq.name
    filename = simplefile.name

    node = storagenode(name="present", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.named_copy_tracked(acqname, filename) is True

    node = storagenode(name="corrupt", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="X")
    assert node.named_copy_tracked(acqname, filename) is True

    node = storagenode(name="unknown", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="M")
    assert node.named_copy_tracked(acqname, filename) is True

    node = storagenode(name="removed", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="N")
    assert node.named_copy_tracked(acqname, filename) is False

    node = storagenode(name="missing", group=simplegroup)
    assert node.named_copy_tracked(acqname, filename) is False


def test_node_copystate(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test StorageNode.filecopy_state()."""

    node = storagenode(name="present", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.filecopy_state(simplefile) == "Y"

    node = storagenode(name="suspect", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="M")
    assert node.filecopy_state(simplefile) == "M"

    node = storagenode(name="corrupt", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="X")
    assert node.filecopy_state(simplefile) == "X"

    node = storagenode(name="removed", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="N")
    assert node.filecopy_state(simplefile) == "N"

    node = storagenode(name="missing", group=simplegroup)
    assert node.filecopy_state(simplefile) == "N"


def test_allfiles(simplenode, simpleacq, archivefile, archivefilecopy):
    """Test StorageNode.get_all_files()."""

    # Make some files
    file = archivefile(name="fileN", acq=simpleacq)
    archivefilecopy(file=file, node=simplenode, has_file="N")

    file = archivefile(name="fileM", acq=simpleacq)
    archivefilecopy(file=file, node=simplenode, has_file="M")

    file = archivefile(name="fileX", acq=simpleacq)
    archivefilecopy(file=file, node=simplenode, has_file="X")

    file = archivefile(name="fileY", acq=simpleacq)
    archivefilecopy(file=file, node=simplenode, has_file="Y")

    pathN = pathlib.PurePath(simpleacq.name, "fileN")
    pathM = pathlib.PurePath(simpleacq.name, "fileM")
    pathX = pathlib.PurePath(simpleacq.name, "fileX")
    pathY = pathlib.PurePath(simpleacq.name, "fileY")

    # Default case
    assert simplenode.get_all_files() == {pathY}

    # Try all the combinations
    for present in [True, False]:
        for corrupt in [True, False]:
            for unknown in [True, False]:
                for removed in [True, False]:
                    result = set()
                    if present:
                        result.add(pathY)
                    if corrupt:
                        result.add(pathX)
                    if unknown:
                        result.add(pathM)
                    if removed:
                        result.add(pathN)

                    assert (
                        simplenode.get_all_files(
                            present=present,
                            corrupt=corrupt,
                            unknown=unknown,
                            removed=removed,
                        )
                        == result
                    )


def test_update_avail_gb(simplenode):
    """test StorageNode.update_avail_gb()"""

    # The test node initially doesn't have this set
    assert simplenode.avail_gb is None

    # Test a number
    before = pw.utcnow()
    simplenode.update_avail_gb(10000, update_timestamp=True)
    # Now the value is set
    node = StorageNode.get(id=simplenode.id)

    avail = node.avail_gb
    assert avail == pytest.approx(10000.0 / 2.0**30)
    tdelta = node.avail_gb_last_checked - before
    assert abs(tdelta.total_seconds()) <= 10

    # Reset time
    StorageNode.update(avail_gb_last_checked=0).where(
        StorageNode.id == simplenode.id
    ).execute()

    # Test no timestamp update
    simplenode.update_avail_gb(20000)
    # Now the value is set
    node = StorageNode.get(id=simplenode.id)

    avail = node.avail_gb
    assert avail == pytest.approx(20000.0 / 2.0**30)
    assert node.avail_gb_last_checked == 0

    # Test None with timestamp update
    simplenode.update_avail_gb(None, update_timestamp=True)
    node = StorageNode.get(id=simplenode.id)
    assert node.avail_gb == avail
    tdelta = node.avail_gb_last_checked - before
    assert abs(tdelta.total_seconds()) <= 10

    # Reset time
    StorageNode.update(avail_gb_last_checked=0).where(
        StorageNode.id == simplenode.id
    ).execute()

    # Test None with no timestamp update
    simplenode.update_avail_gb(None)
    node = StorageNode.get(id=simplenode.id)
    assert node.avail_gb == avail
    assert node.avail_gb_last_checked == 0


def test_check_unregistered_none(dbtables, simplefile, simplenode):
    """Test StorageNode.check_unregistered with no pre-existing copy record."""

    assert simplenode.check_unregistered(simplefile, storage_used=1234)

    # Get the record
    copy = ArchiveFileCopy.get(id=1)

    assert copy.node == simplenode
    assert copy.file == simplefile
    assert copy.size_b == 1234
    assert copy.has_file == "M"
    assert copy.wants_file == "Y"
    assert copy.ready is False


def test_check_unregistered_corrupt(simplecopy):
    """Test StorageNode.check_unregistered with a pre-existing copy record."""

    # Make the copy corrupt
    simplecopy.ready = True
    simplecopy.has_file = "X"
    simplecopy.save()

    # Record wasn't updated
    assert not simplecopy.node.check_unregistered(simplecopy.file, storage_used=1234)

    # Get the record
    copy = ArchiveFileCopy.get(id=1)

    # Record wasn't updated
    assert copy.has_file == "X"
    assert copy.ready is True


def test_check_unregistered_gone(simplecopy):
    """Test StorageNode.check_unregistered with a pre-existing copy record."""

    # Set some parameters that the call will change
    simplecopy.ready = True
    simplecopy.wants_file = "M"
    simplecopy.has_file = "N"
    simplecopy.save()

    simplecopy.node.check_unregistered(simplecopy.file, storage_used=1234)

    # Get the record
    copy = ArchiveFileCopy.get(id=1)

    assert copy.size_b == 1234
    assert copy.has_file == "M"
    assert copy.wants_file == "Y"
    assert copy.ready is False
