"""
test_storage_model
------------------

Tests for `alpenhorn.storage` module.
"""

import pytest
import pathlib
import peewee as pw

from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.io.base import BaseNodeIO, BaseGroupIO, BaseNodeRemote


def test_schema(dbproxy, simplenode):
    assert set(dbproxy.get_tables()) == {"storagegroup", "storagenode"}


def test_group_model(storagegroup):
    storagegroup(name="min")
    storagegroup(name="max", io_class="IOClass", notes="Notes", io_config="{ioconfig}")

    # name is unique
    with pytest.raises(pw.IntegrityError):
        storagegroup(name="min")

    # Check records in DB
    assert StorageGroup.select().where(StorageGroup.name == "min").dicts().get() == {
        "id": 1,
        "name": "min",
        "io_class": None,
        "io_config": None,
        "notes": None,
    }
    assert StorageGroup.select().where(StorageGroup.name == "max").dicts().get() == {
        "id": 2,
        "name": "max",
        "io_class": "IOClass",
        "io_config": "{ioconfig}",
        "notes": "Notes",
    }


def test_storage_model(storagegroup, storagenode):
    group = storagegroup(name="group")
    storagenode(name="min", group=group)
    storagenode(
        name="max",
        group=group,
        active=True,
        address="addr.addr",
        auto_import=True,
        auto_verify=1,
        avail_gb=2.2,
        avail_gb_last_checked=3.3,
        host="host.host",
        io_class="IOClass",
        io_config="{ioconfig}",
        max_total_gb=4.4,
        min_avail_gb=5.5,
        notes="Notes",
        root="/root",
        storage_type="T",
        username="user",
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
        "address": None,
        "auto_import": False,
        "auto_verify": 0,
        "avail_gb": None,
        "avail_gb_last_checked": None,
        "host": None,
        "io_class": None,
        "io_config": None,
        "max_total_gb": None,
        "min_avail_gb": None,
        "notes": None,
        "root": None,
        "storage_type": "A",
        "username": None,
    }

    assert StorageNode.select().where(StorageNode.name == "max").dicts().get() == {
        "id": 2,
        "name": "max",
        "group": group.id,
        "active": True,
        "address": "addr.addr",
        "auto_import": True,
        "auto_verify": 1,
        "avail_gb": 2.2,
        "avail_gb_last_checked": 3.3,
        "host": "host.host",
        "io_class": "IOClass",
        "io_config": "{ioconfig}",
        "max_total_gb": 4.4,
        "min_avail_gb": 5.5,
        "notes": "Notes",
        "root": "/root",
        "storage_type": "T",
        "username": "user",
    }


def test_ioload(have_lfs, storagegroup, storagenode):
    """Test instantiation of the I/O classes"""

    for ioclass in ["Default", "Transport", "Nearline", None]:
        group = storagegroup(
            name="none" if ioclass is None else ioclass, io_class=ioclass
        )

    for ioclass, ioconfig in [
        ("Default", None),
        ("Polling", None),
        ("LFSQuota", '{"quota_group": "qgroup"}'),
        ("Nearline", '{"quota_group": "qgroup", "fixed_quota": 300000000}'),
        (None, None),
    ]:
        storagenode(
            name="none" if ioclass is None else ioclass,
            group=group,
            io_class=ioclass,
            io_config=ioconfig,
        )

    for node in StorageNode.select().execute():
        io = node.io
        assert isinstance(io, BaseNodeIO)
        remote = node.remote
        assert isinstance(remote, BaseNodeRemote)

    for group in StorageGroup.select().execute():
        io = group.io
        assert isinstance(io, BaseGroupIO)


def test_local(simplenode, hostname):
    """Test StorageNode.local"""

    simplenode.host = hostname
    assert simplenode.local

    simplenode.host = "other-host"
    assert not simplenode.local


def test_copy_state(
    simplenode, simpleacq, archivefile, simplefiletype, archivefilecopy
):
    """Test group.copy_state()."""
    group = simplenode.group

    filen = archivefile(name="filen", acq=simpleacq, type=simplefiletype, size_b=1)
    archivefilecopy(file=filen, node=simplenode, has_file="N")
    assert group.copy_state(filen) == "N"

    filey = archivefile(name="filey", acq=simpleacq, type=simplefiletype, size_b=1)
    archivefilecopy(file=filey, node=simplenode, has_file="Y")
    assert group.copy_state(filey) == "Y"

    filex = archivefile(name="filex", acq=simpleacq, type=simplefiletype, size_b=1)
    archivefilecopy(file=filex, node=simplenode, has_file="X")
    assert group.copy_state(filex) == "X"

    filem = archivefile(name="filem", acq=simpleacq, type=simplefiletype, size_b=1)
    archivefilecopy(file=filem, node=simplenode, has_file="M")
    assert group.copy_state(filem) == "M"

    # Non-existent file returns 'N'
    missing = archivefile(name="missing", acq=simpleacq, type=simplefiletype, size_b=1)
    assert group.copy_state(missing) == "N"


def test_archive_property(simplegroup, storagenode):
    """Test the StorageNode.archive boolean."""
    node = storagenode(name="a", group=simplegroup, storage_type="A")
    assert node.archive is True

    node = storagenode(name="f", group=simplegroup, storage_type="F")
    assert node.archive is False

    node = storagenode(name="t", group=simplegroup, storage_type="T")
    assert node.archive is False


def test_undermin(simplegroup, storagenode):
    """Test StorageNode.under_min()."""

    node = storagenode(name="anone", group=simplegroup, min_avail_gb=2.0)
    assert node.under_min() is False  # avail_gb is None

    node = storagenode(name="mnone", group=simplegroup, avail_gb=1.0)
    assert node.under_min() is False  # min_avail_gb is None

    node = storagenode(name="mzero", group=simplegroup, avail_gb=1.0, min_avail_gb=0.0)
    assert node.under_min() is False  # min_avail_gb is zero

    node = storagenode(name="false", group=simplegroup, avail_gb=3.0, min_avail_gb=2.0)
    assert node.under_min() is False

    node = storagenode(name="true", group=simplegroup, avail_gb=1.0, min_avail_gb=2.0)
    assert node.under_min() is True


def test_totalgb(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test StorageNode.totalgb()."""

    # Node with a copy (simplefile has size_b==1GiB)
    node = storagenode(name="good", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.total_gb() == 1.0

    # Node with bad copy
    node = storagenode(name="bad", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="X")
    assert node.total_gb() == 0.0

    # Node with no file copies
    node = storagenode(name="empty", group=simplegroup)
    assert node.total_gb() == 0.0


def test_overmax(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test StorageNode.over_max()."""

    # simplefile has size_b==1GiB
    node = storagenode(name="toofull", group=simplegroup, max_total_gb=0.1)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.over_max() is True

    node = storagenode(name="notfull", group=simplegroup, max_total_gb=2.0)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.over_max() is False

    node = storagenode(name="zero", group=simplegroup, max_total_gb=0)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.over_max() is False

    # This is the old default
    node = storagenode(name="-1", group=simplegroup, max_total_gb=-1.0)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.over_max() is False

    node = storagenode(name="none", group=simplegroup, max_total_gb=None)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.over_max() is False


def test_namedcopypresent(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test StorageNode.named_copy_present()."""
    acqname = simplefile.acq.name
    filename = simplefile.name

    node = storagenode(name="present", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.named_copy_present(acqname, filename) is True

    node = storagenode(name="corrupt", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="X")
    assert node.named_copy_present(acqname, filename) is False

    node = storagenode(name="missing", group=simplegroup)
    assert node.named_copy_present(acqname, filename) is False


def test_copypresent(simplegroup, storagenode, simplefile, archivefilecopy):
    """Test StorageNode.copy_present()."""

    node = storagenode(name="present", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="Y")
    assert node.copy_present(simplefile) is True

    node = storagenode(name="corrupt", group=simplegroup)
    archivefilecopy(file=simplefile, node=node, has_file="X")
    assert node.copy_present(simplefile) is False

    node = storagenode(name="missing", group=simplegroup)
    assert node.copy_present(simplefile) is False


def test_allfiles(simplenode, simpleacq, simplefiletype, archivefile, archivefilecopy):
    """Test StorageNode.all_files()."""

    # Empty
    file = archivefile(name="file1", acq=simpleacq, type=simplefiletype)
    archivefilecopy(file=file, node=simplenode, has_file="N")
    assert simplenode.all_files() == list()

    file = archivefile(name="file2", acq=simpleacq, type=simplefiletype)
    archivefilecopy(file=file, node=simplenode, has_file="Y")
    assert simplenode.all_files() == list([pathlib.PurePath(simpleacq.name, "file2")])
