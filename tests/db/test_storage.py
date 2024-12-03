"""test alpenhorn.storage."""

import pathlib

import peewee as pw
import pytest

from alpenhorn.db.storage import StorageGroup, StorageNode, StorageTransferAction


def test_schema(dbproxy, simplenode, storagetransferaction):
    # Force table creation
    storagetransferaction(node_from=simplenode, group_to=simplenode.group)

    assert set(dbproxy.get_tables()) == {
        "storagegroup",
        "storagenode",
        "storagetransferaction",
    }


def test_group_model(storagegroup):
    storagegroup(name="min")
    storagegroup(name="max", io_class="IOClass", io_config="{ioconfig}", notes="Notes")

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
        "min_avail_gb": 0,
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


def test_local(simplenode, hostname):
    """Test StorageNode.local"""

    simplenode.host = hostname
    assert simplenode.local

    simplenode.host = "other-host"
    assert not simplenode.local


def test_copy_state(simplenode, simpleacq, archivefile, archivefilecopy):
    """Test group.state_on_node()."""
    group = simplenode.group

    # None is returned here with 'N', even though an ArchiveFileCopy
    # exists with a specific node in the group.
    filen = archivefile(name="filen", acq=simpleacq, size_b=1)
    archivefilecopy(file=filen, node=simplenode, has_file="N")
    assert group.state_on_node(filen) == ("N", None)

    filey = archivefile(name="filey", acq=simpleacq, size_b=1)
    archivefilecopy(file=filey, node=simplenode, has_file="Y")
    assert group.state_on_node(filey) == ("Y", simplenode)

    filex = archivefile(name="filex", acq=simpleacq, size_b=1)
    archivefilecopy(file=filex, node=simplenode, has_file="X")
    assert group.state_on_node(filex) == ("X", simplenode)

    filem = archivefile(name="filem", acq=simpleacq, size_b=1)
    archivefilecopy(file=filem, node=simplenode, has_file="M")
    assert group.state_on_node(filem) == ("M", simplenode)

    # Non-existent file returns 'N'
    missing = archivefile(name="missing", acq=simpleacq, size_b=1)
    assert group.state_on_node(missing) == ("N", None)


def test_copy_state_multi(
    simplegroup, storagenode, simpleacq, archivefile, archivefilecopy
):
    """Test group.state_on_node() with mutliple copies on the node."""

    # Several nodes with different kinds of file copies
    nodeN = storagenode(name="N", group=simplegroup)
    nodeX = storagenode(name="X", group=simplegroup)
    nodeM = storagenode(name="M", group=simplegroup)
    nodeY = storagenode(name="Y", group=simplegroup)

    # X wins over N
    fileXN = archivefile(name="XN", acq=simpleacq)
    archivefilecopy(file=fileXN, node=nodeN, has_file="N")
    archivefilecopy(file=fileXN, node=nodeX, has_file="X")

    assert simplegroup.state_on_node(fileXN) == ("X", nodeX)

    # M wins over M and N
    fileMXN = archivefile(name="MXN", acq=simpleacq)
    archivefilecopy(file=fileMXN, node=nodeN, has_file="N")
    archivefilecopy(file=fileMXN, node=nodeX, has_file="X")
    archivefilecopy(file=fileMXN, node=nodeM, has_file="M")

    assert simplegroup.state_on_node(fileMXN) == ("M", nodeM)

    # Y wins over X, M and N
    fileYMXN = archivefile(name="YMXN", acq=simpleacq)
    archivefilecopy(file=fileYMXN, node=nodeN, has_file="N")
    archivefilecopy(file=fileYMXN, node=nodeX, has_file="X")
    archivefilecopy(file=fileYMXN, node=nodeM, has_file="M")
    archivefilecopy(file=fileYMXN, node=nodeY, has_file="Y")

    assert simplegroup.state_on_node(fileYMXN) == ("Y", nodeY)


def test_archive_property(simplegroup, storagenode):
    """Test the StorageNode.archive boolean."""
    node = storagenode(name="a", group=simplegroup, storage_type="A")
    assert node.archive is True

    node = storagenode(name="f", group=simplegroup, storage_type="F")
    assert node.archive is False

    node = storagenode(name="t", group=simplegroup, storage_type="T")
    assert node.archive is False


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
    simplenode.update_avail_gb(10000)
    # Now the value is set
    node = StorageNode.get(id=simplenode.id)
    after = pw.utcnow()

    avail = node.avail_gb
    assert avail == 10000.0 / 2.0**30
    assert node.avail_gb_last_checked >= before
    assert node.avail_gb_last_checked <= after

    # Reset time
    StorageNode.update(avail_gb_last_checked=0).where(
        StorageNode.id == simplenode.id
    ).execute()

    # Test None -- shouldn't change value, but last
    # update has happened
    simplenode.update_avail_gb(None)
    node = StorageNode.get(id=simplenode.id)
    assert node.avail_gb == avail
    assert node.avail_gb_last_checked >= after


def test_edge_model(storagetransferaction, storagenode, storagegroup):
    group1 = storagegroup(name="group1")
    node1 = storagenode(name="node1", group=group1)

    group2 = storagegroup(name="group2")
    node2 = storagenode(name="node2", group=group2)

    storagetransferaction(node_from=node1, group_to=group2)
    storagetransferaction(
        node_from=node2, group_to=group1, autosync=True, autoclean=True
    )

    # (node_from, group_to) is unique
    with pytest.raises(pw.IntegrityError):
        storagetransferaction(node_from=node1, group_to=group2)

    # Check records in DB
    assert StorageTransferAction.select().where(
        StorageTransferAction.node_from == node1,
        StorageTransferAction.group_to == group2,
    ).dicts().get() == {
        "id": 1,
        "node_from": node1.id,
        "group_to": group2.id,
        "autosync": False,
        "autoclean": False,
    }
    assert StorageTransferAction.select().where(
        StorageTransferAction.node_from == node2,
        StorageTransferAction.group_to == group1,
    ).dicts().get() == {
        "id": 2,
        "node_from": node2.id,
        "group_to": group1.id,
        "autosync": True,
        "autoclean": True,
    }


def test_edge_self_loop(storagetransferaction, storagenode, storagegroup):
    """StorageTransferAction.self_loop is True when node_from.group == group_to"""

    group1 = storagegroup(name="group1")
    node1 = storagenode(name="node1", group=group1)

    group2 = storagegroup(name="group2")
    storagenode(name="node2", group=group2)

    # Not a loop
    storagetransferaction(node_from=node1, group_to=group2)

    # Loop
    storagetransferaction(node_from=node1, group_to=group1)

    assert not StorageTransferAction.get(id=1).self_loop
    assert StorageTransferAction.get(id=2).self_loop
