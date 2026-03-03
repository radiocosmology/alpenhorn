"""test alpenhorn.storage."""

import peewee as pw
import pytest

from alpenhorn.db.storage import StorageGroup


def test_schema(dbproxy, storagegroup):
    # Force table creation
    storagegroup(name="group")

    assert set(dbproxy.get_tables()) == {
        "storagegroup",
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
