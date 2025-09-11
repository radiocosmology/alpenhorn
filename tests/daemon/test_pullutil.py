"""Test alpenhorn.daemon.pullutil"""

import datetime

import peewee as pw
import pytest

from alpenhorn.daemon import pullutil
from alpenhorn.db.archive import ArchiveFileCopy, ArchiveFileCopyRequest


def test_autosync(dbtables, simplefile, simplenode, simplegroup, storagetransferaction):
    """Test post_add running autosync."""

    storagetransferaction(node_from=simplenode, group_to=simplegroup, autosync=True)

    pullutil.post_add(simplenode, simplefile)

    assert ArchiveFileCopyRequest.get(
        file=simplefile,
        node_from=simplenode,
        group_to=simplegroup,
        completed=0,
        cancelled=0,
    )


def test_autosync_state(
    dbtables,
    archivefilecopy,
    archivefile,
    simpleacq,
    storagenode,
    simplenode,
    simplegroup,
    storagetransferaction,
):
    """post_add autosync copies whenever dest doesn't have a good copy."""

    destnode = storagenode(name="dest", group=simplegroup)
    storagetransferaction(node_from=simplenode, group_to=simplegroup, autosync=True)

    # Copies with different states
    fileY = archivefile(name="fileY", acq=simpleacq)
    archivefilecopy(file=fileY, node=destnode, has_file="Y")

    fileX = archivefile(name="fileX", acq=simpleacq)
    archivefilecopy(file=fileX, node=destnode, has_file="X")

    fileM = archivefile(name="fileM", acq=simpleacq)
    archivefilecopy(file=fileM, node=destnode, has_file="M")

    fileN = archivefile(name="fileN", acq=simpleacq)
    archivefilecopy(file=fileN, node=destnode, has_file="N")

    # This one shouldn't add a new copy request
    pullutil.post_add(simplenode, fileY)

    with pytest.raises(pw.DoesNotExist):
        ArchiveFileCopyRequest.get(file=fileY)

    # But all these should
    for f in [fileX, fileM, fileN]:
        pullutil.post_add(simplenode, f)
        assert ArchiveFileCopyRequest.get(
            file=f, node_from=simplenode, group_to=simplegroup, completed=0, cancelled=0
        )


def test_autosync_loop(dbtables, simplefile, simplenode, storagetransferaction):
    """post_add autosync ignores graph loops."""

    storagetransferaction(
        node_from=simplenode, group_to=simplenode.group, autosync=True
    )

    pullutil.post_add(simplenode, simplefile)

    with pytest.raises(pw.DoesNotExist):
        ArchiveFileCopyRequest.get(file=simplefile)


def test_autoclean(
    archivefilecopy,
    simplefile,
    simplenode,
    storagenode,
    simplegroup,
    storagetransferaction,
):
    """Test post_add running autoclean."""

    before = pw.utcnow() - datetime.timedelta(seconds=2)

    destnode = storagenode(name="dest", group=simplegroup)

    storagetransferaction(node_from=simplenode, group_to=simplegroup, autoclean=True)
    archivefilecopy(file=simplefile, node=simplenode, wants_file="Y", has_file="Y")

    pullutil.post_add(destnode, simplefile)

    copy = ArchiveFileCopy.get(file=simplefile, node=simplenode)
    assert copy.wants_file == "N"
    assert copy.last_update >= before


def test_autoclean_state(
    archivefile,
    archivefilecopy,
    simpleacq,
    storagenode,
    simplenode,
    simplegroup,
    storagetransferaction,
):
    """post_add autoclean only deletes copies with has_file=='Y'."""

    then = pw.utcnow() - datetime.timedelta(seconds=200)

    srcnode = storagenode(name="src", group=simplegroup)
    storagetransferaction(node_from=srcnode, group_to=simplenode.group, autoclean=True)

    # Copies with different states
    fileY = archivefile(name="fileY", acq=simpleacq)
    archivefilecopy(
        file=fileY, node=srcnode, has_file="Y", wants_file="Y", last_update=then
    )

    fileX = archivefile(name="fileX", acq=simpleacq)
    archivefilecopy(
        file=fileX, node=srcnode, has_file="X", wants_file="Y", last_update=then
    )

    fileM = archivefile(name="fileM", acq=simpleacq)
    archivefilecopy(
        file=fileM, node=srcnode, has_file="M", wants_file="Y", last_update=then
    )

    fileN = archivefile(name="fileN", acq=simpleacq)
    archivefilecopy(
        file=fileN, node=srcnode, has_file="N", wants_file="Y", last_update=then
    )

    # None of these should be deleted
    for f in [fileN, fileX, fileM]:
        pullutil.post_add(simplenode, f)

        copy = ArchiveFileCopy.get(file=f, node=srcnode)
        assert copy.last_update == then
        assert copy.wants_file == "Y"

    # But this one should
    pullutil.post_add(simplenode, fileY)

    copy = ArchiveFileCopy.get(file=fileY, node=srcnode)
    assert copy.last_update > then
    assert copy.wants_file == "N"


def test_autoclean_loop(archivefilecopy, simplefile, simplenode, storagetransferaction):
    """post_add autoclean ignores graph loops."""

    storagetransferaction(
        node_from=simplenode, group_to=simplenode.group, autoclean=True
    )
    archivefilecopy(file=simplefile, node=simplenode, wants_file="Y", has_file="Y")

    pullutil.post_add(simplenode, simplefile)

    copy = ArchiveFileCopy.get(file=simplefile, node=simplenode)
    assert copy.wants_file == "Y"
