"""Test ArchiveFileCopy.trigger_autoactions"""

import datetime

import peewee as pw
import pytest

from alpenhorn.db import ArchiveFileCopy, ArchiveFileCopyRequest


def test_autosync(dbtables, simplecopy, simplegroup, storagetransferaction):
    """Test triggering autosync."""

    # When a copy is added to simplecopy.node, queue a transfer to simplegroup
    storagetransferaction(
        node_from=simplecopy.node, group_to=simplegroup, autosync=True
    )

    simplecopy.trigger_autoactions()

    assert ArchiveFileCopyRequest.get(
        file=simplecopy.file,
        node_from=simplecopy.node,
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
    """autosync triggers whenever dest doesn't have a good copy."""

    destnode = storagenode(name="dest", group=simplegroup)
    # When a copy is added to simplenode, queue a transfer to simplegroup
    storagetransferaction(node_from=simplenode, group_to=simplegroup, autosync=True)

    # Copies with different states
    fileY = archivefile(name="fileY", acq=simpleacq)
    copyY = archivefilecopy(file=fileY, node=simplenode, has_file="Y")
    archivefilecopy(file=fileY, node=destnode, has_file="Y")

    fileX = archivefile(name="fileX", acq=simpleacq)
    copyX = archivefilecopy(file=fileX, node=simplenode, has_file="Y")
    archivefilecopy(file=fileX, node=destnode, has_file="X")

    fileM = archivefile(name="fileM", acq=simpleacq)
    copyM = archivefilecopy(file=fileM, node=simplenode, has_file="Y")
    archivefilecopy(file=fileM, node=destnode, has_file="M")

    fileN = archivefile(name="fileN", acq=simpleacq)
    copyN = archivefilecopy(file=fileN, node=simplenode, has_file="Y")
    archivefilecopy(file=fileN, node=destnode, has_file="N")

    # This one shouldn't add a new copy request
    copyY.trigger_autoactions()

    with pytest.raises(pw.DoesNotExist):
        ArchiveFileCopyRequest.get(file=fileY)

    # But all these should
    for copy in [copyX, copyM, copyN]:
        copy.trigger_autoactions()
        assert ArchiveFileCopyRequest.get(
            file=copy.file,
            node_from=simplenode,
            group_to=simplegroup,
            completed=0,
            cancelled=0,
        )


def test_autosync_loop(dbtables, simplecopy, storagetransferaction):
    """autosync doesn't trigger on graph loops."""

    # When a copy is added to simplecopy.node, queue a transfer to simplecopy.node.group
    # (this is a loop requesting a copy from a group to itself)
    storagetransferaction(
        node_from=simplecopy.node, group_to=simplecopy.node.group, autosync=True
    )

    simplecopy.trigger_autoactions()

    with pytest.raises(pw.DoesNotExist):
        ArchiveFileCopyRequest.get(file=simplecopy.file)


def test_autoclean(
    archivefilecopy,
    storagenode,
    simplecopy,
    simplenode,
    storagetransferaction,
):
    """Test triggering autoclean."""

    before = pw.utcnow() - datetime.timedelta(seconds=2)

    # When a copy is added to simplecopy.node.group, delete it from simplenode
    storagetransferaction(
        node_from=simplenode, group_to=simplecopy.node.group, autoclean=True
    )

    # This is the copy to delete
    archivefilecopy(file=simplecopy.file, node=simplenode, wants_file="Y", has_file="Y")

    simplecopy.trigger_autoactions()

    copy = ArchiveFileCopy.get(file=simplecopy.file, node=simplenode)
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
    # When a copy is added to simplenode.group, delete it from srcnode
    storagetransferaction(node_from=srcnode, group_to=simplenode.group, autoclean=True)

    # Copies with different states on srcnode
    fileY = archivefile(name="fileY", acq=simpleacq)
    archivefilecopy(
        file=fileY, node=srcnode, has_file="Y", wants_file="Y", last_update=then
    )
    copyY = archivefilecopy(file=fileY, node=simplenode, has_file="Y", wants_file="Y")

    fileX = archivefile(name="fileX", acq=simpleacq)
    archivefilecopy(
        file=fileX, node=srcnode, has_file="X", wants_file="Y", last_update=then
    )
    copyX = archivefilecopy(file=fileX, node=simplenode, has_file="Y", wants_file="Y")

    fileM = archivefile(name="fileM", acq=simpleacq)
    archivefilecopy(
        file=fileM, node=srcnode, has_file="M", wants_file="Y", last_update=then
    )
    copyM = archivefilecopy(file=fileM, node=simplenode, has_file="Y", wants_file="Y")

    fileN = archivefile(name="fileN", acq=simpleacq)
    archivefilecopy(
        file=fileN, node=srcnode, has_file="N", wants_file="Y", last_update=then
    )
    copyN = archivefilecopy(file=fileN, node=simplenode, has_file="Y", wants_file="Y")

    # None of these should be deleted
    for copy in [copyN, copyX, copyM]:
        copy.trigger_autoactions()

        # Check
        srccopy = ArchiveFileCopy.get(file=copy.file, node=srcnode)
        assert srccopy.last_update == then
        assert srccopy.wants_file == "Y"

    # But this one should
    copyY.trigger_autoactions()

    srccopy = ArchiveFileCopy.get(file=fileY, node=srcnode)
    assert srccopy.last_update > then
    assert srccopy.wants_file == "N"


def test_autoclean_loop(archivefilecopy, simplefile, simplenode, storagetransferaction):
    """autoclean doesn't trigger on graph loops."""

    # When a file is added to simplenode.group, delete it from simplenode
    # (This is a loop requesting deletion of the file just added)
    storagetransferaction(
        node_from=simplenode, group_to=simplenode.group, autoclean=True
    )
    copy = archivefilecopy(
        file=simplefile, node=simplenode, wants_file="Y", has_file="Y"
    )

    copy.trigger_autoactions()

    # Check that copy wasn't deleted
    copy = ArchiveFileCopy.get(file=simplefile, node=simplenode)
    assert copy.wants_file == "Y"
