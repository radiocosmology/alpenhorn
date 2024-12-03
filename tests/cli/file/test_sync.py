"""Test CLI: alpenhorn file sync"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
)


def test_bad_file(clidb, cli):
    """Test a bad file."""

    dest = StorageGroup.create(name="Dest")
    StorageNode.create(name="DestNode", group=dest)

    group = StorageGroup.create(name="SrcGroup")
    StorageNode.create(name="Src", group=group)

    cli(1, ["file", "sync", "MIS/SING", "--from=Src", "--to=Dest"])


def test_no_source(clidb, cli):
    """Test a missing source node."""

    dest = StorageGroup.create(name="Dest")
    StorageNode.create(name="DestNode", group=dest)

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(2, ["file", "sync", "Acq/File", "--to=Dest"])


def test_bad_source(clidb, cli):
    """Test a bad source node."""

    dest = StorageGroup.create(name="Dest")
    StorageNode.create(name="DestNode", group=dest)

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(1, ["file", "sync", "Acq/File", "--from=MISSING", "--to=Dest"])


def test_no_dest(clidb, cli):
    """Test missing dest group."""

    group = StorageGroup.create(name="SrcGroup")
    StorageNode.create(name="Src", group=group)

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(2, ["file", "sync", "Acq/File", "--from=Src"])


def test_bad_dest(clidb, cli):
    """Test a bad dest group."""

    group = StorageGroup.create(name="SrcGroup")
    StorageNode.create(name="Src", group=group)

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(1, ["file", "sync", "Acq/File", "--from=Src", "--to=MISSING"])


def test_no_src(clidb, cli):
    """Test file missing from src."""

    dest = StorageGroup.create(name="Dest")
    StorageNode.create(name="DestNode", group=dest)

    group = StorageGroup.create(name="SrcGroup")
    StorageNode.create(name="Src", group=group)

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(0, ["file", "sync", "Acq/File", "--from=Src", "--to=Dest"])

    # No request
    assert ArchiveFileCopyRequest.select().count() == 0


def test_sync(clidb, cli):
    """Test a good sync."""

    dest = StorageGroup.create(name="Dest")
    StorageNode.create(name="DestNode", group=dest)

    group = StorageGroup.create(name="SrcGroup")
    src = StorageNode.create(name="Src", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=src, has_file="Y", wants_file="Y")

    cli(0, ["file", "sync", "Acq/File", "--from=Src", "--to=Dest", "--force"])

    # Request created
    afcr = ArchiveFileCopyRequest.get(id=1)
    assert afcr.file == file_
    assert afcr.node_from == src
    assert afcr.group_to == dest
    assert afcr.completed == 0
    assert afcr.cancelled == 0


def test_force(clidb, cli):
    """Test force with no source file."""

    dest = StorageGroup.create(name="Dest")
    StorageNode.create(name="DestNode", group=dest)

    group = StorageGroup.create(name="SrcGroup")
    src = StorageNode.create(name="Src", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)

    cli(0, ["file", "sync", "Acq/File", "--from=Src", "--to=Dest", "--force"])

    # Request created
    afcr = ArchiveFileCopyRequest.get(id=1)
    assert afcr.file == file_
    assert afcr.node_from == src
    assert afcr.group_to == dest
    assert afcr.completed == 0
    assert afcr.cancelled == 0


def test_dest_has(clidb, cli):
    """Test file already on dest."""

    dest = StorageGroup.create(name="Dest")
    node = StorageNode.create(name="DestNode", group=dest)

    group = StorageGroup.create(name="SrcGroup")
    src = StorageNode.create(name="Src", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=src, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(0, ["file", "sync", "Acq/File", "--from=Src", "--to=Dest"])

    # No request
    assert ArchiveFileCopyRequest.select().count() == 0


def test_cancel_bad_file(clidb, cli):
    """Test a bad file with --cancel."""

    cli(1, ["file", "sync", "MIS/SING", "--cancel"])


def test_cancel_all(clidb, cli):
    """Test --cancel without limit."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file1 = ArchiveFile.create(name="File1", acq=acq)
    file2 = ArchiveFile.create(name="File2", acq=acq)

    req0 = ArchiveFileCopyRequest.create(
        file=file1, node_from=node, group_to=group, cancelled=0, completed=0
    )
    req1 = ArchiveFileCopyRequest.create(
        file=file1, node_from=node, group_to=group, cancelled=0, completed=1
    )
    req2 = ArchiveFileCopyRequest.create(
        file=file1, node_from=node, group_to=group, cancelled=1, completed=0
    )
    req3 = ArchiveFileCopyRequest.create(
        file=file1, node_from=node, group_to=group, cancelled=1, completed=1
    )
    req4 = ArchiveFileCopyRequest.create(
        file=file2, node_from=node, group_to=group, cancelled=0, completed=0
    )

    cli(0, ["file", "sync", "Acq/File1", "--cancel"])

    # req0 newly cancelled.   req2, 3 previously cancelled.  Others unchanged.
    assert ArchiveFileCopyRequest.get(id=req0.id).cancelled
    assert not ArchiveFileCopyRequest.get(id=req1.id).cancelled
    assert ArchiveFileCopyRequest.get(id=req2.id).cancelled
    assert ArchiveFileCopyRequest.get(id=req3.id).cancelled
    assert not ArchiveFileCopyRequest.get(id=req4.id).cancelled


def test_cancel_bad_node(clidb, cli):
    """Test --cancel with a bad --from."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(1, ["file", "sync", "Acq/File", "--cancel", "--from=Missing"])


def test_cancel_node(clidb, cli):
    """Test --cancel with --from."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    ArchiveFileCopyRequest.create(
        file=file, node_from=node1, group_to=group, cancelled=0, completed=0
    )
    ArchiveFileCopyRequest.create(
        file=file, node_from=node2, group_to=group, cancelled=0, completed=0
    )

    cli(0, ["file", "sync", "Acq/File", "--cancel", "--from=Node1"])

    assert ArchiveFileCopyRequest.get(node_from=node1).cancelled
    assert not ArchiveFileCopyRequest.get(node_from=node2).cancelled


def test_cancel_bad_group(clidb, cli):
    """Test --cancel with a bad --to."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(1, ["file", "sync", "Acq/File", "--cancel", "--to=Missing"])


def test_cancel_group(clidb, cli):
    """Test --cancel with --to."""

    group1 = StorageGroup.create(name="Group1")
    group2 = StorageGroup.create(name="Group2")
    node = StorageNode.create(name="Node", group=group1)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    ArchiveFileCopyRequest.create(
        file=file, node_from=node, group_to=group1, cancelled=0, completed=0
    )
    ArchiveFileCopyRequest.create(
        file=file, node_from=node, group_to=group2, cancelled=0, completed=0
    )

    cli(0, ["file", "sync", "Acq/File", "--cancel", "--to=Group1"])

    assert ArchiveFileCopyRequest.get(group_to=group1).cancelled
    assert not ArchiveFileCopyRequest.get(group_to=group2).cancelled


def test_cancel_full(clidb, cli):
    """Test --cancel with both --from and --to."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    req11 = ArchiveFileCopyRequest.create(
        file=file, node_from=node1, group_to=group1, cancelled=0, completed=0
    )
    req12 = ArchiveFileCopyRequest.create(
        file=file, node_from=node1, group_to=group2, cancelled=0, completed=0
    )
    req21 = ArchiveFileCopyRequest.create(
        file=file, node_from=node2, group_to=group1, cancelled=0, completed=0
    )
    req22 = ArchiveFileCopyRequest.create(
        file=file, node_from=node2, group_to=group2, cancelled=0, completed=0
    )

    cli(0, ["file", "sync", "Acq/File", "--cancel", "--from=Node1", "--to=Group2"])

    assert not ArchiveFileCopyRequest.get(id=req11.id).cancelled
    assert ArchiveFileCopyRequest.get(id=req12.id).cancelled
    assert not ArchiveFileCopyRequest.get(id=req21.id).cancelled
    assert not ArchiveFileCopyRequest.get(id=req22.id).cancelled
