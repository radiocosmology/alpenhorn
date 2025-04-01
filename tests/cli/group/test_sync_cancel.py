"""Test CLI: alpenhorn group sync --cancel"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
)


def test_no_node(clidb, cli):
    """Test a missing NODE name, with no --all."""

    StorageGroup.create(name="Group")

    cli(2, ["group", "sync", "Group", "--cancel"])


def test_all_no_cancel(clidb, cli):
    """Test --all without --cancel."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["group", "sync", "Group", "--all"])


def test_all_with_node(clidb, cli):
    """Test --all with a NODE."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["group", "sync", "Group", "Node", "--cancel", "--all"])


def test_cancel_target(clidb, cli):
    """Test --cancel --target."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["group", "sync", "Group", "Node", "--cancel", "--target=Group"])


def test_cancel(clidb, cli):
    """Test an unadorned cancel."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    group3 = StorageGroup.create(name="Group3")
    node3 = StorageNode.create(name="Node3", group=group3)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=1234)

    # Copy the file from everywhere to everywhere
    afcr12 = ArchiveFileCopyRequest.create(
        file=file, node_from=node1, group_to=group2, cancelled=0, completed=0
    )
    afcr13 = ArchiveFileCopyRequest.create(
        file=file, node_from=node1, group_to=group3, cancelled=0, completed=0
    )
    afcr21 = ArchiveFileCopyRequest.create(
        file=file, node_from=node2, group_to=group1, cancelled=0, completed=0
    )
    afcr23 = ArchiveFileCopyRequest.create(
        file=file, node_from=node2, group_to=group3, cancelled=0, completed=0
    )
    afcr31 = ArchiveFileCopyRequest.create(
        file=file, node_from=node3, group_to=group1, cancelled=0, completed=0
    )
    afcr32 = ArchiveFileCopyRequest.create(
        file=file, node_from=node3, group_to=group2, cancelled=0, completed=0
    )

    cli(0, ["group", "sync", "Group1", "Node2", "--cancel"], input="Y\n")

    # Only afcr21 should be cancelled
    assert ArchiveFileCopyRequest.get(id=afcr12.id).cancelled == 0
    assert ArchiveFileCopyRequest.get(id=afcr13.id).cancelled == 0
    assert ArchiveFileCopyRequest.get(id=afcr21.id).cancelled == 1
    assert ArchiveFileCopyRequest.get(id=afcr23.id).cancelled == 0
    assert ArchiveFileCopyRequest.get(id=afcr31.id).cancelled == 0
    assert ArchiveFileCopyRequest.get(id=afcr32.id).cancelled == 0


def test_decline(clidb, cli):
    """Test declining confirmation."""

    group1 = StorageGroup.create(name="Group1")
    StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=1234)

    afcr = ArchiveFileCopyRequest.create(
        file=file, node_from=node2, group_to=group1, cancelled=0, completed=0
    )

    cli(0, ["group", "sync", "Group1", "Node2", "--cancel"], input="N\n")

    # Not cancelled
    assert ArchiveFileCopyRequest.get(id=afcr.id).cancelled == 0


def test_force(clidb, cli):
    """Test forcing."""

    group1 = StorageGroup.create(name="Group1")
    StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=1234)

    afcr = ArchiveFileCopyRequest.create(
        file=file, node_from=node2, group_to=group1, cancelled=0, completed=0
    )

    cli(0, ["group", "sync", "Group1", "Node2", "--cancel", "--force"], input="N\n")

    # cancelled
    assert ArchiveFileCopyRequest.get(id=afcr.id).cancelled == 1


def test_check(clidb, cli):
    """Test --check."""

    group1 = StorageGroup.create(name="Group1")
    StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=1234)

    afcr = ArchiveFileCopyRequest.create(
        file=file, node_from=node2, group_to=group1, cancelled=0, completed=0
    )

    cli(0, ["group", "sync", "Group1", "Node2", "--cancel", "--check"])

    # not cancelled
    assert ArchiveFileCopyRequest.get(id=afcr.id).cancelled == 0


def test_cancel_acq(clidb, cli):
    """Test sync --cancel --acq."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq1")
    file1 = ArchiveFile.create(name="File", acq=acq, size_b=1234)
    afcr1 = ArchiveFileCopyRequest.create(
        file=file1, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    acq = ArchiveAcq.create(name="Acq2")
    file2 = ArchiveFile.create(name="File", acq=acq, size_b=1234)
    afcr2 = ArchiveFileCopyRequest.create(
        file=file2, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    acq = ArchiveAcq.create(name="Acq3")
    file3 = ArchiveFile.create(name="File", acq=acq, size_b=1234)
    afcr3 = ArchiveFileCopyRequest.create(
        file=file3, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    cli(
        0,
        [
            "group",
            "sync",
            "GroupTo",
            "NodeFrom",
            "--cancel",
            "--acq=Acq1",
            "--acq=Acq2",
        ],
        input="Y\n",
    )

    # afcr1 and 2 are cancelled
    assert ArchiveFileCopyRequest.get(id=afcr1.id).cancelled == 1
    assert ArchiveFileCopyRequest.get(id=afcr2.id).cancelled == 1
    assert ArchiveFileCopyRequest.get(id=afcr3.id).cancelled == 0


def test_show_acqs(clidb, cli):
    """Test sync --cancel --show-acqs."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq1 = ArchiveAcq.create(name="Acq1")

    file1 = ArchiveFile.create(name="File1", acq=acq1, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file1, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    file2 = ArchiveFile.create(name="File2", acq=acq1, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file2, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    acq2 = ArchiveAcq.create(name="Acq2")

    file3 = ArchiveFile.create(name="File3", acq=acq2, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file3, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    file4 = ArchiveFile.create(name="File4", acq=acq2, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file4, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    result = cli(
        0,
        ["group", "sync", "GroupTo", "NodeFrom", "--cancel", "--show-acqs"],
        input="Y\n",
    )

    assert "Acq1 [" in result.output
    assert "Acq2 [" in result.output


def test_show_acqs_files(clidb, cli):
    """Test --cancel --show-acqs --show-files."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq1 = ArchiveAcq.create(name="Acq1")

    file1 = ArchiveFile.create(name="File1", acq=acq1, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file1, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    file2 = ArchiveFile.create(name="File2", acq=acq1, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file2, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    acq2 = ArchiveAcq.create(name="Acq2")

    file3 = ArchiveFile.create(name="File3", acq=acq2, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file3, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    file4 = ArchiveFile.create(name="File4", acq=acq2, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file4, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    result = cli(
        0,
        [
            "group",
            "sync",
            "GroupTo",
            "NodeFrom",
            "--cancel",
            "--show-acqs",
            "--show-files",
        ],
        input="Y\n",
    )

    assert "Acq1/File1" in result.output
    assert "Acq1/File2" in result.output
    assert "Acq2/File3" in result.output
    assert "Acq2/File4" in result.output


def test_show_files(clidb, cli):
    """Test --cancel --show-files."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq1 = ArchiveAcq.create(name="Acq1")

    file1 = ArchiveFile.create(name="File1", acq=acq1, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file1, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    file2 = ArchiveFile.create(name="File2", acq=acq1, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file2, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    acq2 = ArchiveAcq.create(name="Acq2")

    file3 = ArchiveFile.create(name="File3", acq=acq2, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file3, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    file4 = ArchiveFile.create(name="File4", acq=acq2, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file4, node_from=node_from, group_to=group_to, cancelled=0, completed=0
    )

    result = cli(
        0,
        ["group", "sync", "GroupTo", "NodeFrom", "--cancel", "--show-files"],
        input="Y\n",
    )

    assert "Acq1/File1" in result.output
    assert "Acq1/File2" in result.output
    assert "Acq2/File3" in result.output
    assert "Acq2/File4" in result.output


def test_all(clidb, cli):
    """Test --cancel --all."""

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    group_from = StorageGroup.create(name="GroupFrom")
    node1 = StorageNode.create(name="Node1", group=group_from)
    node2 = StorageNode.create(name="Node2", group=group_from)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file, node_from=node1, group_to=group_to, cancelled=0, completed=0
    )
    ArchiveFileCopyRequest.create(
        file=file, node_from=node2, group_to=group_to, cancelled=0, completed=0
    )

    cli(
        0,
        ["group", "sync", "GroupTo", "--cancel", "--all"],
        input="Y\n",
    )

    # Both requests are cancelled
    assert ArchiveFileCopyRequest.get(id=1).cancelled == 1
    assert ArchiveFileCopyRequest.get(id=2).cancelled == 1


def test_file_list(clidb, cli, xfs):
    """Test sync --cancel --file-list."""

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    group_from = StorageGroup.create(name="GroupFrom")
    node = StorageNode.create(name="Node1", group=group_from)

    acq = ArchiveAcq.create(name="Acq")
    file1 = ArchiveFile.create(name="File1", acq=acq)
    file2 = ArchiveFile.create(name="File2", acq=acq)
    file3 = ArchiveFile.create(name="File3", acq=acq)

    ArchiveFileCopyRequest.create(
        file=file1, node_from=node, group_to=group_to, cancelled=0, completed=0
    )
    ArchiveFileCopyRequest.create(
        file=file2, node_from=node, group_to=group_to, cancelled=0, completed=0
    )
    ArchiveFileCopyRequest.create(
        file=file3, node_from=node, group_to=group_to, cancelled=0, completed=0
    )

    xfs.create_file("/file_list", contents="Acq/File1\n# Comment\nAcq/File3")

    cli(
        0,
        ["group", "sync", "GroupTo", "--cancel", "--all", "--file-list=/file_list"],
        input="Y\n",
    )

    # File1 and File3 are cancelled
    assert ArchiveFileCopyRequest.get(file=file1).cancelled == 1
    assert ArchiveFileCopyRequest.get(file=file2).cancelled == 0
    assert ArchiveFileCopyRequest.get(file=file3).cancelled == 1
