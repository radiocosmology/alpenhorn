"""Test CLI: alpenhorn node sync

Most of the test of the functionality for this command
happen in the "group sync" tests.
"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
    utcnow,
)


def test_no_group(clidb, cli):
    """Test a missing GROUP name, with no --all."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["node", "sync", "Node"])


def test_check_force(clidb, cli):
    """Test --check --force."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["node", "sync", "Node", "Group", "--check", "--force"])


def test_bad_node(clidb, cli):
    """Test a bad NODE name."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["node", "sync", "MISSING", "Group"])


def test_bad_group(clidb, cli):
    """Test a bad GROUP name."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["node", "sync", "Node", "MISSING"])


def test_sync(clidb, cli):
    """Test an unadorned sync."""

    before = utcnow().replace(microsecond=0)

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    node_to = StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    # File1 is on NodeFrom but not GroupTo
    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")

    # File2 is on NodeFrom and GroupTo
    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file2, node=node_to, has_file="Y", wants_file="Y")

    # File3 is not on NodeFrom but is in GroupTo
    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file3, node=node_to, has_file="Y", wants_file="Y")

    cli(0, ["node", "sync", "NodeFrom", "GroupTo"], input="Y\n")

    # Only file 1 is going to be transferred
    assert ArchiveFileCopyRequest.select().count() == 1
    afcr = ArchiveFileCopyRequest.get(id=1)
    assert afcr.file == file1
    assert afcr.node_from == node_from
    assert afcr.group_to == group_to
    assert afcr.cancelled == 0
    assert afcr.completed == 0
    assert afcr.timestamp >= before


def test_all_no_cancel(clidb, cli):
    """Test --all without --cancel."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["node", "sync", "Node", "--all"])


def test_all_with_node(clidb, cli):
    """Test --all with a NODE."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["node", "sync", "Node", "Group", "--cancel", "--all"])


def test_cancel_target(clidb, cli):
    """Test --cancel --target."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["node", "sync", "Node", "Group", "--cancel", "--target=Group"])


def test_all(clidb, cli):
    """Test --cancel --all."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group1 = StorageGroup.create(name="Group1")
    group2 = StorageGroup.create(name="Group2")

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=1234)
    ArchiveFileCopyRequest.create(
        file=file, node_from=node_from, group_to=group1, cancelled=0, completed=0
    )
    ArchiveFileCopyRequest.create(
        file=file, node_from=node_from, group_to=group2, cancelled=0, completed=0
    )

    cli(
        0,
        ["node", "sync", "NodeFrom", "--cancel", "--all"],
        input="Y\n",
    )

    # Both requests are cancelled
    assert ArchiveFileCopyRequest.get(id=1).cancelled == 1
    assert ArchiveFileCopyRequest.get(id=2).cancelled == 1


def test_file_list(clidb, cli, xfs):
    """Test sync --file-list."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")

    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")

    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file3, node=node_from, has_file="Y", wants_file="Y")

    # No newline at the end of this file
    xfs.create_file("/file_list", contents="Acq/File1\n# Comment\nAcq/File3")

    cli(
        0,
        [
            "node",
            "sync",
            "NodeFrom",
            "GroupTo",
            "--force",
            "--file-list=/file_list",
        ],
    )

    # File1 and File3 are transferred
    assert ArchiveFileCopyRequest.select().count() == 2
    assert ArchiveFileCopyRequest.get(id=1).file == file1
    assert ArchiveFileCopyRequest.get(id=2).file == file3
