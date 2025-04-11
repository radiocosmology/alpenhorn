"""Test CLI: alpenhorn group sync

But not "alpenhorn group sync --cancel" tests.
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


def test_no_node(clidb, cli):
    """Test a missing NODE name, with no --all."""

    StorageGroup.create(name="Group")

    cli(2, ["group", "sync", "Group"])


def test_check_force(clidb, cli):
    """Test --check --force."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["group", "sync", "Group", "Node", "--check", "--force"])


def test_bad_group(clidb, cli):
    """Test a bad GROUP name."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["group", "sync", "MISSING", "Node"])


def test_bad_node(clidb, cli):
    """Test a bad NODE name."""

    StorageGroup.create(name="Group")

    cli(1, ["group", "sync", "Group", "MISSING"])


def test_bad_acq(clidb, cli):
    """Test a bad --acq name."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["group", "sync", "Group", "Node", "--acq=MISSING"])


def test_bad_target(clidb, cli):
    """Test a bad --target name."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["group", "sync", "Group", "Node", "--target=MISSING"])


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

    cli(0, ["group", "sync", "GroupTo", "NodeFrom"], input="Y\n")

    # Only file 1 is going to be transferred
    assert ArchiveFileCopyRequest.select().count() == 1
    afcr = ArchiveFileCopyRequest.get(id=1)
    assert afcr.file == file1
    assert afcr.node_from == node_from
    assert afcr.group_to == group_to
    assert afcr.cancelled == 0
    assert afcr.completed == 0
    assert afcr.timestamp >= before


def test_sync_existing(clidb, cli):
    """Test an sync with an existing AFCR."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node_from, has_file="Y", wants_file="Y")
    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node_from,
        group_to=group_to,
        timestamp=1,
        completed=0,
        cancelled=0,
    )

    cli(0, ["group", "sync", "GroupTo", "NodeFrom"], input="Y\n")

    # No change to AFCR
    assert ArchiveFileCopyRequest.select().count() == 1
    afcr = ArchiveFileCopyRequest.get(id=1)
    assert afcr.cancelled == 0
    assert afcr.completed == 0
    assert afcr.timestamp == 1


def test_sync_prior(clidb, cli):
    """Test a sync with an prior AFCR."""

    before = utcnow().replace(microsecond=0)

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node_from, has_file="Y", wants_file="Y")
    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node_from,
        group_to=group_to,
        timestamp=1,
        completed=0,
        cancelled=1,
    )

    cli(0, ["group", "sync", "GroupTo", "NodeFrom"], input="Y\n")

    # New AFCR inserted
    assert ArchiveFileCopyRequest.select().count() == 2

    # First AFCR still cancelled
    assert ArchiveFileCopyRequest.get(id=1).cancelled == 1

    afcr = ArchiveFileCopyRequest.get(id=2)
    assert afcr.cancelled == 0
    assert afcr.completed == 0
    assert afcr.timestamp >= before


def test_sync_different(clidb, cli):
    """Test a sync with pending, but different AFCRs."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    node_to = StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node_from, has_file="Y", wants_file="Y")
    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=1234)

    # Some of these don't make sense, but that's okay
    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node_from,
        group_to=group_to,
        timestamp=1,
        completed=1,
        cancelled=0,
    )
    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node_from,
        group_to=group_from,
        timestamp=1,
        completed=0,
        cancelled=0,
    )
    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node_to,
        group_to=group_to,
        timestamp=1,
        completed=0,
        cancelled=0,
    )
    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node_to,
        group_to=group_from,
        timestamp=1,
        completed=0,
        cancelled=0,
    )
    ArchiveFileCopyRequest.create(
        file=file2,
        node_from=node_from,
        group_to=group_to,
        timestamp=1,
        completed=0,
        cancelled=0,
    )

    cli(0, ["group", "sync", "GroupTo", "NodeFrom"], input="Y\n")

    # New AFCR inserted
    assert ArchiveFileCopyRequest.select().count() == 6


def test_sync_declined(clidb, cli):
    """Test declining confirmation."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node_from, has_file="Y", wants_file="Y")

    cli(0, ["group", "sync", "GroupTo", "NodeFrom"], input="N\n")

    # No AFCR inserted
    assert ArchiveFileCopyRequest.select().count() == 0


def test_sync_force(clidb, cli):
    """Test forcing a sync."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node_from, has_file="Y", wants_file="Y")

    # Input is ignored
    cli(0, ["group", "sync", "GroupTo", "NodeFrom", "--force"], input="N\n")

    # AFCR inserted
    assert ArchiveFileCopyRequest.select().count() == 1


def test_sync_check(clidb, cli):
    """Test sync --check."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node_from, has_file="Y", wants_file="Y")

    # Input is ignored
    cli(0, ["group", "sync", "GroupTo", "NodeFrom", "--check"], input="Y\n")

    # No AFCR inserted
    assert ArchiveFileCopyRequest.select().count() == 0


def test_sync_acq(clidb, cli):
    """Test sync --acq."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq1")
    file1 = ArchiveFile.create(name="File", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")

    acq = ArchiveAcq.create(name="Acq2")
    file2 = ArchiveFile.create(name="File", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")

    acq = ArchiveAcq.create(name="Acq3")
    file3 = ArchiveFile.create(name="File", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file3, node=node_from, has_file="Y", wants_file="Y")

    cli(
        0,
        ["group", "sync", "GroupTo", "NodeFrom", "--acq=Acq1", "--acq=Acq2"],
        input="Y\n",
    )

    # file1 and 2 are synced
    assert ArchiveFileCopyRequest.select().count() == 2
    assert set(
        ArchiveFileCopyRequest.select(ArchiveAcq.name)
        .join(ArchiveFile)
        .join(ArchiveAcq)
        .scalars()
    ) == {"Acq1", "Acq2"}


def test_sync_target(clidb, cli):
    """Test sync --target."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    target1g = StorageGroup.create(name="Target1")
    target1n = StorageNode.create(name="Target1", group=target1g)

    target2g = StorageGroup.create(name="Target2")
    target2n = StorageNode.create(name="Target2", group=target2g)

    target3g = StorageGroup.create(name="Target3")
    target3n = StorageNode.create(name="Target3", group=target3g)

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file1, node=target1n, has_file="Y", wants_file="Y")

    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file2, node=target2n, has_file="Y", wants_file="Y")

    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file3, node=node_from, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file3, node=target3n, has_file="Y", wants_file="Y")

    cli(
        0,
        [
            "group",
            "sync",
            "GroupTo",
            "NodeFrom",
            "--target=Target1",
            "--target=Target2",
        ],
        input="Y\n",
    )

    # Only file3 is transferred
    assert ArchiveFileCopyRequest.select().count() == 1
    assert ArchiveFileCopyRequest.get(id=1).file == file3


def test_sync_show_acqs(clidb, cli):
    """Test sync --show-acqs."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq1 = ArchiveAcq.create(name="Acq1")

    file1 = ArchiveFile.create(name="File1", acq=acq1, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")

    file2 = ArchiveFile.create(name="File2", acq=acq1, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")

    acq2 = ArchiveAcq.create(name="Acq2")

    file3 = ArchiveFile.create(name="File3", acq=acq2, size_b=1234)
    ArchiveFileCopy.create(file=file3, node=node_from, has_file="Y", wants_file="Y")

    file4 = ArchiveFile.create(name="File4", acq=acq2, size_b=1234)
    ArchiveFileCopy.create(file=file4, node=node_from, has_file="Y", wants_file="Y")

    result = cli(
        0,
        ["group", "sync", "GroupTo", "NodeFrom", "--show-acqs"],
        input="Y\n",
    )

    assert "Acq1 [" in result.output
    assert "Acq2 [" in result.output


def test_sync_show_acqs_files(clidb, cli):
    """Test sync --show-acqs --show-files."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq1 = ArchiveAcq.create(name="Acq1")

    file1 = ArchiveFile.create(name="File1", acq=acq1, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")

    file2 = ArchiveFile.create(name="File2", acq=acq1, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")

    acq2 = ArchiveAcq.create(name="Acq2")

    file3 = ArchiveFile.create(name="File3", acq=acq2, size_b=1234)
    ArchiveFileCopy.create(file=file3, node=node_from, has_file="Y", wants_file="Y")

    file4 = ArchiveFile.create(name="File4", acq=acq2, size_b=1234)
    ArchiveFileCopy.create(file=file4, node=node_from, has_file="Y", wants_file="Y")

    result = cli(
        0,
        ["group", "sync", "GroupTo", "NodeFrom", "--show-acqs", "--show-files"],
        input="Y\n",
    )

    assert "Acq1/File1" in result.output
    assert "Acq1/File2" in result.output
    assert "Acq2/File3" in result.output
    assert "Acq2/File4" in result.output


def test_sync_show_files(clidb, cli):
    """Test sync --show-files."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq1 = ArchiveAcq.create(name="Acq1")

    file1 = ArchiveFile.create(name="File1", acq=acq1, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")

    file2 = ArchiveFile.create(name="File2", acq=acq1, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")

    acq2 = ArchiveAcq.create(name="Acq2")

    file3 = ArchiveFile.create(name="File3", acq=acq2, size_b=1234)
    ArchiveFileCopy.create(file=file3, node=node_from, has_file="Y", wants_file="Y")

    file4 = ArchiveFile.create(name="File4", acq=acq2, size_b=1234)
    ArchiveFileCopy.create(file=file4, node=node_from, has_file="Y", wants_file="Y")

    result = cli(
        0,
        ["group", "sync", "GroupTo", "NodeFrom", "--show-files"],
        input="Y\n",
    )

    assert "Acq1/File1" in result.output
    assert "Acq1/File2" in result.output
    assert "Acq2/File3" in result.output
    assert "Acq2/File4" in result.output


def test_file_list_missing(clidb, cli):
    """Test sync --file-list with non-existent file."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["group", "sync", "Group", "Node", "--file-list=missing"])


def test_file_list_bad_file(clidb, cli, xfs):
    """Test sync --file-list with a bad entry."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from)

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")

    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")

    xfs.create_file("/file_list", contents="Acq/File1\n# Comment\nAcq/File3")

    cli(
        1,
        [
            "group",
            "sync",
            "GroupTo",
            "NodeFrom",
            "--force",
            "--file-list=/file_list",
        ],
    )

    # No files synced
    assert ArchiveFileCopyRequest.select().count() == 0


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
            "group",
            "sync",
            "GroupTo",
            "NodeFrom",
            "--force",
            "--file-list=/file_list",
        ],
    )

    # File1 and File3 are transferred
    assert ArchiveFileCopyRequest.select().count() == 2
    assert ArchiveFileCopyRequest.get(id=1).file == file1
    assert ArchiveFileCopyRequest.get(id=2).file == file3


def test_from_stdin(clidb, cli):
    """Test sync --file-list=-."""

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

    cli(
        0,
        [
            "group",
            "sync",
            "GroupTo",
            "NodeFrom",
            "--file-list=-",
            "--force",
        ],
        input="Acq/File1\n# Comment\nAcq/File3",
    )

    # File1 and File3 are transferred
    assert ArchiveFileCopyRequest.select().count() == 2
    assert ArchiveFileCopyRequest.get(id=1).file == file1
    assert ArchiveFileCopyRequest.get(id=2).file == file3


def test_from_stdin_unforced(clidb, cli):
    """Test sync --file-list=-. without --force"""

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

    cli(
        0,
        [
            "group",
            "sync",
            "GroupTo",
            "NodeFrom",
            "--file-list=-",
        ],
        input="Acq/File1\n# Comment\nAcq/File3",
    )

    # Nothing is transferred because --check was turned on
    assert ArchiveFileCopyRequest.select().count() == 0


def test_from_file_good_root(clidb, cli, xfs):
    """Test --file-list with node root appended."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from, root="/NodeFrom")

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")

    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")

    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file3, node=node_from, has_file="Y", wants_file="Y")

    xfs.create_file(
        "/file_list", contents="/NodeFrom/Acq/File1\n# Comment\n/NodeFrom/Acq/File3"
    )

    cli(
        0,
        [
            "group",
            "sync",
            "GroupTo",
            "NodeFrom",
            "--file-list=/file_list",
        ],
        input="Y\n",
    )

    # File1 and File3 are transferred
    assert ArchiveFileCopyRequest.select().count() == 2
    assert ArchiveFileCopyRequest.get(id=1).file == file1
    assert ArchiveFileCopyRequest.get(id=2).file == file3


def test_from_file_bad_root(clidb, cli, xfs):
    """Test --file-list with invalid node root."""

    group_from = StorageGroup.create(name="GroupFrom")
    node_from = StorageNode.create(name="NodeFrom", group=group_from, root="/NodeFrom")

    group_to = StorageGroup.create(name="GroupTo")
    StorageNode.create(name="NodeTo", group=group_to)

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file1, node=node_from, has_file="Y", wants_file="Y")

    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file2, node=node_from, has_file="Y", wants_file="Y")

    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file3, node=node_from, has_file="Y", wants_file="Y")

    xfs.create_file(
        "/file_list", contents="/OtherRoot/Acq/File1\n# Comment\n/OtherRoot/Acq/File3"
    )

    cli(
        1,
        [
            "group",
            "sync",
            "GroupTo",
            "NodeFrom",
            "--file-list=/file_list",
        ],
    )

    # no files are transferred
    assert ArchiveFileCopyRequest.select().count() == 0
