"""Test CLI: alpenhorn file find"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


def test_bad_acq(clidb, cli):
    """Test a non-existent --acq"""

    cli(1, ["file", "find", "--acq=MISSING"])


def test_bad_node(clidb, cli):
    """Test a non-existent --node"""

    cli(1, ["file", "find", "--node=MISSING"])


def test_bad_group(clidb, cli):
    """Test a non-existent --group"""

    cli(1, ["file", "find", "--group=MISSING"])


def test_everything(clidb, cli, assert_row_present):
    """Test listing everything."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)

    acq = ArchiveAcq.create(name="acq1")
    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="X", wants_file="Y")
    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="M", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="N", wants_file="Y")
    acq = ArchiveAcq.create(name="acq2")
    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="M")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = cli(0, ["file", "find"])

    assert_row_present(result.output, "acq1/file1", "Node1", "Healthy")
    assert_row_present(result.output, "acq2/file3", "Node1", "Removable")
    assert_row_present(result.output, "acq2/file3", "Node2", "Healthy")
    assert result.output.count("acq") == 3


def test_acq(clidb, cli, assert_row_present):
    """Test limiting by --acq."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="acq1")
    file = ArchiveFile.create(name="file", acq=acq)
    ArchiveFileCopy.create(file=file, node=node, has_file="Y", wants_file="Y")
    acq = ArchiveAcq.create(name="acq2")
    file = ArchiveFile.create(name="file", acq=acq)
    ArchiveFileCopy.create(file=file, node=node, has_file="Y", wants_file="Y")

    result = cli(0, ["file", "find", "--acq=acq1"])

    assert_row_present(result.output, "acq1/file", "Node", "Healthy")
    assert result.output.count("acq") == 1


def test_node(clidb, cli, assert_row_present):
    """Test limiting by --node."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    node3 = StorageNode.create(name="Node3", group=group)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="N", wants_file="N")

    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="X", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    result = cli(0, ["file", "find", "--node=Node1", "--node=Node2"])

    assert_row_present(result.output, "acq/file1", "Node1", "Healthy")
    assert_row_present(result.output, "acq/file1", "Node2", "Healthy")
    assert_row_present(result.output, "acq/file2", "Node1", "Healthy")
    assert_row_present(result.output, "acq/file3", "Node2", "Healthy")
    assert result.output.count("acq") == 4


def test_group(clidb, cli, assert_row_present):
    """Test liming by --group."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    group3 = StorageGroup.create(name="Group3")
    node3 = StorageNode.create(name="Node3", group=group3)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="N", wants_file="N")

    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="X", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    result = cli(0, ["file", "find", "--group=Group1", "--group=Group2"])

    assert_row_present(result.output, "acq/file1", "Node1", "Healthy")
    assert_row_present(result.output, "acq/file1", "Node2", "Healthy")
    assert_row_present(result.output, "acq/file2", "Node1", "Healthy")
    assert_row_present(result.output, "acq/file3", "Node2", "Healthy")
    assert result.output.count("acq") == 4


def test_node_group(clidb, cli, assert_row_present):
    """Test --node and --group together."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    group3 = StorageGroup.create(name="Group3")
    node3 = StorageNode.create(name="Node3", group=group3)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="N", wants_file="N")

    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="X", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    result = cli(0, ["file", "find", "--group=Group1", "--node=Node1", "--node=Node2"])

    assert_row_present(result.output, "acq/file1", "Node1", "Healthy")
    assert_row_present(result.output, "acq/file1", "Node2", "Healthy")
    assert_row_present(result.output, "acq/file2", "Node1", "Healthy")
    assert_row_present(result.output, "acq/file3", "Node2", "Healthy")
    assert result.output.count("acq") == 4


def test_state(clidb, cli, assert_row_present):
    """Test the state flags."""

    acq = ArchiveAcq.create(name="acq")
    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    for has in ["Y", "M", "X", "N"]:
        for wants in ["Y", "M", "N"]:
            file = ArchiveFile.create(name="File" + has + wants, acq=acq)
            ArchiveFileCopy.create(file=file, node=node, has_file=has, wants_file=wants)

    # The rows we want to find for each state
    state_rows = {
        "corrupt": [
            ("acq/FileXY", "Node", "Corrupt"),
            ("acq/FileXM", "Node", "Corrupt"),
        ],
        "healthy": [
            ("acq/FileYY", "Node", "Healthy"),
            ("acq/FileYM", "Node", "Removable"),
        ],
        "suspect": [
            ("acq/FileMY", "Node", "Suspect"),
            ("acq/FileMM", "Node", "Suspect"),
        ],
        "missing": [("acq/FileNY", "Node", "Missing")],
    }

    # Run through all the state combinations.  There are 2**4 of these:
    for num in range(16):
        corrupt = num & 1 == 0
        healthy = num & 2 == 0
        suspect = num & 4 == 0
        missing = num & 8 == 0

        # Build command and resultant file list
        command = ["file", "find"]
        rows = []
        if corrupt:
            command.append("--corrupt")
            rows += state_rows["corrupt"]
        if suspect:
            command.append("--suspect")
            rows += state_rows["suspect"]
        if missing:
            command.append("--missing")
            rows += state_rows["missing"]
        if healthy:
            command.append("--healthy")
            rows += state_rows["healthy"]
        # The default when no state is chosen
        if not rows:
            rows = state_rows["healthy"]

        # Execute
        result = cli(0, command)

        # Check files
        for row in rows:
            assert_row_present(result.output, *row)
        # Count rows to make sure there aren't extras
        assert result.output.count("acq") == len(rows)
