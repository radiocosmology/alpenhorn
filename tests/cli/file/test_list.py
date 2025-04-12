"""Test CLI: alpenhorn file show"""

from datetime import datetime

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


def test_schema_mismatch(clidb, cli, cli_wrong_schema):
    """Test schema mismatch."""

    cli(1, ["file", "list"])


def test_only_from_to(clidb, cli):
    """Can't only use --from or --to"""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["file", "list", "--from=Node"])
    cli(2, ["file", "list", "--to=Group"])


def test_state_to_from(clidb, cli):
    """state flags can't be used with --to or --from."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    for state_flag in ["--corrupt", "--missing", "--healthy", "--suspect"]:
        cli(2, ["file", "list", state_flag, "--to=Group", "--from=Node"])


def test_bare_all(clidb, cli):
    """--all must accompany a location constraint"""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["file", "list", "--all"])

    # But any of these are fine

    cli(0, ["file", "list", "--all", "--absent-node=Node"])
    cli(0, ["file", "list", "--all", "--absent-group=Group"])
    cli(0, ["file", "list", "--all", "--node=Node"])
    cli(0, ["file", "list", "--all", "--group=Group"])


def test_bad_acq(clidb, cli):
    """Test a non-existent --acq"""

    cli(1, ["file", "list", "--acq=MISSING"])


def test_bad_node(clidb, cli):
    """Test a non-existent --node"""

    cli(1, ["file", "list", "--node=MISSING"])


def test_bad_group(clidb, cli):
    """Test a non-existent --group"""

    cli(1, ["file", "list", "--group=MISSING"])


def test_bad_from(clidb, cli):
    """Test a non-existent --from target"""

    StorageGroup.create(name="Group")

    cli(1, ["file", "list", "--to=Group", "--from=MISSING"])


def test_bad_to(clidb, cli):
    """Test a non-existent --to target"""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["file", "list", "--to=MISSING", "--from=Node"])


def test_list(clidb, cli):
    """Test listing everything."""

    acq = ArchiveAcq.create(name="acq1")
    ArchiveFile.create(name="file1", acq=acq)
    ArchiveFile.create(name="file2", acq=acq)
    acq = ArchiveAcq.create(name="acq2")
    ArchiveFile.create(name="file3", acq=acq)
    ArchiveFile.create(name="file4", acq=acq)

    result = cli(0, ["file", "list"])

    assert "acq1/file1" in result.output
    assert "acq1/file2" in result.output
    assert "acq2/file3" in result.output
    assert "acq2/file4" in result.output


def test_list_acq(clidb, cli):
    """Test limiting by --acq."""

    acq = ArchiveAcq.create(name="acq1")
    ArchiveFile.create(name="file1", acq=acq)
    ArchiveFile.create(name="file2", acq=acq)
    acq = ArchiveAcq.create(name="acq2")
    ArchiveFile.create(name="file3", acq=acq)
    ArchiveFile.create(name="file4", acq=acq)

    result = cli(0, ["file", "list", "--acq=acq1"])

    assert "acq1/file1" in result.output
    assert "acq1/file2" in result.output
    assert "acq2" not in result.output


def test_list_details(clidb, cli, assert_row_present):
    """Test listing details without node/group."""

    acq = ArchiveAcq.create(name="acq1")
    ArchiveFile.create(
        name="file1",
        acq=acq,
        size_b=3456,
        md5sum="d41d8cd98f00b204e9800998ecf8427e",
        registered=datetime(2001, 1, 1, 1, 1, 1, 1),
    )
    ArchiveFile.create(
        name="file2",
        acq=acq,
        size_b=4567,
        md5sum="68b329da9893e34099c7d8ad5cb9c940",
        registered=datetime(2002, 3, 2, 2, 2, 2, 2),
    )
    acq = ArchiveAcq.create(name="acq2")
    ArchiveFile.create(
        name="file3",
        acq=acq,
        size_b=5678,
        md5sum="7309AF63395717D5B9F8AA6619301937",
        registered=datetime(2003, 3, 3, 3, 3, 3, 3),
    )
    ArchiveFile.create(name="file4", acq=acq, registered=0)

    result = cli(0, ["file", "list", "--details"])

    assert_row_present(
        result.output,
        "acq1/file1",
        "3.375 kiB",
        "d41d8cd98f00b204e9800998ecf8427e",
        "Mon Jan  1 01:01:01 2001 UTC",
    )
    assert_row_present(
        result.output,
        "acq1/file2",
        "4.460 kiB",
        "68b329da9893e34099c7d8ad5cb9c940",
        "Sat Mar  2 02:02:02 2002 UTC",
    )
    assert_row_present(
        result.output,
        "acq2/file3",
        "5.545 kiB",
        "7309af63395717d5b9f8aa6619301937",
        "Mon Mar  3 03:03:03 2003 UTC",
    )
    assert_row_present(result.output, "acq2/file4", "-", "-", "-")


def test_list_node(clidb, cli):
    """Test --node."""

    group = StorageGroup.create(name="Group")
    acq = ArchiveAcq.create(name="acq")

    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    node3 = StorageNode.create(name="Node3", group=group)

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

    result = cli(0, ["file", "list", "--node=Node1", "--node=Node2"])

    assert "acq/file1" in result.output
    assert "acq/file2" in result.output
    assert "acq/file3" in result.output
    assert "acq/file4" not in result.output


def test_list_group(clidb, cli):
    """Test --group."""

    acq = ArchiveAcq.create(name="acq")

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    group3 = StorageGroup.create(name="Group3")
    node3 = StorageNode.create(name="Node3", group=group3)

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

    result = cli(0, ["file", "list", "--group=Group1", "--group=Group2"])

    assert "acq/file1" in result.output
    assert "acq/file2" in result.output
    assert "acq/file3" in result.output
    assert "acq/file4" not in result.output


def test_list_node_group(clidb, cli):
    """Test --node and --group together."""

    acq = ArchiveAcq.create(name="acq")

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    group3 = StorageGroup.create(name="Group3")
    node3 = StorageNode.create(name="Node3", group=group3)

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

    result = cli(0, ["file", "list", "--group=Group1", "--node=Node1", "--node=Node2"])

    assert "acq/file1" in result.output
    assert "acq/file2" in result.output
    assert "acq/file3" in result.output
    assert "acq/file4" not in result.output

    # i.e. no double listing
    assert result.output.count("acq") == 3


def test_list_all(clidb, cli):
    """Test --all."""

    group = StorageGroup.create(name="Group")
    acq = ArchiveAcq.create(name="acq")

    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    node3 = StorageNode.create(name="Node3", group=group)

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

    result = cli(0, ["file", "list", "--node=Node1", "--node=Node2", "--all"])

    assert "acq/file1" in result.output
    assert "acq/file2" not in result.output
    assert "acq/file3" not in result.output
    assert "acq/file4" not in result.output


def test_state(clidb, cli):
    """Test the state flags."""

    acq = ArchiveAcq.create(name="acq")
    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    for has in ["Y", "M", "X", "N"]:
        for wants in ["Y", "M", "N"]:
            file = ArchiveFile.create(name="File" + has + wants, acq=acq)
            ArchiveFileCopy.create(file=file, node=node, has_file=has, wants_file=wants)

    # The files we want to find for each state
    state_files = {
        "corrupt": ["FileXY", "FileXM"],
        "healthy": ["FileYY", "FileYM"],
        "suspect": ["FileMY", "FileMM"],
        "missing": ["FileNY"],
    }

    # Run through all the state combinations.  There are 2**4 of these:
    for num in range(16):
        corrupt = num & 1 == 0
        healthy = num & 2 == 0
        suspect = num & 4 == 0
        missing = num & 8 == 0

        # Build command and resultant file list
        command = ["file", "list", "--node=Node"]
        files = []
        if corrupt:
            command.append("--corrupt")
            files += state_files["corrupt"]
        if suspect:
            command.append("--suspect")
            files += state_files["suspect"]
        if missing:
            command.append("--missing")
            files += state_files["missing"]
        if healthy:
            command.append("--healthy")
            files += state_files["healthy"]
        # The default when no state is chosen
        if not files:
            files = state_files["healthy"]

        # Execute
        result = cli(0, command)

        # Check files
        for file in files:
            assert "acq/" + file in result.output
        # Count files listed to make sure there aren't extras
        assert result.output.count("acq") == len(files)


def test_sync(clidb, cli):
    """Test --from --to."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="M")
    ArchiveFileCopy.create(file=file, node=node2, has_file="N", wants_file="Y")

    result = cli(0, ["file", "list", "--from=Node1", "--to=Group2"])

    assert "file1" in result.output
    assert "file2" not in result.output
    assert "file3" not in result.output
    assert "file4" in result.output


def test_sync_select(clidb, cli):
    """Test --from --to with selection"""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    group3 = StorageGroup.create(name="Group3")
    node3 = StorageNode.create(name="Node3", group=group3)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="M")
    ArchiveFileCopy.create(file=file, node=node2, has_file="N", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    result = cli(0, ["file", "list", "--from=Node1", "--to=Group2", "--node=Node3"])

    assert "file1" not in result.output
    assert "file2" not in result.output
    assert "file3" not in result.output
    assert "file4" in result.output

    result = cli(
        0, ["file", "list", "--from=Node1", "--to=Group2", "--absent-node=Node3"]
    )

    assert "file1" in result.output
    assert "file2" not in result.output
    assert "file3" not in result.output
    assert "file4" not in result.output


def test_detail_node(clidb, cli, assert_row_present):
    """Test --details with extra details"""

    group = StorageGroup.create(name="group")
    node = StorageNode.create(name="node", group=group)

    acq = ArchiveAcq.create(name="acq")

    file = ArchiveFile.create(
        name="file1",
        acq=acq,
        size_b=1234,
        md5sum="d41d8cd98f00b204e9800998ecf8427e",
        registered=0,
    )
    ArchiveFileCopy.create(
        file=file, node=node, size_b=2345, has_file="Y", wants_file="M"
    )

    file = ArchiveFile.create(
        name="file2",
        acq=acq,
        size_b=3456,
        md5sum="68b329da9893e34099c7d8ad5cb9c940",
        registered=0,
    )
    ArchiveFileCopy.create(
        file=file, node=node, size_b=4567, has_file="M", wants_file="Y"
    )

    file = ArchiveFile.create(
        name="file3",
        acq=acq,
        size_b=5678,
        md5sum="7309AF63395717D5B9F8AA6619301937",
        registered=0,
    )
    ArchiveFileCopy.create(
        file=file, node=node, size_b=6789, has_file="N", wants_file="Y"
    )

    result = cli(
        0,
        [
            "file",
            "list",
            "--node=node",
            "--healthy",
            "--missing",
            "--suspect",
            "--details",
        ],
    )

    assert_row_present(
        result.output,
        "acq/file1",
        "1.205 kiB",
        "d41d8cd98f00b204e9800998ecf8427e",
        "-",
        "Removable",
        "2.290 kiB",
    )
    assert_row_present(
        result.output,
        "acq/file2",
        "3.375 kiB",
        "68b329da9893e34099c7d8ad5cb9c940",
        "-",
        "Suspect",
        "4.460 kiB",
    )
    assert_row_present(
        result.output,
        "acq/file3",
        "5.545 kiB",
        "7309af63395717d5b9f8aa6619301937",
        "-",
        "Missing",
        "6.630 kiB",
    )

    result = cli(
        0,
        [
            "file",
            "list",
            "--group=group",
            "--healthy",
            "--missing",
            "--suspect",
            "--details",
        ],
    )

    assert_row_present(
        result.output,
        "acq/file1",
        "1.205 kiB",
        "d41d8cd98f00b204e9800998ecf8427e",
        "-",
        "Present",
        "node",
    )
    assert_row_present(
        result.output,
        "acq/file2",
        "3.375 kiB",
        "68b329da9893e34099c7d8ad5cb9c940",
        "-",
        "Suspect",
        "node",
    )
    assert_row_present(
        result.output,
        "acq/file3",
        "5.545 kiB",
        "7309af63395717d5b9f8aa6619301937",
        "-",
        "Absent",
        "-",
    )


def test_absent(clidb, cli):
    """Test --absent-node and --absent-group."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)
    node2 = StorageNode.create(name="Node2", group=group1)
    group2 = StorageGroup.create(name="Group2")
    node3 = StorageNode.create(name="Node3", group=group2)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="file12", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file13", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file23", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    result = cli(0, ["file", "list", "--absent-node=Node1"])

    assert "acq/file2" in result.output
    assert "acq/file23" in result.output
    assert "acq/file3" in result.output
    assert result.output.count("acq") == 3

    result = cli(0, ["file", "list", "--absent-node=Node1", "--absent-node=Node2"])

    assert "acq/file13" in result.output
    assert "acq/file23" in result.output
    assert "acq/file1" in result.output
    assert "acq/file2" in result.output
    assert "acq/file3" in result.output
    assert result.output.count("acq") == 5

    result = cli(0, ["file", "list", "--absent-group=Group1"])

    assert "acq/file3" in result.output
    assert result.output.count("acq") == 1

    result = cli(0, ["file", "list", "--absent-node=Node1", "--absent-group=Group2"])

    assert "acq/file12" in result.output
    assert "acq/file23" in result.output
    assert "acq/file1" in result.output
    assert "acq/file2" in result.output
    assert "acq/file3" in result.output
    assert result.output.count("acq") == 5


def test_positive_negative(clidb, cli):
    """Test --node and --absent-node together."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)
    node2 = StorageNode.create(name="Node2", group=group1)
    group2 = StorageGroup.create(name="Group2")
    node3 = StorageNode.create(name="Node3", group=group2)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="file12", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file13", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file23", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    result = cli(0, ["file", "list", "--node=Node1", "--absent-node=Node2"])

    assert "acq/file1" in result.output
    assert "acq/file12" in result.output
    assert "acq/file13" in result.output
    assert "acq/file3" in result.output
    assert result.output.count("acq") == 4

    result = cli(0, ["file", "list", "--node=Node1", "--absent-node=Node2", "--all"])

    assert "acq/file1" in result.output
    assert "acq/file13" in result.output
    assert result.output.count("acq") == 2


def test_all_absent(clidb, cli):
    """Test --absent-node and --absent-group with --all."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)
    node2 = StorageNode.create(name="Node2", group=group1)
    group2 = StorageGroup.create(name="Group2")
    node3 = StorageNode.create(name="Node3", group=group2)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="file12", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file13", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file23", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y", wants_file="Y")

    result = cli(
        0, ["file", "list", "--absent-node=Node1", "--absent-node=Node2", "--all"]
    )

    assert "acq/file3" in result.output
    assert result.output.count("acq") == 1

    result = cli(
        0, ["file", "list", "--absent-node=Node1", "--absent-group=Group2", "--all"]
    )

    assert "acq/file2" in result.output
    assert result.output.count("acq") == 1


def test_state_absent(clidb, cli):
    """State flags don't apply to negative location constraints"""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="file1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="X", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="file3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="X", wants_file="Y")

    file = ArchiveFile.create(name="file4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="X", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="X", wants_file="Y")

    result = cli(
        0, ["file", "list", "--corrupt", "--node=Node1", "--absent-node=Node2"]
    )

    assert "acq/file2" in result.output
    assert "acq/file3" in result.output
    assert "acq/file4" in result.output
    assert result.output.count("acq") == 3

    result = cli(
        0, ["file", "list", "--corrupt", "--node=Node1", "--absent-node=Node2", "--all"]
    )

    assert "acq/file4" in result.output
    assert result.output.count("acq") == 1
