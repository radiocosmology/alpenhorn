"""Test CLI: alpenhorn acq files"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


def test_no_acq(clidb, cli):
    """Test with no acq specified."""

    cli(2, ["acq", "files"])


def test_acq_not_found(clidb, cli):
    """Test with non-existent acq."""

    cli(1, ["acq", "files", "Acq"])


def test_no_files(clidb, cli):
    """Test with nothing to list."""

    ArchiveAcq.create(name="Acq")

    result = cli(0, ["acq", "files", "Acq"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list(clidb, cli, assert_row_present):
    """Test with no constraints."""

    acq = ArchiveAcq.create(name="Acq1")
    ArchiveFile.create(
        name="File1", acq=acq, size_b=123, md5sum="0123456789abcdef0123456789abcdef"
    )
    ArchiveFile.create(
        name="File2", acq=acq, size_b=45678, md5sum="fedcba9876543210fedcba9876543210"
    )
    acq = ArchiveAcq.create(name="Acq2")
    ArchiveFile.create(
        name="File3", acq=acq, size_b=9, md5sum="fedcba98765432100123456789abcdef"
    )

    result = cli(0, ["acq", "files", "Acq1"])

    # Check table
    assert_row_present(
        result.output, "File1", "123 B", "0123456789abcdef0123456789abcdef"
    )
    assert_row_present(
        result.output, "File2", "44.61 kiB", "fedcba9876543210fedcba9876543210"
    )
    assert "File3" not in result.output
    assert result.output.count("File") == 2


def test_list_node(clidb, cli, assert_row_present):
    """Test with node constraint."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)

    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="FileYY", acq=acq, size_b=123)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    file = ArchiveFile.create(name="File2", acq=acq, size_b=456)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")
    file = ArchiveFile.create(name="FileMY", acq=acq, size_b=789)
    ArchiveFileCopy.create(
        file=file, node=node1, has_file="M", wants_file="Y", size_b=1234
    )
    file = ArchiveFile.create(name="FileXY", acq=acq, size_b=5678)
    ArchiveFileCopy.create(
        file=file, node=node1, has_file="X", wants_file="Y", size_b=0
    )
    file = ArchiveFile.create(name="FileNY", acq=acq, size_b=0)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="Y")

    file = ArchiveFile.create(name="FileYM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="M")
    file = ArchiveFile.create(name="FileMM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="M", wants_file="M")
    file = ArchiveFile.create(name="FileXM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="X", wants_file="M")
    file = ArchiveFile.create(name="FileNM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="M")

    file = ArchiveFile.create(name="FileYN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="N")
    file = ArchiveFile.create(name="FileMN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="M", wants_file="N")
    file = ArchiveFile.create(name="FileXN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="X", wants_file="N")
    file = ArchiveFile.create(name="FileNN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")

    # Acq2 has one file on Node1
    acq = ArchiveAcq.create(name="Acq2")
    file = ArchiveFile.create(name="File3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N")

    result = cli(0, ["acq", "files", "Acq1", "--node=Node1"])

    assert_row_present(result.output, "FileYY", "123 B", "Healthy", "-")
    assert "File2" not in result.output
    assert "File3" not in result.output
    assert_row_present(result.output, "FileMY", "789 B", "Suspect", "1.205 kiB")
    assert_row_present(result.output, "FileXY", "5.545 kiB", "Corrupt", "0 B")
    assert_row_present(result.output, "FileNY", "0 B", "Missing", "-")
    assert_row_present(result.output, "FileYM", "-", "Removable", "-")
    assert_row_present(result.output, "FileMM", "-", "Suspect", "-")
    assert_row_present(result.output, "FileXM", "-", "Corrupt", "-")
    assert "FileNM" not in result.output
    assert_row_present(result.output, "FileYN", "-", "Released", "-")
    assert_row_present(result.output, "FileMN", "-", "Released", "-")
    assert_row_present(result.output, "FileXN", "-", "Released", "-")
    assert "FileNN" not in result.output
    assert result.output.count("File") == 10


def test_list_node_removed(clidb, cli, assert_row_present):
    """Test --node --show-removed."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)

    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="FileYY", acq=acq, size_b=123)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    file = ArchiveFile.create(name="FileNY", acq=acq, size_b=0)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="Y")

    file = ArchiveFile.create(name="FileYM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="M")
    file = ArchiveFile.create(name="FileNM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="M")

    file = ArchiveFile.create(name="FileYN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="N")
    file = ArchiveFile.create(name="FileNN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")

    result = cli(0, ["acq", "files", "Acq1", "--node=Node1", "--show-removed"])

    assert_row_present(result.output, "FileYY", "123 B", "Healthy", "-")
    assert_row_present(result.output, "FileNY", "0 B", "Missing", "-")
    assert_row_present(result.output, "FileYM", "-", "Removable", "-")
    assert_row_present(result.output, "FileNM", "-", "Removed", "-")
    assert_row_present(result.output, "FileYN", "-", "Released", "-")
    assert_row_present(result.output, "FileNN", "-", "Removed", "-")
    assert result.output.count("File") == 6


def test_list_group_removed(clidb, cli, assert_row_present):
    """Test --group --show-removed."""

    group = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group)

    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="FileYY", acq=acq, size_b=123)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    file = ArchiveFile.create(name="FileNY", acq=acq, size_b=0)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="Y")

    file = ArchiveFile.create(name="FileYM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="M")
    file = ArchiveFile.create(name="FileNM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="M")

    file = ArchiveFile.create(name="FileYN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="N")
    file = ArchiveFile.create(name="FileNN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")

    result = cli(0, ["acq", "files", "Acq1", "--group=Group1", "--show-removed"])

    # --group state ignores wants_file
    assert_row_present(result.output, "FileYY", "123 B", "Present", "Node1")
    assert_row_present(result.output, "FileNY", "0 B", "Removed", "-")
    assert_row_present(result.output, "FileYM", "-", "Present", "Node1")
    assert_row_present(result.output, "FileNM", "-", "Removed", "-")
    assert_row_present(result.output, "FileYN", "-", "Present", "Node1")
    assert_row_present(result.output, "FileNN", "-", "Removed", "-")
    assert result.output.count("File") == 6


def test_list_no_node(clidb, cli):
    """Test with non-existent node."""

    ArchiveAcq.create(name="Acq1")

    cli(1, ["acq", "files", "Acq1", "--node=Missing"])


def test_no_list_node(clidb, cli):
    """Test no list with node constraint."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)

    # On Node2, not Node1
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = cli(0, ["acq", "files", "Acq1", "--node=Node1"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list_node_only_removed(clidb, cli, assert_row_present):
    """Test list --node with only removed files."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)

    # On Node2, not Node1
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = cli(0, ["acq", "files", "Acq1", "--node=Node1", "--show-removed"])

    assert_row_present(result.output, "File1", "123 B", "Removed", "-")


def test_list_no_group(clidb, cli):
    """Test with non-existent group."""

    ArchiveAcq.create(name="Acq1")

    cli(1, ["acq", "files", "Acq1", "--group=Missing"])


def test_no_list_group(clidb, cli):
    """Test no list with group constraint."""

    group = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group)
    group = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node3", group=group)

    # Everything is in Group2
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = cli(0, ["acq", "files", "Acq1", "--group=Group1"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list_group_only_removed(clidb, cli, assert_row_present):
    """Test no list with group constraint."""

    group = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group)
    group = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node3", group=group)

    # Everything is in Group2
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = cli(0, ["acq", "files", "Acq1", "--group=Group1", "--show-removed"])

    assert_row_present(result.output, "File1", "-", "Removed", "-")


def test_list_node_group(clidb, cli):
    """Test with both node and group."""

    ArchiveAcq.create(name="Acq1")
    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["acq", "files", "Acq1", "--node=Node", "--group=Group"])
