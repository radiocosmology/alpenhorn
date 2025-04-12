"""Test CLI: alpenhorn acq list"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


def test_no_list(clidb, cli):
    """Test with nothing to list."""

    result = cli(0, ["acq", "list"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_schema_mismatch(clidb, cli, cli_wrong_schema):
    """Test schema mismatch."""

    cli(1, ["acq", "list"])


def test_list(clidb, cli, assert_row_present):
    """Test with no constraints."""

    acq = ArchiveAcq.create(name="Acq1")
    ArchiveFile.create(name="File1", acq=acq)
    ArchiveFile.create(name="File2", acq=acq)
    acq = ArchiveAcq.create(name="Acq2")
    ArchiveFile.create(name="File3", acq=acq)

    # Acq 3 has no files, but should still get listed
    ArchiveAcq.create(name="Acq3")

    result = cli(0, ["acq", "list"])

    # Check table
    assert_row_present(result.output, "Acq1", 2)
    assert_row_present(result.output, "Acq2", 1)
    assert_row_present(result.output, "Acq3", 0)


def test_list_node(clidb, cli, assert_row_present):
    """Test with node constraint."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)

    # Acq1 has two files; File1 is on Node1; File2 is on Node2
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y")
    file = ArchiveFile.create(name="File2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y")

    # Acq2 has one file; File3 is on Node2
    acq = ArchiveAcq.create(name="Acq2")
    file = ArchiveFile.create(name="File3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y")

    # Acq3 has one file; File4 is on Node2
    acq = ArchiveAcq.create(name="Acq3")
    file = ArchiveFile.create(name="File4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y")

    result = cli(0, ["acq", "list", "--node=Node1"])

    # Only Acq1 should be listed, with only one file
    assert_row_present(result.output, "Acq1", 1)
    assert "Acq2" not in result.output
    assert "Acq3" not in result.output


def test_list_no_node(clidb, cli):
    """Test with non-existent node."""

    cli(1, ["acq", "list", "--node=Missing"])


def test_no_list_node(clidb, cli):
    """Test no list with node constraint."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    StorageNode.create(name="Node2", group=group)

    # All files are on Node1
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y")
    file = ArchiveFile.create(name="File2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y")
    acq = ArchiveAcq.create(name="Acq2")
    file = ArchiveFile.create(name="File3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y")
    acq = ArchiveAcq.create(name="Acq3")
    file = ArchiveFile.create(name="File4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y")

    result = cli(0, ["acq", "list", "--node=Node2"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output
    assert "Acq1" not in result.output
    assert "Acq2" not in result.output
    assert "Acq3" not in result.output


def test_list_group(clidb, cli, assert_row_present):
    """Test with group constraint."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)
    node2 = StorageNode.create(name="Node2", group=group1)
    group2 = StorageGroup.create(name="Group2")
    node3 = StorageNode.create(name="Node3", group=group2)

    # Acq1 has two files, both in Group1
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y")
    file = ArchiveFile.create(name="File2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y")

    # Acq2 has two file; File3 is in Group1, File4 is in Group2
    acq = ArchiveAcq.create(name="Acq2")
    file = ArchiveFile.create(name="File3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y")
    file = ArchiveFile.create(name="File4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y")

    # Acq3 has one file; File5 is in Group 2
    acq = ArchiveAcq.create(name="Acq3")
    file = ArchiveFile.create(name="File5", acq=acq)
    ArchiveFileCopy.create(file=file, node=node3, has_file="Y")

    result = cli(0, ["acq", "list", "--group=Group1"])

    # Check results.  Only files in the group should be counted
    assert_row_present(result.output, "Acq1", 2)
    assert_row_present(result.output, "Acq2", 1)
    assert "Acq3" not in result.output


def test_list_no_group(clidb, cli):
    """Test with non-existent group."""

    cli(1, ["acq", "list", "--group=Missing"])


def test_no_list_group(clidb, cli):
    """Test no list with group constraint."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)
    node2 = StorageNode.create(name="Node2", group=group1)
    group2 = StorageGroup.create(name="Group2")
    StorageNode.create(name="Node3", group=group2)

    # Everything is in Group1
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y")
    file = ArchiveFile.create(name="File2", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y")
    acq = ArchiveAcq.create(name="Acq2")
    file = ArchiveFile.create(name="File3", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y")
    file = ArchiveFile.create(name="File4", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y")
    acq = ArchiveAcq.create(name="Acq3")
    file = ArchiveFile.create(name="File5", acq=acq)
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y")

    result = cli(0, ["acq", "list", "--group=Group2"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list_node_group(clidb, cli):
    """Test with both node and group."""

    cli(2, ["acq", "list", "--node=Node", "--group=Group"])
