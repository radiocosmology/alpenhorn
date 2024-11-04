"""Test CLI: alpenhorn acq list"""

import re

from alpenhorn.db import (
    StorageGroup,
    StorageNode,
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
)


def test_no_list(clidb, client):
    """Test with nothing to list."""

    result = client(0, ["acq", "list"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list(clidb, client):
    """Test with no constraints."""

    acq = ArchiveAcq.create(name="Acq1")
    ArchiveFile.create(name="File1", acq=acq)
    ArchiveFile.create(name="File2", acq=acq)
    acq = ArchiveAcq.create(name="Acq2")
    ArchiveFile.create(name="File3", acq=acq)

    # Acq 3 has no files, but should still get listed
    ArchiveAcq.create(name="Acq3")

    result = client(0, ["acq", "list"])

    # Check table
    assert re.search(r"Acq1\s+2", result.output) is not None
    assert re.search(r"Acq2\s+1", result.output) is not None
    assert re.search(r"Acq3\s+0", result.output) is not None


def test_list_node(clidb, client):
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

    result = client(0, ["acq", "list", "--node=Node1"])

    # Only Acq1 should be listed, with only one file
    assert re.search(r"Acq1\s+1", result.output) is not None
    assert "Acq2" not in result.output
    assert "Acq3" not in result.output


def test_list_no_node(clidb, client):
    """Test with non-existent node."""

    client(1, ["acq", "list", "--node=Missing"])


def test_no_list_node(clidb, client):
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

    result = client(0, ["acq", "list", "--node=Node2"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list_group(clidb, client):
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

    result = client(0, ["acq", "list", "--group=Group1"])

    # Check results.  Only files in the group should be counted
    assert re.search(r"Acq1\s+2", result.output) is not None
    assert re.search(r"Acq2\s+1", result.output) is not None
    assert "Acq3" not in result.output


def test_list_no_group(clidb, client):
    """Test with non-existent group."""

    client(1, ["acq", "list", "--group=Missing"])


def test_no_list_group(clidb, client):
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

    result = client(0, ["acq", "list", "--group=Group2"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list_node_group(clidb, client):
    """Test with both node and group."""

    client(2, ["acq", "list", "--node=Node", "--group=Group"])
