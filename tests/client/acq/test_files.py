"""Test CLI: alpenhorn acq files"""

import re

from alpenhorn.db import (
    StorageGroup,
    StorageNode,
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
)


def test_no_acq(clidb, client):
    """Test with no acq specified."""

    client(2, ["acq", "files"])


def test_acq_not_found(clidb, client):
    """Test with non-existent acq."""

    client(1, ["acq", "files", "Acq"])


def test_no_files(clidb, client):
    """Test with nothing to list."""

    ArchiveAcq.create(name="Acq")

    result = client(0, ["acq", "files", "Acq"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list(clidb, client):
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

    result = client(0, ["acq", "files", "Acq1"])

    # Check table
    assert (
        re.search(r"File1\s+123 B\s+0123456789abcdef0123456789abcdef", result.output)
        is not None
    )
    assert (
        re.search(
            r"File2\s+44\.61 kiB\s+fedcba9876543210fedcba9876543210", result.output
        )
        is not None
    )
    assert "File3" not in result.output
    assert result.output.count("File") == 2


def test_list_node(clidb, client):
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

    result = client(0, ["acq", "files", "Acq1", "--node=Node1"])

    assert re.search(r"FileYY\s+123 B\s+Present\s+-", result.output) is not None
    assert "File2" not in result.output
    assert "File3" not in result.output
    assert (
        re.search(r"FileMY\s+789 B\s+Needs Check\s+1\.205 kiB", result.output)
        is not None
    )
    assert re.search(r"FileXY\s+5.545 kiB\s+Corrupt\s+-", result.output) is not None
    assert re.search(r"FileNY\s+0 B\s+Missing", result.output) is not None
    assert re.search(r"FileYM\s+-\s+Removable", result.output) is not None
    assert re.search(r"FileMM\s+-\s+Removable", result.output) is not None
    assert re.search(r"FileXM\s+-\s+Removable", result.output) is not None
    assert "FileNM" not in result.output
    assert re.search(r"FileYN\s+-\s+Pending Removal", result.output) is not None
    assert re.search(r"FileMN\s+-\s+Pending Removal", result.output) is not None
    assert re.search(r"FileXN\s+-\s+Pending Removal", result.output) is not None
    assert "FileNN" not in result.output
    assert result.output.count("File") == 10


def test_list_node_removed(clidb, client):
    """Test --node --show-removed."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)

    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="FileYY", acq=acq, size_b=123)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    file = ArchiveFile.create(name="FileNY", acq=acq, size_b=0)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="Y")

    file = ArchiveFile.create(name="FileYM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="X", wants_file="M")
    file = ArchiveFile.create(name="FileNM", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="M")

    file = ArchiveFile.create(name="FileYN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="X", wants_file="N")
    file = ArchiveFile.create(name="FileNN", acq=acq)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")

    result = client(0, ["acq", "files", "Acq1", "--node=Node1", "--show-removed"])

    assert re.search(r"FileYY\s+123 B\s+Present\s+-", result.output) is not None
    assert re.search(r"FileNY\s+0 B\s+Missing", result.output) is not None
    assert re.search(r"FileYM\s+-\s+Removable", result.output) is not None
    assert re.search(r"FileNM\s+-\s+Removed", result.output) is not None
    assert re.search(r"FileYN\s+-\s+Pending Removal", result.output) is not None
    assert re.search(r"FileNN\s+-\s+Removed", result.output) is not None
    assert result.output.count("File") == 6


def test_list_group_removed(clidb, client):
    """Test --group --show-removed."""

    group = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)

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

    result = client(0, ["acq", "files", "Acq1", "--group=Group1", "--show-removed"])

    assert re.search(r"FileYY\s+123 B\s+Present", result.output) is not None
    assert re.search(r"FileNY\s+0 B\s+Removed", result.output) is not None
    assert re.search(r"FileYM\s+-\s+Present", result.output) is not None
    assert re.search(r"FileNM\s+-\s+Removed", result.output) is not None
    assert re.search(r"FileYN\s+-\s+Present", result.output) is not None
    assert re.search(r"FileNN\s+-\s+Removed", result.output) is not None
    assert result.output.count("File") == 6


def test_list_no_node(clidb, client):
    """Test with non-existent node."""

    ArchiveAcq.create(name="Acq1")

    client(1, ["acq", "files", "Acq1", "--node=Missing"])


def test_no_list_node(clidb, client):
    """Test no list with node constraint."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)

    # On Node2, not Node1
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = client(0, ["acq", "files", "Acq1", "--node=Node1"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list_node_only_removed(clidb, client):
    """Test list --node with only removed files."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    acq = ArchiveAcq.create(name="Acq1")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)

    # On Node2, not Node1
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="N")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = client(0, ["acq", "files", "Acq1", "--node=Node1", "--show-removed"])

    assert "File1" in result.output


def test_list_no_group(clidb, client):
    """Test with non-existent group."""

    ArchiveAcq.create(name="Acq1")

    client(1, ["acq", "files", "Acq1", "--group=Missing"])


def test_no_list_group(clidb, client):
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

    result = client(0, ["acq", "files", "Acq1", "--group=Group1"])

    # i.e. the header hasn't been printed
    assert "Name" not in result.output


def test_list_group_only_removed(clidb, client):
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

    result = client(0, ["acq", "files", "Acq1", "--group=Group1", "--show-removed"])

    # i.e. the header hasn't been printed
    assert "File1" in result.output


def test_list_node_group(clidb, client):
    """Test with both node and group."""

    ArchiveAcq.create(name="Acq1")

    client(2, ["acq", "files", "Acq1", "--node=Node", "--group=Group"])
