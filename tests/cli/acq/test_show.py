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

    cli(2, ["acq", "show"])


def test_acq_not_found(clidb, cli):
    """Test with non-existent acq."""

    cli(1, ["acq", "show", "Acq"])


def test_simple_show(clidb, cli):
    """Test show with no options."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File1", acq=acq, size_b=123)
    ArchiveFile.create(name="File2", acq=acq, size_b=456)

    result = cli(0, ["acq", "show", "Acq"])

    assert "Acq" in result.output
    assert ": 2" in result.output
    assert "579 B" in result.output


def test_show_nodes(clidb, cli, assert_row_present):
    """Test show with --show-nodes."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    StorageNode.create(name="Node3", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)
    ArchiveFileCopy.create(node=node1, file=file, has_file="Y")
    ArchiveFileCopy.create(node=node2, file=file, has_file="N")
    file = ArchiveFile.create(name="File2", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node1, file=file, has_file="Y")
    file = ArchiveFile.create(name="File3", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node2, file=file, has_file="Y")

    result = cli(0, ["acq", "show", "Acq", "--show-nodes"])

    assert_row_present(result.output, "Node1", 2, "579 B")
    assert_row_present(result.output, "Node2", 1, "456 B")
    assert "Node3" not in result.output


def test_show_groups(clidb, cli, assert_row_present):
    """Test show with --show-groups."""

    group = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    group = StorageGroup.create(name="Group2")
    node3 = StorageNode.create(name="Node3", group=group)
    group = StorageGroup.create(name="Group3")
    StorageNode.create(name="Node4", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)
    ArchiveFileCopy.create(node=node1, file=file, has_file="Y")
    ArchiveFileCopy.create(node=node2, file=file, has_file="N")
    file = ArchiveFile.create(name="File2", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node1, file=file, has_file="Y")
    file = ArchiveFile.create(name="File3", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node2, file=file, has_file="Y")
    file = ArchiveFile.create(name="File4", acq=acq, size_b=789)
    ArchiveFileCopy.create(node=node3, file=file, has_file="Y")

    result = cli(0, ["acq", "show", "Acq", "--show-groups"])

    assert_row_present(result.output, "Group1", 3, "1.011 kiB")
    assert_row_present(result.output, "Group2", 1, "789 B")
    assert "Group3" not in result.output


def test_show_groups_nodes(clidb, cli, assert_row_present):
    """Test show with --show-groups and --show-nodes."""

    group = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    group = StorageGroup.create(name="Group2")
    node3 = StorageNode.create(name="Node3", group=group)
    StorageNode.create(name="Node4", group=group)
    group = StorageGroup.create(name="Group3")
    StorageNode.create(name="Node5", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)
    ArchiveFileCopy.create(node=node1, file=file, has_file="Y")
    ArchiveFileCopy.create(node=node2, file=file, has_file="N")
    file = ArchiveFile.create(name="File2", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node1, file=file, has_file="Y")
    file = ArchiveFile.create(name="File3", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node2, file=file, has_file="Y")
    file = ArchiveFile.create(name="File4", acq=acq, size_b=789)
    ArchiveFileCopy.create(node=node3, file=file, has_file="Y")

    result = cli(0, ["acq", "show", "Acq", "--show-groups", "--show-nodes"])

    assert_row_present(result.output, "Group1", 3, "1.011 kiB")
    assert_row_present(result.output, "-- Node1", 2, "579 B")
    assert_row_present(result.output, "-- Node2", 1, "456 B")
    assert_row_present(result.output, "Group2", 1, "789 B")
    assert_row_present(result.output, "-- Node3", 1, "789 B")
    assert "Node4" not in result.output
    assert "Group3" not in result.output
    assert "Node5" not in result.output


def test_show_no_nodes(clidb, cli):
    """Test --show-nodes, with no nodes to show."""

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    StorageNode.create(name="Node3", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)
    ArchiveFileCopy.create(node=node1, file=file, has_file="M")
    ArchiveFileCopy.create(node=node2, file=file, has_file="N")
    file = ArchiveFile.create(name="File2", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node1, file=file, has_file="X")
    file = ArchiveFile.create(name="File3", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node2, file=file, has_file="N")

    result = cli(0, ["acq", "show", "Acq", "--show-nodes"])

    # No table printed
    assert result.output.count("Size") == 1


def test_show_no_groups(clidb, cli):
    """Test show with --show-groups with no groups to show."""

    group = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    group = StorageGroup.create(name="Group2")
    node3 = StorageNode.create(name="Node3", group=group)
    group = StorageGroup.create(name="Group3")
    StorageNode.create(name="Node4", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)
    ArchiveFileCopy.create(node=node1, file=file, has_file="X")
    ArchiveFileCopy.create(node=node2, file=file, has_file="N")
    file = ArchiveFile.create(name="File2", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node1, file=file, has_file="M")
    file = ArchiveFile.create(name="File3", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node2, file=file, has_file="N")
    file = ArchiveFile.create(name="File4", acq=acq, size_b=789)
    ArchiveFileCopy.create(node=node3, file=file, has_file="N")

    result = cli(0, ["acq", "show", "Acq", "--show-groups"])

    # No table printed
    assert result.output.count("Size") == 1


def test_show_no_groups_nodes(clidb, cli):
    """Test show with --show-groups and --show-nodes with no groups to show."""

    group = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    group = StorageGroup.create(name="Group2")
    node3 = StorageNode.create(name="Node3", group=group)
    StorageNode.create(name="Node4", group=group)
    group = StorageGroup.create(name="Group3")
    StorageNode.create(name="Node5", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=123)
    ArchiveFileCopy.create(node=node1, file=file, has_file="N")
    ArchiveFileCopy.create(node=node2, file=file, has_file="N")
    file = ArchiveFile.create(name="File2", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node1, file=file, has_file="X")
    file = ArchiveFile.create(name="File3", acq=acq, size_b=456)
    ArchiveFileCopy.create(node=node2, file=file, has_file="M")
    file = ArchiveFile.create(name="File4", acq=acq, size_b=789)
    ArchiveFileCopy.create(node=node3, file=file, has_file="N")

    result = cli(0, ["acq", "show", "Acq", "--show-groups", "--show-nodes"])

    # No table printed
    assert result.output.count("Size") == 1
