"""Test CLI: alpenhorn file show"""

import datetime

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
)


def test_no_file(clidb, cli):
    """Test with no file."""

    cli(2, ["file", "show"])


def test_file_not_found(clidb, cli):
    """Test with non-existent file."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(1, ["file", "show", "Acq/File2"])


def test_absfile(clidb, cli):
    """Test show with absolute file."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File1", acq=acq, size_b=123)

    cli(1, ["file", "show", "/node/Acq/File1"])


def test_simple_show(clidb, cli):
    """Test show with no options."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(
        name="File", acq=acq, size_b=123, md5sum="D41D8CD98F00B204E9800998ECF8427E"
    )

    result = cli(0, ["file", "show", "Acq/File"])

    assert "Name: File" in result.output
    assert "Acquisition: Acq" in result.output
    assert "Path: Acq/File" in result.output

    assert "Size: 123 B" in result.output
    assert "MD5 Hash: d41d8cd98f00b204e9800998ecf8427e" in result.output


def test_show_groups(clidb, cli, assert_row_present):
    """Test show with --groups."""

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=123)

    group1 = StorageGroup.create(name="Group1")
    node1a = StorageNode.create(name="Node1a", group=group1)
    ArchiveFileCopy.create(
        node=node1a, file=file, has_file="X", wants_file="Y", size_b=234
    )
    node1b = StorageNode.create(name="Node1b", group=group1)
    ArchiveFileCopy.create(
        node=node1b, file=file, has_file="M", wants_file="Y", size_b=345
    )
    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)
    ArchiveFileCopy.create(
        node=node2, file=file, has_file="Y", wants_file="Y", size_b=456
    )
    group3 = StorageGroup.create(name="Group3")
    node3a = StorageNode.create(name="Node3a", group=group3)
    StorageNode.create(name="Node3b", group=group3)
    ArchiveFileCopy.create(
        node=node3a, file=file, has_file="N", wants_file="Y", size_b=567
    )
    group4 = StorageGroup.create(name="Group4")
    StorageNode.create(name="Node4", group=group4)

    result = cli(0, ["file", "show", "Acq/File", "--groups"])

    assert_row_present(result.output, "Group1", "Suspect", "Node1b")
    assert_row_present(result.output, "Group2", "Present", "Node2")
    assert_row_present(result.output, "Group3", "Absent", "-")
    assert "Group4" not in result.output


def test_show_no_groups(clidb, cli, assert_row_present):
    """Test show with --groups, but nothing to show."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq, size_b=123)

    result = cli(0, ["file", "show", "Acq/File", "--groups"])

    assert "No extant copies" in result.output


def test_show_nodes(clidb, cli, assert_row_present):
    """Test show with --nodes."""

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=123)

    group = StorageGroup.create(name="Group")

    node = StorageNode.create(name="NodeYY", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="Y", wants_file="Y", size_b=234
    )
    node = StorageNode.create(name="NodeYM", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="Y", wants_file="M", size_b=345
    )
    node = StorageNode.create(name="NodeYN", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="Y", wants_file="N", size_b=456
    )

    node = StorageNode.create(name="NodeMY", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="M", wants_file="Y", size_b=567
    )
    node = StorageNode.create(name="NodeMM", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="M", wants_file="M", size_b=678
    )
    node = StorageNode.create(name="NodeMN", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="M", wants_file="N", size_b=789
    )

    node = StorageNode.create(name="NodeXY", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="X", wants_file="Y", size_b=321
    )
    node = StorageNode.create(name="NodeXM", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="X", wants_file="M", size_b=423
    )
    node = StorageNode.create(name="NodeXN", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="X", wants_file="N", size_b=534
    )

    node = StorageNode.create(name="NodeNY", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="N", wants_file="Y", size_b=654
    )
    node = StorageNode.create(name="NodeNM", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="N", wants_file="M", size_b=765
    )
    node = StorageNode.create(name="NodeNN", group=group)
    ArchiveFileCopy.create(
        node=node, file=file, has_file="N", wants_file="N", size_b=876
    )
    file = ArchiveFile.create(name="File2", acq=acq, size_b=123)
    node = StorageNode.create(name="Node2", group=group)
    ArchiveFileCopy.create(node=node, file=file, has_file="Y", wants_file="Y")

    result = cli(0, ["file", "show", "Acq/File", "--nodes"])

    assert_row_present(result.output, "NodeYY", "Healthy", "234 B")
    assert_row_present(result.output, "NodeYM", "Removable", "345 B")
    assert_row_present(result.output, "NodeYN", "Released", "456 B")

    assert_row_present(result.output, "NodeMY", "Suspect", "567 B")
    assert_row_present(result.output, "NodeMM", "Suspect", "678 B")
    assert_row_present(result.output, "NodeMN", "Released", "789 B")

    assert_row_present(result.output, "NodeXY", "Corrupt", "-")
    assert_row_present(result.output, "NodeXM", "Corrupt", "-")
    assert_row_present(result.output, "NodeXN", "Released", "-")

    assert_row_present(result.output, "NodeNY", "Missing", "-")
    assert_row_present(result.output, "NodeNM", "Removed", "-")
    assert_row_present(result.output, "NodeNN", "Removed", "-")
    assert "Node2" not in result.output


def test_show_no_nodes(clidb, cli, assert_row_present):
    """Test show --nodes with no nodes to show."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq, size_b=123)

    result = cli(0, ["file", "show", "Acq/File", "--nodes"])

    assert "No extant copies" in result.output


def test_show_transfers(clidb, cli, assert_row_present):
    """Test show with --transfers."""

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    time1 = datetime.datetime(2001, 1, 1, 1, 1, 1, 1)
    time2 = datetime.datetime(2002, 2, 2, 2, 2, 2, 2)
    time3 = datetime.datetime(2003, 3, 3, 3, 3, 3, 3)

    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node1,
        group_to=group1,
        completed=0,
        cancelled=0,
        timestamp=time1,
        transfer_started=time2,
        transfer_completed=time3,
    )
    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node1,
        group_to=group2,
        completed=0,
        cancelled=1,
        timestamp=time2,
    )
    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node2,
        group_to=group1,
        completed=1,
        cancelled=0,
        timestamp=time3,
    )
    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node2,
        group_to=group2,
        completed=1,
        cancelled=1,
        timestamp=0,
    )

    result = cli(0, ["file", "show", "Acq/File", "--transfers"])

    assert_row_present(
        result.output,
        "Node1",
        "Group1",
        "Pending",
        "Mon Jan  1 01:01:01 2001 UTC",
        "Sat Feb  2 02:02:02 2002 UTC",
        "Mon Mar  3 03:03:03 2003 UTC",
    )
    assert_row_present(
        result.output,
        "Node1",
        "Group2",
        "Cancelled",
        "Sat Feb  2 02:02:02 2002 UTC",
        "-",
        "-",
    )
    assert_row_present(
        result.output,
        "Node2",
        "Group1",
        "Complete",
        "Mon Mar  3 03:03:03 2003 UTC",
        "-",
        "-",
    )
    assert_row_present(result.output, "Node2", "Group2", "Complete", "-", "-", "-")


def test_show_no_transfers(clidb, cli, assert_row_present):
    """Test show --transfers with no transfers to show."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq, size_b=123)

    result = cli(0, ["file", "show", "Acq/File", "--transfers"])

    assert "No transfers" in result.output


def test_show_all(clidb, cli, assert_row_present):
    """Test show --all."""

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=123)

    group1 = StorageGroup.create(name="Group1")
    node1a = StorageNode.create(name="Node1a", group=group1)
    ArchiveFileCopy.create(
        node=node1a, file=file, has_file="X", wants_file="Y", size_b=234
    )
    node1b = StorageNode.create(name="Node1b", group=group1)
    ArchiveFileCopy.create(
        node=node1b, file=file, has_file="M", wants_file="Y", size_b=345
    )
    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)
    ArchiveFileCopy.create(
        node=node2, file=file, has_file="Y", wants_file="Y", size_b=456
    )

    ArchiveFileCopyRequest.create(
        file=file,
        node_from=node2,
        group_to=group1,
        completed=0,
        cancelled=0,
        timestamp=0,
    )

    result = cli(0, ["file", "show", "Acq/File", "--all"])

    assert_row_present(result.output, "Group1", "Suspect", "Node1b")
    assert_row_present(result.output, "Group2", "Present", "Node2")

    assert_row_present(result.output, "Node1a", "Corrupt", "-")
    assert_row_present(result.output, "Node1b", "Suspect", "345 B")
    assert_row_present(result.output, "Node2", "Healthy", "456 B")

    assert_row_present(result.output, "Node2", "Group1", "Pending", "-", "-", "-")
