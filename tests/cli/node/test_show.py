"""Test CLI: alpenhorn node show"""

import pytest
from alpenhorn.db import (
    StorageGroup,
    StorageNode,
    StorageTransferAction,
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    utcnow,
)


def test_no_show(clidb, cli):
    """Test showing nothing."""

    cli(1, ["node", "show", "TEST"])


def test_show_defaults(clidb, cli):
    """Test show with default parameters."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup")
    node = StorageNode.create(name="SNode", group=group)

    result = cli(0, ["node", "show", "SNode"])

    assert "SNode" in result.output
    assert "SGroup" in result.output
    assert "Archive" in result.output
    assert "Notes" in result.output
    assert "Default" in result.output
    assert "I/O Config" in result.output


def test_show_empty_full(clidb, cli):
    """Test show most fields full."""

    now = utcnow()

    group = StorageGroup.create(name="Group")
    StorageNode.create(
        name="Node",
        group=group,
        notes="Comment",
        io_class="IOClass",
        active=True,
        auto_import=1,
        auto_verify=11,
        host="Host",
        address="Addr",
        username="User",
        max_total_gb=10.5,
        min_avail_gb=0.25,
        avail_gb=3.333,
        avail_gb_last_checked=now,
    )

    result = cli(0, ["node", "show", "Node"])

    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "Active: Yes" in result.output
    assert "Auto-Import: On" in result.output
    assert "Auto-Verify: On" in result.output
    assert "11" in result.output
    assert "Host" in result.output
    assert "Addr" in result.output
    assert "User" in result.output
    assert "10.50 GiB" in result.output
    assert "3.333 GiB" in result.output
    assert "256.0 MiB" in result.output
    assert now.ctime() + " UTC" in result.output


def test_show_empty_io_config(clidb, cli):
    """Test show with empty I/O config."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group, io_class="IOClass", io_config="{}")

    result = cli(0, ["node", "show", "Node"])

    assert "IOClass" in result.output
    assert "I/O Config" in result.output
    assert "empty" in result.output


def test_show_io_config(clidb, cli):
    """Test show with I/O config."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="Group")
    group = StorageNode.create(
        name="Node",
        group=group,
        io_class="IOClass",
        io_config='{"Param1": 1, "Param2": 2}',
    )

    result = cli(0, ["node", "show", "Node"])

    assert "Param1" in result.output
    assert "Param2" in result.output


def test_show_node_stats(clidb, cli):
    """Test show --stats."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(
        name="Node",
        group=group,
        active=True,
        max_total_gb=2**-17,  # 2**(30-17) == 2**13 == 8 kiB
    )

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="File2", acq=acq, size_b=2345)
    ArchiveFileCopy.create(file=file, node=node, has_file="X", wants_file="Y")

    file = ArchiveFile.create(name="File3", acq=acq, size_b=3456)
    ArchiveFileCopy.create(file=file, node=node, has_file="Y", wants_file="Y")

    result = cli(0, ["node", "show", "Node", "--stats"])

    assert "Total Files: 2" in result.output

    # 1234 + 3456 = 4690 bytes = 4.580078 kiB
    assert "4.580 kiB" in result.output

    # 4.58 out of 8 == 57.25 percent
    assert "57.25%" in result.output


def test_show_actions(clidb, cli, assert_row_present):
    """Test show --actions."""

    group1 = StorageGroup.create(name="Group1")
    node1 = StorageNode.create(name="Node1", group=group1)

    group2 = StorageGroup.create(name="Group2")
    node2 = StorageNode.create(name="Node2", group=group2)

    StorageTransferAction.create(
        node_from=node1, group_to=group2, autosync=1, autoclean=1
    )

    group = StorageGroup.create(name="Group3")
    StorageTransferAction.create(
        node_from=node1, group_to=group, autosync=0, autoclean=1
    )

    group = StorageGroup.create(name="Group4")
    StorageTransferAction.create(
        node_from=node1, group_to=group, autosync=1, autoclean=0
    )

    group = StorageGroup.create(name="Group5")
    StorageTransferAction.create(
        node_from=node1, group_to=group, autosync=0, autoclean=0
    )

    group = StorageGroup.create(name="Group6")
    StorageTransferAction.create(
        node_from=node2, group_to=group, autosync=1, autoclean=1
    )

    result = cli(0, ["node", "show", "Node1", "--actions"])

    # Groups 2 and 3 are autocleaned
    assert_row_present(
        result.output, "Group2", "Auto-clean", "File added to that group"
    )
    assert_row_present(
        result.output, "Group3", "Auto-clean", "File added to that group"
    )

    # Groups 2 and 4 are autosynced
    assert_row_present(result.output, "Group2", "Auto-sync", "File added to this node")
    assert_row_present(result.output, "Group4", "Auto-sync", "File added to this node")

    assert "Node5" not in result.output
    assert "Node6" not in result.output


def test_show_transfers(clidb, cli, assert_row_present):
    """Test show --transfers."""

    group1 = StorageGroup.create(name="Group1")
    group2 = StorageGroup.create(name="Group2")
    node = StorageNode.create(name="Node", group=group1)

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)

    # Transfers
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group1, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group1, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group1, file=file, completed=0, cancelled=0
    )

    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group2, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group2, file=file, completed=0, cancelled=1
    )
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group2, file=file, completed=1, cancelled=0
    )

    result = cli(0, ["node", "show", "Node", "--transfers"])

    assert_row_present(result.output, "Group1", "3", "3.615 kiB")
    assert_row_present(result.output, "Group2", "1", "1.205 kiB")


def test_show_all(clidb, cli, assert_row_present):
    """Test show --all."""

    group = StorageGroup.create(name="Group")
    group2 = StorageGroup.create(name="Group2")
    node = StorageNode.create(
        name="Node",
        group=group,
        active=True,
        max_total_gb=2**-17,  # 2**(30-17) == 2**13 == 8 kiB
    )
    StorageTransferAction.create(
        node_from=node, group_to=group, autosync=1, autoclean=1
    )

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="File2", acq=acq, size_b=2345)
    ArchiveFileCopy.create(file=file, node=node, has_file="X", wants_file="Y")

    file = ArchiveFile.create(name="File3", acq=acq, size_b=3456)
    ArchiveFileCopy.create(file=file, node=node, has_file="Y", wants_file="Y")

    # Transfers
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group, file=file, completed=0, cancelled=0
    )

    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group2, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group2, file=file, completed=0, cancelled=1
    )
    ArchiveFileCopyRequest.create(
        node_from=node, group_to=group2, file=file, completed=1, cancelled=0
    )

    result = cli(0, ["node", "show", "Node", "--all"])

    assert "Total Files: 2" in result.output

    # 1234 + 3456 = 4690 bytes = 4.580078 kiB
    assert "4.580 kiB" in result.output

    # 4.58 out of 8 == 57.25 percent
    assert "57.25%" in result.output

    assert_row_present(result.output, "Group", "3", "10.12 kiB")
    assert_row_present(result.output, "Group2", "1", "3.375 kiB")

    assert_row_present(result.output, "Group", "Auto-clean", "File added to that group")
    assert_row_present(result.output, "Group", "Auto-sync", "File added to this node")
