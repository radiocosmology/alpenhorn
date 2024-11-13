"""Test CLI: alpenhorn node show"""

import pytest
from alpenhorn.db import (
    StorageGroup,
    StorageNode,
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
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
