"""Test CLI: alpenhorn group show"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
    StorageTransferAction,
)


def test_no_show(clidb, cli):
    """Test showing nothing."""

    cli(1, ["group", "show", "TEST"])


def test_show_defaults(clidb, cli):
    """Test show with default parameters and no nodes."""

    # Make a StorageGroup with some nodes in it.
    StorageGroup.create(name="SGroup")

    result = cli(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Notes" in result.output
    assert "Default" in result.output
    assert "Nodes" in result.output


def test_show_no_io_config(clidb, cli):
    """Test show with no I/O config."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", notes="Comment", io_class="IOClass")
    StorageNode.create(name="Node1", group=group)
    StorageNode.create(name="Node2", group=group)

    result = cli(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "I/O Config" in result.output


def test_show_empty_io_config(clidb, cli):
    """Test show with empty I/O config."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(
        name="SGroup", notes="Comment", io_class="IOClass", io_config="{}"
    )
    StorageNode.create(name="Node1", group=group)
    StorageNode.create(name="Node2", group=group)

    result = cli(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "I/O Config" in result.output
    assert "Node1" in result.output
    assert "Node2" in result.output


def test_show_io_config(clidb, cli):
    """Test show with I/O config."""

    # Make a StorageGroup with some nodes in it.
    StorageGroup.create(
        name="SGroup",
        notes="Comment",
        io_class="IOClass",
        io_config='{"Param1": 1, "Param2": 2}',
    )

    result = cli(0, ["group", "show", "SGroup"])

    assert "SGroup" in result.output
    assert "Comment" in result.output
    assert "IOClass" in result.output
    assert "Param1" in result.output
    assert "Param2" in result.output


def test_show_node_details(clidb, cli, assert_row_present):
    """Test show --node-details."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", io_class="IOClass")
    StorageNode.create(name="Node1", group=group, active=True, host="over_here")
    StorageNode.create(
        name="Node2", group=group, active=False, host="over_there", io_class="NodeClass"
    )

    result = cli(0, ["group", "show", "SGroup", "--node-details"])

    assert_row_present(result.output, "Node1", "over_here", "Yes", "Default")
    assert_row_present(result.output, "Node2", "over_there", "No", "NodeClass")


def test_show_node_stats(clidb, cli, assert_row_present):
    """Test show --node-stats."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", io_class="IOClass")
    node1 = StorageNode.create(name="Node1", group=group, active=True, host="over_here")
    node2 = StorageNode.create(
        name="Node2",
        group=group,
        active=False,
        host="over_there",
        io_class="NodeClass",
        max_total_gb=1,
    )

    # And some files
    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="X", wants_file="Y")

    file = ArchiveFile.create(name="File2", acq=acq, size_b=2345)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="File3", acq=acq, size_b=3456)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = cli(0, ["group", "show", "SGroup", "--node-stats"])

    assert_row_present(result.output, "Node1", 2, "4.580 kiB", "-")
    assert_row_present(result.output, "Node2", 2, "5.665 kiB", "0.00")


def test_show_node_details_stats(clidb, cli, assert_row_present):
    """Test show --node-details --node-stats."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", io_class="IOClass")
    node1 = StorageNode.create(name="Node1", group=group, active=True, host="over_here")
    node2 = StorageNode.create(
        name="Node2",
        group=group,
        active=False,
        host="over_there",
        io_class="NodeClass",
        max_total_gb=1,
    )

    # And some files
    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="X", wants_file="Y")

    file = ArchiveFile.create(name="File2", acq=acq, size_b=2345)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="File3", acq=acq, size_b=3456)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    result = cli(0, ["group", "show", "SGroup", "--node-stats", "--node-details"])

    assert_row_present(
        result.output, "Node1", "over_here", "Yes", "Default", 2, "4.580 kiB", "-"
    )
    assert_row_present(
        result.output, "Node2", "over_there", "No", "NodeClass", 2, "5.665 kiB", "0.00"
    )


def test_show_actions(clidb, cli, assert_row_present):
    """Test show --actions."""

    group = StorageGroup.create(name="Group1")

    group2 = StorageGroup.create(name="Group2")
    node = StorageNode.create(name="Node1", group=group2)
    StorageTransferAction.create(
        node_from=node, group_to=group, autosync=1, autoclean=1
    )

    node = StorageNode.create(name="Node2", group=group2)
    StorageTransferAction.create(
        node_from=node, group_to=group, autosync=0, autoclean=1
    )

    node = StorageNode.create(name="Node3", group=group2)
    StorageTransferAction.create(
        node_from=node, group_to=group, autosync=1, autoclean=0
    )

    node = StorageNode.create(name="Node4", group=group2)
    StorageTransferAction.create(
        node_from=node, group_to=group, autosync=0, autoclean=0
    )

    node = StorageNode.create(name="Node5", group=group2)
    StorageTransferAction.create(
        node_from=node, group_to=group2, autosync=1, autoclean=1
    )

    result = cli(0, ["group", "show", "Group1", "--actions"])

    # Nodes 1 and 2 are autocleaned
    assert_row_present(result.output, "Node1", "Auto-clean", "File added to this group")
    assert_row_present(result.output, "Node2", "Auto-clean", "File added to this group")

    # Nodes 1 and 3 are autosynced
    assert_row_present(result.output, "Node1", "Auto-sync", "File added to that node")
    assert_row_present(result.output, "Node3", "Auto-sync", "File added to that node")

    assert "Node4" not in result.output
    assert "Node5" not in result.output


def test_show_transfers(clidb, cli, assert_row_present):
    """Test show --transfers."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", io_class="IOClass")
    node1 = StorageNode.create(name="Node1", group=group, active=True, host="over_here")
    node2 = StorageNode.create(
        name="Node2",
        group=group,
        active=False,
        host="over_there",
        io_class="NodeClass",
        max_total_gb=1,
    )

    StorageTransferAction.create(
        node_from=node1, group_to=group, autosync=1, autoclean=1
    )

    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="X", wants_file="Y")

    # Transfers
    ArchiveFileCopyRequest.create(
        node_from=node1, group_to=group, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node1, group_to=group, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node1, group_to=group, file=file, completed=0, cancelled=0
    )

    ArchiveFileCopyRequest.create(
        node_from=node2, group_to=group, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node2, group_to=group, file=file, completed=0, cancelled=1
    )
    ArchiveFileCopyRequest.create(
        node_from=node2, group_to=group, file=file, completed=1, cancelled=0
    )

    result = cli(0, ["group", "show", "SGroup", "--transfers"])

    assert_row_present(result.output, "Node1", "3", "3.615 kiB")
    assert_row_present(result.output, "Node2", "1", "1.205 kiB")


def test_show_all(clidb, cli, assert_row_present):
    """Test show --all."""

    # Make a StorageGroup with some nodes in it.
    group = StorageGroup.create(name="SGroup", io_class="IOClass")
    node1 = StorageNode.create(name="Node1", group=group, active=True, host="over_here")
    node2 = StorageNode.create(
        name="Node2",
        group=group,
        active=False,
        host="over_there",
        io_class="NodeClass",
        max_total_gb=1,
    )

    StorageTransferAction.create(
        node_from=node1, group_to=group, autosync=1, autoclean=1
    )

    # And some files
    acq = ArchiveAcq.create(name="acq")
    file = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="X", wants_file="Y")

    file = ArchiveFile.create(name="File2", acq=acq, size_b=2345)
    ArchiveFileCopy.create(file=file, node=node1, has_file="N", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    file = ArchiveFile.create(name="File3", acq=acq, size_b=3456)
    ArchiveFileCopy.create(file=file, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file, node=node2, has_file="Y", wants_file="Y")

    # Transfers
    ArchiveFileCopyRequest.create(
        node_from=node1, group_to=group, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node1, group_to=group, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node1, group_to=group, file=file, completed=0, cancelled=0
    )

    ArchiveFileCopyRequest.create(
        node_from=node2, group_to=group, file=file, completed=0, cancelled=0
    )
    ArchiveFileCopyRequest.create(
        node_from=node2, group_to=group, file=file, completed=0, cancelled=1
    )
    ArchiveFileCopyRequest.create(
        node_from=node2, group_to=group, file=file, completed=1, cancelled=0
    )

    result = cli(0, ["group", "show", "SGroup", "--all"])

    assert_row_present(
        result.output, "Node1", "over_here", "Yes", "Default", 2, "4.580 kiB", "-"
    )
    assert_row_present(
        result.output, "Node2", "over_there", "No", "NodeClass", 2, "5.665 kiB", "0.00"
    )

    assert_row_present(result.output, "Node1", "3", "10.12 kiB")
    assert_row_present(result.output, "Node2", "1", "3.375 kiB")

    assert_row_present(result.output, "Node1", "Auto-clean", "File added to this group")
    assert_row_present(result.output, "Node1", "Auto-sync", "File added to that node")
