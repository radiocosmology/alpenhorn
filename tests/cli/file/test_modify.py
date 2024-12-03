"""Test CLI: alpenhorn file modify"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


def test_no_data(clidb, cli):
    """Test providing no data."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="Name", acq=acq)

    cli(2, ["file", "modify", "Acq/Name"])


def test_bad_size(clidb, cli):
    """Test a negative size."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="Name", acq=acq)

    cli(1, ["file", "modify", "Acq/Name", "--size=-3"])


def test_bad_md5(clidb, cli):
    """Test a bad MD5s."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="Name", acq=acq)

    cli(1, ["file", "modify", "Acq/Name", "--md5="])
    cli(1, ["file", "modify", "Acq/Name", "--md5=FEDCBA9876543210FEDCBA987654321"])
    cli(1, ["file", "modify", "Acq/Name", "--md5=FEDCBA9876543210FEDCBA987654321Q"])
    cli(1, ["file", "modify", "Acq/Name", "--md5=FEDCBA9876543210FEDCBA9876543210F"])


def test_bad_path(clidb, cli):
    """Test a bad path."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="Name", acq=acq)

    cli(1, ["file", "modify", "Acq/MISSING", "--size=3"])


def test_update(clidb, cli):
    """Test an update."""

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="Name", acq=acq, size_b=4)

    group = StorageGroup.create(name="Group")
    node1 = StorageNode.create(name="Node1", group=group)
    node2 = StorageNode.create(name="Node2", group=group)
    node3 = StorageNode.create(name="Node3", group=group)
    node4 = StorageNode.create(name="Node4", group=group)
    node5 = StorageNode.create(name="Node5", group=group)
    node6 = StorageNode.create(name="Node6", group=group)

    ArchiveFileCopy.create(file=file_, node=node1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file_, node=node2, has_file="X", wants_file="Y")
    ArchiveFileCopy.create(file=file_, node=node3, has_file="M", wants_file="Y")
    ArchiveFileCopy.create(file=file_, node=node4, has_file="N", wants_file="Y")
    ArchiveFileCopy.create(file=file_, node=node5, has_file="Y", wants_file="M")
    ArchiveFileCopy.create(file=file_, node=node6, has_file="X", wants_file="N")

    cli(
        0,
        [
            "file",
            "modify",
            "Acq/Name",
            "--size=3",
            "--md5=FEDCBA9876543210FEDCBA9876543210",
        ],
    )

    file_ = ArchiveFile.get(name="Name", acq=acq)

    assert file_.md5sum.upper() == "FEDCBA9876543210FEDCBA9876543210"
    assert file_.size_b == 3

    # Check file copies

    assert ArchiveFileCopy.get(node=node1).has_file == "M"
    assert ArchiveFileCopy.get(node=node2).has_file == "M"
    assert ArchiveFileCopy.get(node=node3).has_file == "M"
    assert ArchiveFileCopy.get(node=node4).has_file == "N"
    assert ArchiveFileCopy.get(node=node5).has_file == "M"
    assert ArchiveFileCopy.get(node=node6).has_file == "X"


def test_update_no_change(clidb, cli):
    """Test no change for update."""

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(
        name="Name", acq=acq, md5sum="FEDCBA9876543210FEDCBA9876543210", size_b=4
    )

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(
        0,
        [
            "file",
            "modify",
            "Acq/Name",
            "--size=4",
            "--md5=FEDCBA9876543210FEDCBA9876543210",
        ],
    )

    file_ = ArchiveFile.get(name="Name", acq=acq)
    assert file_.md5sum.upper() == "FEDCBA9876543210FEDCBA9876543210"
    assert file_.size_b == 4

    # File copies haven't changed
    assert ArchiveFileCopy.get(node=node).has_file == "Y"


def test_update_noreverify(clidb, cli):
    """Test an update with --no-reverify."""

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="Name", acq=acq, size_b=3)

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(
        0,
        [
            "file",
            "modify",
            "Acq/Name",
            "--no-reverify",
            "--size=4",
            "--md5=FEDCBA9876543210FEDCBA9876543210",
        ],
    )

    file_ = ArchiveFile.get(name="Name", acq=acq)
    assert file_.md5sum.upper() == "FEDCBA9876543210FEDCBA9876543210"
    assert file_.size_b == 4

    # File copies haven't changed
    assert ArchiveFileCopy.get(node=node).has_file == "Y"
