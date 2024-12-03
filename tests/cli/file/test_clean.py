"""Test CLI: alpenhorn file clean"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


def test_no_cancel_node(clidb, cli):
    """Must have at least one of --cancel or --node"""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(2, ["file", "clean", "Acq/File"])


def test_cancel_now(clidb, cli):
    """Can't use --now and --cancel together"""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(2, ["file", "clean", "Acq/File", "--cancel", "--now"])


def test_missing_file(clidb, cli):
    """Test a bad path."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(1, ["file", "clean", "MIS/SING", "--node=Node"])


def test_missing_node(clidb, cli):
    """Test a bad node name."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(1, ["file", "clean", "Acq/File", "--node=MISSING"])


def test_clean(clidb, cli):
    """Test M-cleaning."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="F")
    node2 = StorageNode.create(name="Node2", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(file=file_, node=node2, has_file="Y", wants_file="Y")

    cli(0, ["file", "clean", "Acq/File", "--node=Node"])

    assert ArchiveFileCopy.get(node=node).wants_file == "M"
    assert ArchiveFileCopy.get(node=node2).wants_file == "Y"


def test_no_change(clidb, cli):
    """Test no change."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="M")

    cli(0, ["file", "clean", "Acq/File", "--node=Node"])

    assert ArchiveFileCopy.get(id=1).wants_file == "M"


def test_now(clidb, cli):
    """Test --now."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(0, ["file", "clean", "Acq/File", "--node=Node", "--now"])

    assert ArchiveFileCopy.get(id=1).wants_file == "N"


def test_archive(clidb, cli):
    """Test archive node."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="A")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(0, ["file", "clean", "Acq/File", "--node=Node"])

    # Not done
    assert ArchiveFileCopy.get(id=1).wants_file == "Y"


def test_archive_ok(clidb, cli):
    """Test archive node forcing."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="A")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="Y")

    cli(0, ["file", "clean", "Acq/File", "--node=Node", "--archive-ok"])

    assert ArchiveFileCopy.get(id=1).wants_file == "M"


def test_cancel(clidb, cli):
    """Test archive node forcing."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="M")

    cli(0, ["file", "clean", "Acq/File", "--node=Node", "--cancel"])

    assert ArchiveFileCopy.get(id=1).wants_file == "Y"


def test_cancel_no_node(clidb, cli):
    """Test archive node forcing."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, storage_type="F")
    node2 = StorageNode.create(name="Node2", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node, has_file="Y", wants_file="M")
    ArchiveFileCopy.create(file=file_, node=node2, has_file="Y", wants_file="N")

    cli(0, ["file", "clean", "Acq/File", "--cancel"])

    assert ArchiveFileCopy.get(id=1).wants_file == "Y"
    assert ArchiveFileCopy.get(id=2).wants_file == "Y"


def test_absent(clidb, cli):
    """Test file absent from node."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group, storage_type="F")
    node2 = StorageNode.create(name="Node2", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node2, has_file="Y", wants_file="Y")

    cli(0, ["file", "clean", "Acq/File", "--node=Node"])

    assert ArchiveFileCopy.get(id=1).wants_file == "Y"


def test_cancel_absent(clidb, cli):
    """Test cancel with file absent from node."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group, storage_type="F")
    node2 = StorageNode.create(name="Node2", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")
    file_ = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file_, node=node2, has_file="Y", wants_file="N")

    cli(0, ["file", "clean", "Acq/File", "--node=Node", "--cancel"])

    assert ArchiveFileCopy.get(id=1).wants_file == "N"
