"""Test CLI: alpenhorn file create"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


def test_bad_file(clidb, cli):
    """Test bad file."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["file", "verify", "MIS/SING", "Node"])


def test_bad_node(clidb, cli):
    """Test bad node."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(1, ["file", "verify", "Acq/File", "MISSING"])


def test_no_copy(clidb, cli):
    """Test verify with no copy record."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(1, ["file", "verify", "Acq/File", "Node"])


def test_removed(clidb, cli):
    """Test verify after removal."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    ArchiveFileCopy.create(file=file, node=node, has_file="N", wants_file="N")

    cli(1, ["file", "verify", "Acq/File", "Node"])


def test_missing(clidb, cli):
    """Test verify of missing file."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    ArchiveFileCopy.create(file=file, node=node, has_file="N", wants_file="Y")

    cli(0, ["file", "verify", "Acq/File", "Node"])

    # Now copy is suspect, not missing
    copy = ArchiveFileCopy.get(id=1)
    assert copy.has_file == "M"
    assert copy.wants_file == "Y"


def test_corrupt(clidb, cli):
    """Test verify of corrupt file."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    ArchiveFileCopy.create(file=file, node=node, has_file="X", wants_file="Y")

    cli(0, ["file", "verify", "Acq/File", "Node"])

    # Now copy is suspect, not corrupt
    copy = ArchiveFileCopy.get(id=1)
    assert copy.has_file == "M"
    assert copy.wants_file == "Y"


def test_suspect(clidb, cli):
    """Test verify of suspect file."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    ArchiveFileCopy.create(file=file, node=node, has_file="M", wants_file="Y")

    cli(0, ["file", "verify", "Acq/File", "Node"])

    # No change, I suppose
    copy = ArchiveFileCopy.get(id=1)
    assert copy.has_file == "M"
    assert copy.wants_file == "Y"


def test_good(clidb, cli):
    """Test verify of good file."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    ArchiveFileCopy.create(file=file, node=node, has_file="Y", wants_file="Y")

    cli(0, ["file", "verify", "Acq/File", "Node"])

    # Now copy is suspect, not good
    copy = ArchiveFileCopy.get(id=1)
    assert copy.has_file == "M"
    assert copy.wants_file == "Y"
