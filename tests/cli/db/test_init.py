"""Test CLI: alpenhorn db init"""

import peewee as pw
import pytest

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    ArchiveFileImportRequest,
    DataIndexVersion,
    StorageGroup,
    StorageNode,
    StorageTransferAction,
    current_version,
)


def test_init(clidb_noinit, cli):
    """Test DB init"""

    # No tables
    with pytest.raises(pw.OperationalError):
        StorageGroup.create(name="Test")

    cli(0, ["db", "init"])

    # Check schema version
    assert DataIndexVersion.get(component="alpenhorn").version == current_version

    # Now we should have tables
    StorageGroup.create(name="Test")
    assert StorageGroup.get(name="Test").id == 1

    StorageNode.create(name="Test", group_id=1)
    assert StorageNode.get(name="Test").id == 1

    StorageTransferAction.create(group_to_id=1, node_from_id=1)
    assert StorageTransferAction.get(group_to_id=1, node_from_id=1).id == 1

    ArchiveAcq.create(name="Test")
    assert ArchiveAcq.get(name="Test").id == 1

    ArchiveFile.create(name="Test", acq_id=1)
    assert ArchiveFile.get(name="Test", acq_id=1).id == 1

    ArchiveFileCopy.create(file_id=1, node_id=1)
    assert ArchiveFileCopy.get(file_id=1, node_id=1).id == 1

    ArchiveFileCopyRequest.create(file_id=1, node_from_id=1, group_to_id=1)
    assert ArchiveFileCopyRequest.get(file_id=1, node_from_id=1, group_to_id=1).id == 1

    ArchiveFileImportRequest.create(path="path", node_id=1)
    assert ArchiveFileImportRequest.get(path="path", node_id=1).id == 1


def test_init_existing(clidb, cli):
    """Test db init with already eixsting data index."""

    # Tables are already present
    StorageGroup.create(name="Test")
    StorageNode.create(name="Test", group_id=1)
    StorageTransferAction.create(group_to_id=1, node_from_id=1)
    ArchiveAcq.create(name="Test")
    ArchiveFile.create(name="Test", acq_id=1)
    ArchiveFileCopy.create(file_id=1, node_id=1)
    ArchiveFileCopyRequest.create(file_id=1, node_from_id=1, group_to_id=1)
    ArchiveFileImportRequest.create(path="path", node_id=1)

    cli(0, ["db", "init"])

    # Check schema version
    assert DataIndexVersion.get(component="alpenhorn").version == current_version

    # Tables weren't overwritten
    assert StorageGroup.get(name="Test").id == 1
    assert StorageNode.get(name="Test").id == 1
    assert StorageTransferAction.get(group_to_id=1, node_from_id=1).id == 1
    assert ArchiveAcq.get(name="Test").id == 1
    assert ArchiveFile.get(name="Test", acq_id=1).id == 1
    assert ArchiveFileCopy.get(file_id=1, node_id=1).id == 1
    assert ArchiveFileCopyRequest.get(file_id=1, node_from_id=1, group_to_id=1).id == 1
    assert ArchiveFileImportRequest.get(path="path", node_id=1).id == 1


def test_init_wrong_version(clidb, cli, cli_wrong_schema):
    """Test init with version mismatch."""

    # Init fails
    cli(1, ["db", "init"])


def test_init_version1(clidb, cli):
    """Test init with old CHIME data index."""

    # By "old CHIME data index" we mean one without a schema version
    DataIndexVersion.delete().execute()

    # Init fails
    cli(1, ["db", "init"])
