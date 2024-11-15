"""Test CLI: alpenhorn db init"""

import pytest
import peewee as pw

from alpenhorn.db import (
    StorageGroup,
    StorageNode,
    StorageTransferAction,
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
)


def test_init(clidb_noinit, cli):
    """Test DB init"""

    # No tables
    with pytest.raises(pw.OperationalError):
        StorageGroup.create(name="Test")

    cli(0, ["db", "init"])

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


def test_init_safe(clidb, cli):
    """Test DB init doesn't overwrite tables"""

    # Tables are already present
    StorageGroup.create(name="Test")
    StorageNode.create(name="Test", group_id=1)
    StorageTransferAction.create(group_to_id=1, node_from_id=1)
    ArchiveAcq.create(name="Test")
    ArchiveFile.create(name="Test", acq_id=1)
    ArchiveFileCopy.create(file_id=1, node_id=1)
    ArchiveFileCopyRequest.create(file_id=1, node_from_id=1, group_to_id=1)

    cli(0, ["db", "init"])

    # Tables weren't overwritten
    assert StorageGroup.get(name="Test").id == 1
    assert StorageNode.get(name="Test").id == 1
    assert StorageTransferAction.get(group_to_id=1, node_from_id=1).id == 1
    assert ArchiveAcq.get(name="Test").id == 1
    assert ArchiveFile.get(name="Test", acq_id=1).id == 1
    assert ArchiveFileCopy.get(file_id=1, node_id=1).id == 1
    assert ArchiveFileCopyRequest.get(file_id=1, node_from_id=1, group_to_id=1).id == 1
