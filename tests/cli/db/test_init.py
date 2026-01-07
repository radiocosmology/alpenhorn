"""Test CLI: alpenhorn db init"""

import pathlib
import sys
from unittest.mock import MagicMock, patch
from urllib.parse import quote as urlquote

import peewee as pw
import pytest
import yaml

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
from alpenhorn.extensions import DataIndexExtension


@pytest.fixture
def patimp_ext(clidb_noinit, clidb_uri, xfs, reset_extensions):
    """Fixture to set-up using the pattern_importer extension.

    Returns a tuple:
    extlist:
        list of extensions, excluding the DataIndexExtension
    post_init:
        a MagicMock being used as the DataIndexExtension's post init function
    """

    # Pre-import the pattern importer to make the CLI's job easier
    sys.path.append(
        str(pathlib.Path(__file__).parent.joinpath("..", "..", "..", "examples"))
    )
    import pattern_importer

    # Add the pattern_importer extension to the config
    with open("/etc/alpenhorn/alpenhorn.conf", "w") as f:
        config = {
            "database": {
                "url": "sqlite:///?database=" + urlquote(clidb_uri) + "&uri=true"
            },
            "extensions": ["pattern_importer"],
        }
        f.write(yaml.dump(config))

    # Separate out the DataIndexExtension
    extlist = []
    for ext in pattern_importer.register_extensions():
        if isinstance(ext, DataIndexExtension):
            di_ext = ext
        else:
            extlist.append(ext)

    # The mocked post-init function
    post_init = MagicMock()

    # The replacement register_extensions hook
    def _register_extensions():
        nonlocal extlist, di_ext, post_init

        # Re-create the DataIndexExtension
        new_di_ext = DataIndexExtension(
            name=di_ext.name,
            version=str(di_ext.version),
            component=di_ext.component,
            schema_version=di_ext.schema_version,
            tables=di_ext.tables,
            post_init=post_init,
        )

        # Return all extensions
        return [new_di_ext, *extlist]

    # Mock the register_extensions hook so changes to ext will be propagated.
    with patch("pattern_importer.register_extensions", _register_extensions):
        yield extlist, post_init


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


def test_init_nodb(cli, xfs):
    # Mess up the config file
    with open("/etc/alpenhorn/alpenhorn.conf", "w") as f:
        config = {"database": {"url": "sqlite:////MISSING/MISSING.db"}}
        f.write(yaml.dump(config))

    # Init fails
    cli(1, ["db", "init"])


def test_init_ext(cli, dbproxy, patimp_ext):
    """Test init with a data index extension."""

    import pattern_importer

    _, post_init = patimp_ext

    # Run Init
    cli(0, ["db", "init"])

    # Check schema version
    assert (
        DataIndexVersion.get(component="pattern_importer").version
        == pattern_importer.schema_version
    )

    # Check tables
    tables = dbproxy.get_tables()
    assert "acqdata" in tables
    assert "acqtype" in tables
    assert "filedata" in tables
    assert "filetype" in tables

    # Check that the post_init function was called
    post_init.assert_called()


def test_ext_rollback(cli, dbproxy, patimp_ext):
    """An exception in a post_init callback triggers rollback."""

    _, post_init = patimp_ext

    # Set the exception
    post_init.side_effect = RuntimeError("test")

    # Run Init
    cli(1, ["db", "init"])

    # Check schema versions -- data index itself should exist
    assert DataIndexVersion.get(component="alpenhorn").version == current_version
    # But pattern importer shouldn't exist
    with pytest.raises(pw.DoesNotExist):
        DataIndexVersion.get(component="pattern_importer").version

    # Check tables aren't present
    tables = dbproxy.get_tables()
    assert "acqdata" not in tables
    assert "acqtype" not in tables
    assert "filedata" not in tables
    assert "filetype" not in tables

    # Check that the post_init function was called
    post_init.assert_called()


def test_only_missing(clidb_noinit, cli):
    """Test using --only on an unknown component"""

    cli(1, ["db", "init", "--only=missing"])

    # Data index doesn't exist
    with pytest.raises(pw.OperationalError):
        DataIndexVersion.get(component="alpenhorn").version


def test_only_alpenhorn(clidb_noinit, cli, patimp_ext):
    """Test --only=alpenhorn"""

    cli(0, ["db", "init", "--only=alpenhorn"])

    # Data index exists
    DataIndexVersion.get(component="alpenhorn").version

    # But the patern importer stuff does
    with pytest.raises(pw.DoesNotExist):
        DataIndexVersion.get(component="pattern_importer").version


def test_only_no_alpenhorn(clidb_noinit, cli, patimp_ext):
    """Test --only without a data index."""

    # Deliberately fails because there's no DataIndexVersion table to record success.
    cli(1, ["db", "init", "--only=pattern_importer"])


def test_init_old_ext(clidb, cli, patimp_ext):
    """Test init with an old extension.

    i.e. where the schema version of the component
    in the database is too new."""

    from pattern_importer import schema_version

    # "Init" a new version of the component
    DataIndexVersion.create(component="pattern_importer", version=schema_version + 1)

    # Init fails because of required_schema fail
    cli(1, ["db", "init"])


def test_init_new_ext(clidb, cli, patimp_ext):
    """Test init with a newer extension.

    i.e. where the schema version of the component
    in the database exists bue is an old version."""

    from pattern_importer import schema_version

    # "Init" an old version of the component
    DataIndexVersion.create(component="pattern_importer", version=schema_version - 1)

    # Init fails because migration is needed
    cli(1, ["db", "init"])
