"""Common fixtures"""
import os
import yaml
import pytest
import pathlib
import shutil
from unittest.mock import patch

from alpenhorn import config, db, extensions
from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.acquisition import AcqType, ArchiveAcq, ArchiveFile, FileType
from alpenhorn.archive import ArchiveFile, ArchiveFileCopy, ArchiveFileCopyRequest


@pytest.fixture
def lfs():
    """Set up the test lfs by fixing the location of the lfs binary to our test lfs.py.

    Must be used by any test that instantiates LFSQuotaNodeIO or NearlineNodeIO (because
    they, in turn, instantiate alpenhorn.io.lfs.LFS).
    """

    def _mocked_which(cmd, mode=os.F_OK | os.X_OK, path=None):
        """A mock of shutil.which that points to our test LFS command."""
        if cmd == "lfs":
            return pathlib.Path(__file__).with_name("lfs.py").absolute()

        return shutil.which(cmd, mode, path)

    with patch("shutil.which", _mocked_which):
        yield


@pytest.fixture
def use_chimedb():
    """Use chimedb, if possible.

    If chimedb.core can't be imported, tests
    using this fixture will be skipped.
    """
    cdb = pytest.importorskip("chimedb.core")
    cdb.test_enable()

    config.merge_config({"extensions": ["chimedb.core.alpenhorn"]})

    yield

    # Reset the config
    config.merge_config(dict(), replace=True)


@pytest.fixture
def dbproxy():
    """Database init and teardown.

    The fixture returns the database proxy after initialisation.
    """
    # Ensure config is initialised
    config.merge_config(dict())

    # Load extensions
    extensions.load_extensions()

    # DB start
    db.init()
    db.connect()

    yield db.database_proxy

    db.close()

# Path to the data YAML files
data_path = pathlib.Path(__file__).with_name("fixtures")

@pytest.fixture
def storage_data(dbproxy):
    """Loads StorageNode and StorageGroup data into dbproxy"""
    dbproxy.create_tables([StorageGroup, StorageNode])

    # Check we're starting from a clean slate
    assert StorageGroup.select().count() == 0
    assert StorageNode.select().count() == 0

    with open(data_path.joinpath("storage.yml")) as f:
        fixtures = yaml.safe_load(f)

    StorageGroup.insert_many(fixtures["groups"]).execute()
    groups = {group["name"]: 1 + id_ for id_, group in enumerate(fixtures["groups"])}

    # fixup foreign keys for the nodes
    for node in fixtures["nodes"]:
        node["group"] = groups[node["group"]]

    # bulk load the nodes
    StorageNode.insert_many(fixtures["nodes"]).execute()

    return fixtures

@pytest.fixture
def acq_data(dbproxy):
    """Loads ArchiveAcq, AcqType, FileType, ArchiveFile data into dbproxy"""
    dbproxy.create_tables([ArchiveAcq, AcqType, FileType, ArchiveFile])

    # Check we're starting from a clean slate
    assert ArchiveAcq.select().count() == 0
    assert AcqType.select().count() == 0

    with open(data_path.joinpath("acquisition.yml")) as f:
        fixtures = yaml.safe_load(f)

    AcqType.insert_many(fixtures["types"]).execute()
    types = dict(AcqType.select(AcqType.name, AcqType.id).tuples())

    # fixup foreign keys for the acquisitions
    for acq in fixtures["acquisitions"]:
        acq["type"] = types[acq["type"]]

    ArchiveAcq.insert_many(fixtures["acquisitions"]).execute()
    acqs = dict(ArchiveAcq.select(ArchiveAcq.name, ArchiveAcq.id).tuples())

    FileType.insert_many(fixtures["file_types"]).execute()
    file_types = dict(FileType.select(FileType.name, FileType.id).tuples())

    # fixup foreign keys for the files
    for file in fixtures["files"]:
        file["acq"] = acqs[file["acq"]]
        file["type"] = file_types[file["type"]]

    ArchiveFile.insert_many(fixtures["files"]).execute()

    return fixtures

@pytest.fixture
def archive_data(dbproxy, acq_data, storage_data):
    """Loads ArchiveFile, ArchiveFileCopy, ArchiveFileCopyRequest data into dbproxy"""

    dbproxy.create_tables([ArchiveFileCopy, ArchiveFileCopyRequest])

    # Check we're starting from a clean slate
    assert ArchiveFileCopy.select().count() == 0
    assert ArchiveFileCopyRequest.select().count() == 0

    with open(data_path.joinpath("archive.yml")) as f:
        fixtures = yaml.safe_load(f)

    # name -> id lookups
    files = {file["name"]: 1 + id_ for id_, file in enumerate(acq_data["files"])}
    nodes = {node["name"]: 1 + id_ for id_, node in enumerate(storage_data["nodes"])}
    groups = {group["name"]: 1 + id_ for id_, group in enumerate(storage_data["groups"])}

    # fixup foreign keys for the file copies
    for copy in fixtures["file_copies"]:
        copy["file"] = files[copy["file"]]
        copy["node"] = nodes[copy["node"]]

    # bulk load the file copies
    ArchiveFileCopy.insert_many(fixtures["file_copies"]).execute()

    # fixup foreign keys for the copy requests
    for req in fixtures["copy_requests"]:
        req["file"] = files[req["file"]]
        req["node_from"] = nodes[req["node_from"]]
        req["group_to"] = groups[req["group_to"]]

    # bulk load the file copies
    ArchiveFileCopyRequest.insert_many(fixtures["copy_requests"]).execute()

    return fixtures


