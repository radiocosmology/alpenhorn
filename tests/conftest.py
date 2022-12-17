"""Common fixtures"""
import os
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
            return pathlib.Path(__file__).with_name("lfs").absolute()

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


# Data table fixtures.  Each of these will add a row with the specified
# data to the appropriate table in the DB, creating the table first if
# necessary


@pytest.fixture
def factory_factory(dbproxy):
    """Fixture which creates a factory which, in turn, creates a factory fixture
    for inserting data into the database."""

    def _factory_factory(model):
        nonlocal dbproxy

        def _factory(**kwargs):
            nonlocal dbproxy
            nonlocal model

            # This does nothing if the table already exists
            dbproxy.create_tables([model])

            # Add and return the record
            return model.create(**kwargs)

        return _factory

    return _factory_factory


@pytest.fixture
def storagegroup(factory_factory):
    return factory_factory(StorageGroup)


@pytest.fixture
def storagenode(factory_factory):
    return factory_factory(StorageNode)


@pytest.fixture
def acqtype(factory_factory):
    return factory_factory(AcqType)


@pytest.fixture
def archiveacq(factory_factory):
    return factory_factory(ArchiveAcq)


@pytest.fixture
def filetype(factory_factory):
    return factory_factory(FileType)


@pytest.fixture
def archivefile(factory_factory):
    return factory_factory(ArchiveFile)


@pytest.fixture
def archivefilecopy(factory_factory):
    return factory_factory(ArchiveFileCopy)


# Generic versions of the above.  When you just want a record, but don't care
# what it is.


@pytest.fixture
def genericgroup(storagegroup):
    """Create a generic StorageGroup record."""
    return storagegroup(name="genericgroup")


@pytest.fixture
def genericnode(storagenode, storagegroup):
    """Create a generic StorageNode record.

    Creates all necessary backrefs.
    """
    group = storagegroup(name="genericnode_group")
    return storagenode(name="genericnode", group=group, root="/root")


@pytest.fixture
def genericacq(acqtype, archiveacq, filetype, archivefile):
    """Create a generic ArchiveAcq record.

    Creates all necessary backrefs.
    """
    acqtype = acqtype(name="genericacq_acqetype")
    return archiveacq(name="genericacq", type=acqtype)


@pytest.fixture
def genericfile(acqtype, archiveacq, filetype, archivefile):
    """Create a generic ArchiveFile record.

    Creates all necessary backrefs.
    """
    acqtype = acqtype(name="genericfile_actype")
    acq = archiveacq(name="genericfile_acq", type=acqtype)
    filetype = filetype(name="genericfile_filetype")
    return archivefile(name="genericfile", acq=acq, type=filetype, size_b=2**20)
