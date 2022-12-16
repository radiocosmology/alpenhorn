"""Common fixtures"""
import os
import pytest
import pathlib
import shutil
from unittest.mock import patch

from alpenhorn import config, db, extensions


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
