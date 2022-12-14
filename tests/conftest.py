"""Common fixtures"""
import pytest

from alpenhorn import config, db, extensions


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
