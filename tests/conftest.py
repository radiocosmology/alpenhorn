"""Common fixtures"""
import pytest

from alpenhorn import config, db, extensions


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "alpenhorn_config(*config_dict): "
        "used to set the alpenhorn.config for testing.  config_dict"
        "is merged with the default config.",
    )


@pytest.fixture
def set_config(request):
    """Set alpenhorn.config.config for testing.

    Any value given in the alpenhorn_config mark is merged into the
    default config.

    Yields alpenhorn.config.config.

    After the test completes, alpenhorn.config.config is set to None."""
    # If initialise with the default
    config.config = config._default_config.copy()

    marker = request.node.get_closest_marker("alpenhorn_config")
    if marker is not None:
        config.config = config.merge_dict_tree(config.config, marker.args[0])

    yield config.config

    # Reset globals
    config.config = None
    extensions._db_ext = None
    extensions._ext = None


@pytest.fixture
def use_chimedb(set_config):
    """Use chimedb, if possible.

    Use this fixture before dbproxy, i.e.:

        def test_chimedb(use_chimedb, dbproxy):
            [...]

    If chimedb.core can't be imported, tests
    using this fixture will be skipped.
    """
    cdb = pytest.importorskip("chimedb.core")
    cdb.test_enable()

    config.config = config.merge_dict_tree(
        set_config, {"extensions": ["chimedb.core.alpenhorn"]}
    )


@pytest.fixture
def dbproxy(set_config):
    """Database init and teardown.

    The fixture returns the database proxy after initialisation.
    """
    # Load extensions
    extensions.load_extensions()

    # DB start
    db.init()
    db.connect()

    yield db.database_proxy

    db.close()
