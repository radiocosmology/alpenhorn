import pytest
from unittest.mock import patch, MagicMock

from alpenhorn import extensions


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_no_module(set_config):
    """Test that trying to extend with an invalid module raises ImportError."""

    with pytest.raises(ImportError):
        extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["alpenhorn.extensions"]})
def test_bad_extensions(set_config):
    """Test that trying to extend with a module lacking a register_extension
    function returns RuntimeError."""

    with pytest.raises(RuntimeError):
        extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_good_extension(set_config):
    """Test that importing two DB extensions fails"""

    # Make a fake DB module
    test_module = MagicMock()
    test_module.register_extension.return_value = {"database": 1}

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["db1", "db2"]})
def test_two_dbs(set_config):
    """Test that importing two DB extensions fails"""

    # Make a couple of fake DB modules
    db1 = MagicMock()
    db1.register_extension.return_value = {"database": 1}

    db2 = MagicMock()
    db2.register_extension.return_value = {"database": 2}

    # Patch sys.modules so import can find them.
    with patch.dict("sys.modules", db1=db1, db2=db2):
        with pytest.raises(RuntimeError):
            extensions.load_extensions()
