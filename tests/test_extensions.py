"""test alpenhorn.extensions."""
import pytest
from unittest.mock import patch, MagicMock

from alpenhorn import extensions


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_no_module(set_config):
    """Test that trying to extend with an invalid module raises
    ModuleNotFoundError."""

    with pytest.raises(ModuleNotFoundError):
        extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["alpenhorn.extensions"]})
def test_bad_extensions(set_config):
    """Test that trying to extend with a module lacking a register_extension
    function returns RuntimeError."""

    with pytest.raises(RuntimeError):
        extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_bad_db(set_config):
    """Test that importing a bad DB extension."""

    # Make a fake DB module
    test_module = MagicMock()
    test_module.register_extension.return_value = {"database": 1}

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        with pytest.raises(TypeError):
            extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_good_db(set_config):
    """Test that importing a proper DB extensions succeeds."""

    # Make a fake DB module
    test_module = MagicMock()
    test_module.register_extension.return_value = {"database": dict()}

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["db1", "db2"]})
def test_two_dbs(set_config):
    """Test that importing two DB extensions fails"""

    # Make a couple of fake DB modules
    db1 = MagicMock()
    db1.register_extension.return_value = {"database": dict()}

    db2 = MagicMock()
    db2.register_extension.return_value = {"database": dict()}

    # Patch sys.modules so import can find them.
    with patch.dict("sys.modules", db1=db1, db2=db2):
        with pytest.raises(ValueError):
            extensions.load_extensions()


@pytest.mark.skip(reason="broken")
def test_generic_extension(dbproxy):
    # Test that extension registration works correctly for the generic extension

    conf = {
        "extensions": ["alpenhorn.generic"],
        "acq_types": {"generic": {"patterns": "*.zxc", "file_types": ["generic"]}},
        "file_types": {"generic": {"patterns": "*.zxc"}},
    }

    # Load the extensions. This should cause the acq/file info types to be registered
    with patch("alpenhorn.config.config", conf):
        extensions.load_extensions()

        extensions.register_type_extensions()

    # Check that we have registered every known type
    acquisition.AcqType.check_registration()
    acquisition.FileType.check_registration()

    # Check that the correct entries have appeared
    assert "generic" in acquisition.AcqType._registered_acq_types
    assert "generic" in acquisition.FileType._registered_file_types
    assert (
        generic.GenericAcqInfo is acquisition.AcqType._registered_acq_types["generic"]
    )
    assert (
        generic.GenericFileInfo
        is acquisition.FileType._registered_file_types["generic"]
    )

    # Do a few look ups mapping betweeb the AcqType entry and the AcqInfo tables
    # back and forth
    assert acquisition.AcqType.get(name="generic").acq_info is generic.GenericAcqInfo
    assert generic.GenericAcqInfo.get_acq_type().acq_info is generic.GenericAcqInfo

    # Check that the file_types registration works out
    reg_file_types = generic.GenericAcqInfo.get_acq_type().file_types
    assert reg_file_types.count() == 1
    assert reg_file_types.get().file_info is generic.GenericFileInfo
