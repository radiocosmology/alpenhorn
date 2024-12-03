"""test alpenhorn.common.extensions."""

from unittest.mock import MagicMock, patch

import pytest

from alpenhorn.common import extensions


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_no_module(set_config):
    """Test that trying to extend with an invalid module raises
    ModuleNotFoundError."""

    with pytest.raises(ModuleNotFoundError):
        extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["alpenhorn.common.extensions"]})
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
    test_module.register_extension.return_value = {"database": {}}

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["db1", "db2"]})
def test_two_dbs(set_config):
    """Test that importing two DB extensions fails"""

    # Make a couple of fake DB modules
    db1 = MagicMock()
    db1.register_extension.return_value = {"database": {}}

    db2 = MagicMock()
    db2.register_extension.return_value = {"database": {}}

    # Patch sys.modules so import can find them.
    with patch.dict("sys.modules", db1=db1, db2=db2):
        with pytest.raises(ValueError):
            extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_importdetect_not_callalbe(set_config):
    """Providing a non-callable import-detect object is an error."""

    # Fake extension
    test_module = MagicMock()
    test_module.register_extension.return_value = {"import-detect": None}

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        with pytest.raises(ValueError):
            extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_importdetect_good(set_config):
    """Test good import-detect module"""

    # Fake extension
    func = MagicMock()
    test_module = MagicMock()
    test_module.register_extension.return_value = {"import-detect": func}

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        extensions.load_extensions()

    assert extensions._id_ext == [func]


@pytest.mark.alpenhorn_config({"extensions": ["id1", "id2", "id3", "id4"]})
def test_importdetect_multi(set_config):
    """Test multiple import-detect modules"""

    # Fake extensions
    func1 = MagicMock()
    id1 = MagicMock()
    id1.register_extension.return_value = {"import-detect": func1}

    func2 = MagicMock()
    id2 = MagicMock()
    id2.register_extension.return_value = {"import-detect": func2}

    func3 = MagicMock()
    id3 = MagicMock()
    id3.register_extension.return_value = {"import-detect": func3}

    func4 = MagicMock()
    id4 = MagicMock()
    id4.register_extension.return_value = {"import-detect": func4}

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", id1=id1, id2=id2, id3=id3, id4=id4):
        extensions.load_extensions()

    assert extensions._id_ext == [func1, func2, func3, func4]


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_io_module_internal_name(set_config):
    """io-module with an internal name must be rejected."""

    # Also a fake extension module
    test_module = MagicMock()
    test_module.register_extension.return_value = {"io-modules": {"default": None}}

    with patch.dict("sys.modules", test_module=test_module):
        with pytest.raises(ValueError):
            extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["mod1", "mod2"]})
def test_io_module_duplicate(set_config):
    """Reject duplicated I/O modules."""

    # Also a fake extension module
    test_module = MagicMock()
    test_module.register_extension.return_value = {"io-modules": {"iomod": None}}

    with patch.dict("sys.modules", mod1=test_module, mod2=test_module):
        with pytest.raises(ValueError):
            extensions.load_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_io_module_external(set_config):
    """Test io_module() returning an external io module."""

    # This is our fake IO module
    iomod = MagicMock()

    # Also a fake extension module
    test_module = MagicMock()
    test_module.register_extension.return_value = {"io-modules": {"iomod": iomod}}

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        # Load the extension
        extensions.load_extensions()

    # Module should be returned
    assert iomod is extensions.io_module("IOMod")


def test_io_module_internal():
    """Test io_module() returning an internal io module."""
    import alpenhorn.io.default

    assert alpenhorn.io.default is extensions.io_module("Default")


def test_io_module_missing():
    """Test a failed load in io_module()."""

    assert extensions.io_module("Missing") is None


def test_io_module_base():
    """Loading the I/O base in io_module() is not allowed."""

    assert extensions.io_module("base") is None
