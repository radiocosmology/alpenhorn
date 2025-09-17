"""test alpenhorn.common.extload."""

from unittest.mock import MagicMock, patch

import click
import pytest

from alpenhorn import extensions
from alpenhorn.common import extload
from alpenhorn.io.default import DefaultNodeIO


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_no_module(set_config):
    """Test that trying to extend with an invalid module raises
    ModuleNotFoundError."""

    with pytest.raises(click.ClickException):
        extload.find_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["alpenhorn.common.extload"]})
def test_bad_extensions(set_config):
    """Test that trying to extend with a module lacking a register_extensions
    function returns RuntimeError."""

    with pytest.raises(click.ClickException):
        extload.find_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_good_db(set_config):
    """Test that importing a proper DB extensions succeeds."""

    # Make a fake DB module
    test_module = MagicMock()
    test_module.register_extensions.return_value = [
        extensions.DatabaseExtension("test", "1.0", connect=lambda config: None)
    ]

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        extload.find_extensions()


@pytest.mark.alpenhorn_config({"extensions": ["db1", "db2"]})
def test_two_dbs(set_config):
    """Test that initialising two DB extensions fails"""

    # Make a couple of fake DB modules
    db1 = MagicMock()
    db1.register_extensions.return_value = [
        extensions.DatabaseExtension("db1", "1.0", connect=lambda config: None)
    ]

    db2 = MagicMock()
    db2.register_extensions.return_value = [
        extensions.DatabaseExtension("db2", "2.0", connect=lambda config: None)
    ]

    # Patch sys.modules so import can find them.
    with patch.dict("sys.modules", db1=db1, db2=db2):
        # This is fine: two extensions can be found.
        ext = extload.find_extensions()

    # But initialising both of them won't work.
    with pytest.raises(click.ClickException):
        extload.init_extensions(ext, stage=1)


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_importdetect_good(set_config):
    """Test good import-detect module"""

    # Fake extension
    idext = extensions.ImportDetectExtension("test", "1.0", detect=MagicMock())
    test_module = MagicMock()
    test_module.register_extensions.return_value = [idext]

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        ext = extload.find_extensions()

    extload.init_extensions(ext, stage=2)

    assert extload.import_detection() == [idext]


@pytest.mark.alpenhorn_config({"extensions": ["id1", "id2", "id3"]})
def test_importdetect_multi(set_config):
    """Test multiple import-detect modules"""

    # Fake extensions
    ext1 = extensions.ImportDetectExtension("ext1", "1.0", detect=MagicMock())
    id1 = MagicMock()
    id1.register_extensions.return_value = [ext1]

    ext2 = extensions.ImportDetectExtension("ext2", "1.0", detect=MagicMock())
    id2 = MagicMock()
    id2.register_extensions.return_value = [ext2]

    # These are in the same module
    ext3 = extensions.ImportDetectExtension("ext3", "1.0", detect=MagicMock())
    ext4 = extensions.ImportDetectExtension("ext4", "1.0", detect=MagicMock())
    id3 = MagicMock()
    id3.register_extensions.return_value = [ext3, ext4]

    # Patch sys.modules so import can find the modules.
    with patch.dict("sys.modules", id1=id1, id2=id2, id3=id3):
        ext = extload.find_extensions()

    extload.init_extensions(ext, stage=2)

    # We don't know the order of initialisation, so we convert to a
    # set
    assert set(extload.import_detection()) == {ext1, ext2, ext3, ext4}


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_io_extension_internal_name(set_config):
    """Extension with an internal io-class name must be rejected."""

    # Also a fake extension module
    ext = extensions.IOClassExtension(
        "ext", "1.0", io_class_name="Default", node_class=DefaultNodeIO
    )
    test_module = MagicMock()
    test_module.register_extensions.return_value = [ext]

    with patch.dict("sys.modules", test_module=test_module):
        ext = extload.find_extensions()

    with pytest.raises(click.ClickException):
        extload.init_extensions(ext, stage=2)


@pytest.mark.alpenhorn_config({"extensions": ["mod1", "mod2"]})
def test_io_extension_duplicate(set_config):
    """Reject duplicated I/O classes."""

    # This fake extension module gets loaded twice (under different names),
    # leading to the duplication
    ext = extensions.IOClassExtension(
        "ext", "1.0", io_class_name="Test", node_class=DefaultNodeIO
    )
    test_module = MagicMock()
    test_module.register_extensions.return_value = [ext]

    with patch.dict("sys.modules", mod1=test_module, mod2=test_module):
        ext = extload.find_extensions()

    with pytest.raises(click.ClickException):
        extload.init_extensions(ext, stage=2)


@pytest.mark.alpenhorn_config({"extensions": ["test_module"]})
def test_io_extension_external(set_config):
    """Test io_extension() returning an external io module."""

    # This is our fake IO module
    ioext = extensions.IOClassExtension(
        "test", "1.0", io_class_name="IOMod", node_class=DefaultNodeIO
    )

    # Also a fake extension module
    test_module = MagicMock()
    test_module.register_extensions.return_value = [ioext]

    # Patch sys.modules so import can find it.
    with patch.dict("sys.modules", test_module=test_module):
        # Load the extension module
        ext = extload.find_extensions()

    # Initialise
    extload.init_extensions(ext, stage=2)

    # Module should be returned
    assert ioext is extload.io_extension("IOMod")


def test_io_extension_internal():
    """Test io_extension() returning the internal io modules."""
    from alpenhorn.io import internal_io

    # This is needed to properly initialise the extload internals
    extload.find_extensions()

    for class_name, extension in internal_io.items():
        assert extension is extload.io_extension(class_name)


def test_io_extension_missing():
    """Test a failed load in io_extension()."""

    assert extload.io_extension("Missing") is None
