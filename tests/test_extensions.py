import os

import pytest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from alpenhorn import acquisition, extensions, generic


@pytest.fixture
def fixtures():

    from alpenhorn import db

    db._connect()

    db.database_proxy.create_tables(
        [
            acquisition.AcqType,
            acquisition.FileType,
            acquisition.ArchiveAcq,
            acquisition.ArchiveFile,
            generic.GenericAcqInfo,
            generic.GenericFileInfo,
        ]
    )

    yield

    # cleanup
    db.database_proxy.close()


def test_invalid_extension():
    # Test that invalid extension paths, or modules that are not extensions
    # throw the approproate exceptions

    with patch("alpenhorn.config.config", {"extensions": ["unknown_module"]}):
        with pytest.raises(ImportError):
            extensions.load_extensions()

    with patch("alpenhorn.config.config", {"extensions": ["alpenhorn.acquisition"]}):
        with pytest.raises(RuntimeError):
            extensions.load_extensions()


def test_generic_extension(fixtures):
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
