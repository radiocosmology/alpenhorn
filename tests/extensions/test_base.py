"""Tests of the base Extension."""

from unittest.mock import MagicMock, call, patch

import pytest

from alpenhorn import __version__
from alpenhorn.extensions.base import Extension


def test_no_name():
    """Name may not be empty."""
    with pytest.raises(ValueError):
        Extension("", "1")


def test_dot_name():
    """Dot is not allowed in the name."""
    with pytest.raises(ValueError):
        Extension("a.b", "1")


def test_empty_version():
    """Version may not be empty."""
    with pytest.raises(ValueError):
        Extension("test", "")


def test_bad_version():
    """Version must be parseable."""
    with pytest.raises(ValueError):
        Extension("test", "one")


def test_bad_min_version():
    """min_version must be parseable."""
    with pytest.raises(ValueError):
        Extension("test", "1", min_version="two")


def test_bad_max_version():
    """max_version must be parseable."""
    with pytest.raises(ValueError):
        Extension("test", "1", max_version="two")


def test_init_too_old():
    """Init doesn't happen if we're too old."""

    ext = Extension("test", "1", min_version="1" + __version__)
    assert not ext.init_extension()


def test_init_too_new():
    """Init doesn't happen if we're too new."""

    ext = Extension("test", "1", max_version="0")
    assert not ext.init_extension()


def test_init_good_version():
    """Check version checking."""

    ext = Extension("test", "1", min_version=__version__, max_version=__version__)
    assert ext.init_extension()

    ext = Extension("test", "1", min_version="0", max_version="1" + __version__)
    assert ext.init_extension()


def test_init_schema_skip():
    """Schema checking is skipped for stage-1 Extensions."""

    ext = Extension("test", "1", require_schema={"alpenhorn": 1, "other_thing": 2})
    ext.stage = 1

    mock = MagicMock()
    mock.return_value = False
    with patch("alpenhorn.db.schema_version", mock):
        assert ext.init_extension()

    mock.assert_not_called()


def test_init_schema_bad():
    """Check schema check failure."""

    ext = Extension("test", "1", require_schema={"alpenhorn": 1, "other_thing": 2})
    ext.stage = 2

    mock = MagicMock()
    mock.return_value = False
    with patch("alpenhorn.db.schema_version", mock):
        assert not ext.init_extension()

    mock.assert_called()


def test_init_schema_good():
    """Check schema check success."""

    required_schema = {"alpenhorn": 1, "other_thing": 2}

    ext = Extension("test", "1", require_schema=required_schema)
    ext.stage = 2

    mock = MagicMock()
    mock.return_value = True
    with patch("alpenhorn.db.schema_version", mock):
        assert ext.init_extension()

    calls = [
        call(component=key, component_version=val, return_check=True)
        for key, val in required_schema.items()
    ]
    mock.assert_has_calls(calls, any_order=True)
