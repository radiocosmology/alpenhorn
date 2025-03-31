"""Test alpenhorn.cli.options"""

import click
import pytest

from alpenhorn.cli.options import check_if_from_stdin, set_io_config


def test_sic_empty():
    """set_io_config with no user input should return default"""
    io_config = set_io_config(None, (), {"a": 1, "b": 2})
    assert io_config == {"a": 1, "b": 2}


def test_sic_io_config():
    """set_io_config decodes io_config and replace the default"""

    io_config = set_io_config('{"a": 3, "b": 4}', (), {"a": 1, "b": 2})
    assert io_config == {"a": 3, "b": 4}


def test_sic_io_var():
    """io_var overrides io_config"""
    io_config = set_io_config(
        '{"a": 3, "b": 4}', ("a=5", "b=6", "a=7", "c=8"), {"a": 1, "b": 2}
    )
    assert io_config == {"a": 7, "b": 6, "c": 8}


def test_sic_decodeerror():
    """Check set_io_config JSON decoding error."""

    with pytest.raises(click.ClickException):
        set_io_config("a=9", (), {})


def test_sic_bad_ioconfig():
    """Check set_io_config with wrong JSON type."""

    with pytest.raises(click.ClickException):
        set_io_config("[10, 11]", (), {})


def test_sic_iovar_no_equals():
    """Test io_var with no equals sign."""

    with pytest.raises(click.ClickException):
        set_io_config(None, ("a",), {})


def test_create_iovar_equals_equals():
    """Test io_var with many equals signs."""

    io_config = set_io_config(None, ("a=12=13=14",), {})
    assert io_config == {"a": "12=13=14"}


def test_sic_coercion():
    """Test coersion of numeric types in set_io_config"""

    io_config = set_io_config('{"a": 15, "b": 16.17}', ("c=18", "d=19.20"), {})
    assert io_config == {"a": 15, "b": 16.17, "c": 18, "d": 19.2}


def test_sic_empty_ioconfig():
    """An empty string as io_config is treated as an empty dict."""

    io_config = set_io_config("", (), {"a": 21, "b": 22})
    assert io_config is None


def test_sic_iovar_del():
    """--io-var VAR= should delete VAR if present in the I/O config."""

    io_config = set_io_config('{"a": 23, "b": 24}', ("a=", "c="), {})
    assert io_config == {"b": 24}


def test_sic_str_default():
    """Test a string default to set_io_config"""

    io_config = set_io_config(None, ("a=25",), '{"a": 26, "b": 27}')
    assert io_config == {"a": 25, "b": 27}


def test_sic_default_decode():
    """Test a decode error in default to set_io_config"""

    with pytest.raises(click.ClickException):
        set_io_config(None, ("a=28",), "rawr")


def test_sic_default_decode_override():
    """Providing io_config avoids the decode error in set_io_config"""

    io_config = set_io_config("", ("a=29",), "rawr")

    assert io_config == {"a": 29}


def test_check_if_from_stdin():
    """Test check_if_from_stdin"""

    assert check_if_from_stdin("path", True, True) is True
    assert check_if_from_stdin("path", True, False) is True
    assert check_if_from_stdin("path", False, True) is False
    assert check_if_from_stdin("path", False, False) is False

    assert check_if_from_stdin("-", True, True) is True
    assert check_if_from_stdin("-", True, False) is True
    assert check_if_from_stdin("-", False, True) is False
    assert check_if_from_stdin("-", False, False) is True
