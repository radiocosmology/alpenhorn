"""alpenhorn.util tests."""

import pytest

from alpenhorn.common import util


def test_pretty_bytes():
    """Test util.pretty_bytes."""
    with pytest.raises(TypeError):
        util.pretty_bytes({})

    # This is explicitly allowed
    assert util.pretty_bytes(None) == "-"

    # int conversion should happen
    assert util.pretty_bytes("456") == "456 B"

    # This is an overflow
    assert util.pretty_bytes(1234567890123456789012) == "1234567890123456789012 B"

    # Regular behaviour
    assert util.pretty_bytes(123456789012345678901) == "107.1 EiB"
    assert util.pretty_bytes(12345678901234567890) == "10.71 EiB"
    assert util.pretty_bytes(1234567890123456789) == "1.071 EiB"
    assert util.pretty_bytes(123456789012345678) == "109.7 PiB"
    assert util.pretty_bytes(12345678901234567) == "10.97 PiB"
    assert util.pretty_bytes(1234567890123456) == "1.097 PiB"
    assert util.pretty_bytes(123456789012345) == "112.3 TiB"
    assert util.pretty_bytes(12345678901234) == "11.23 TiB"
    assert util.pretty_bytes(1234567890123) == "1.123 TiB"
    assert util.pretty_bytes(123456789012) == "115.0 GiB"
    assert util.pretty_bytes(12345678901) == "11.50 GiB"
    assert util.pretty_bytes(1234567890) == "1.150 GiB"
    assert util.pretty_bytes(123456789) == "117.7 MiB"
    assert util.pretty_bytes(12345678) == "11.77 MiB"
    assert util.pretty_bytes(1234567) == "1.177 MiB"
    assert util.pretty_bytes(123456) == "120.6 kiB"
    assert util.pretty_bytes(12345) == "12.06 kiB"
    assert util.pretty_bytes(1234) == "1.205 kiB"
    assert util.pretty_bytes(1025) == "1.001 kiB"
    assert util.pretty_bytes(1024) == "1.000 kiB"
    assert util.pretty_bytes(1023) == "1023 B"
    assert util.pretty_bytes(123) == "123 B"
    assert util.pretty_bytes(12) == "12 B"
    assert util.pretty_bytes(1) == "1 B"
    assert util.pretty_bytes(0) == "0 B"

    # Also negative sizes are permitted
    assert util.pretty_bytes(-123456789012) == "-115.0 GiB"
    assert util.pretty_bytes(-12345678) == "-11.77 MiB"
    assert util.pretty_bytes(-1234) == "-1.205 kiB"
    assert util.pretty_bytes(-1) == "-1 B"


def test_pretty_deltat():
    """Test util.pretty_deltat."""

    with pytest.raises(TypeError):
        util.pretty_deltat(None)

    assert util.pretty_deltat(1234567) == "342h56m07s"
    assert util.pretty_deltat(123456) == "34h17m36s"
    assert util.pretty_deltat(12345) == "3h25m45s"
    assert util.pretty_deltat(1234) == "20m34s"
    assert util.pretty_deltat(123) == "2m03s"
    assert util.pretty_deltat(12) == "12.0s"
    assert util.pretty_deltat(1) == "1.0s"
    assert util.pretty_deltat(0.1) == "0.1s"
    assert util.pretty_deltat(0.01) == "0.0s"
    assert util.pretty_deltat(0) == "0.0s"
    assert util.pretty_deltat(-1) == "-1.0s"


def test_invalid_import_path():
    """Test invalid_import_path"""

    # Explicitly forbidden names
    assert util.invalid_import_path("") is not None
    assert util.invalid_import_path(".") is not None
    assert util.invalid_import_path("..") is not None

    # Forbidden starts
    assert util.invalid_import_path("/name") is not None
    assert util.invalid_import_path("./name") is not None
    assert util.invalid_import_path("../name") is not None

    # Forbidden ends
    assert util.invalid_import_path("name/") is not None
    assert util.invalid_import_path("name/.") is not None
    assert util.invalid_import_path("name/..") is not None

    # Forbidden middles
    assert util.invalid_import_path("name//name") is not None
    assert util.invalid_import_path("name///name") is not None
    assert util.invalid_import_path("name/./name") is not None
    assert util.invalid_import_path("name/../name") is not None

    # These are fine
    assert util.invalid_import_path("name") is None
    assert util.invalid_import_path("name/name") is None
    assert util.invalid_import_path("name/.../name") is None
