"""alpenhorn.util tests."""
import pytest

from alpenhorn import util


def test_gethostname_config(hostname):
    """Test util.get_hostname with config"""

    assert util.get_hostname() == hostname


def test_gethostname_default():
    """Test util.get_hostname with no config"""
    host = util.get_hostname()
    assert "." not in host
    assert len(host) > 0


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
