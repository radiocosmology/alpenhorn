"""alpenhorn.util tests."""

from alpenhorn import util


def test_gethostname_config(hostname):
    """Test util.get_hostname with config"""

    assert util.get_hostname() == hostname


def test_gethostname_default():
    """Test util.get_hostname with no config"""
    host = util.get_hostname()
    assert "." not in host
    assert len(host) > 0
