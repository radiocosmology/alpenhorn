"""Test LustreQuotaNodeIO."""

import pytest

from alpenhorn.daemon.update import UpdateableNode
from alpenhorn.io.lustrequota import LustreQuotaNodeIO


@pytest.fixture
def node(simplenode, mock_lfs):
    """A LustreQuota node for testing"""
    simplenode.io_class = "LustreQuota"
    simplenode.io_config = '{"quota_group": "qgroup"}'

    return UpdateableNode(None, simplenode)


def test_no_quota_group(simplenode):
    """Test instantiating I/O without specifying quota_group."""

    simplenode.io_class = "LustreQuota"

    with pytest.raises(KeyError):
        UpdateableNode(None, simplenode)


def test_no_lfs(simplenode):
    """Test crashing if lfs(1) can't be found."""

    simplenode.io_class = "LustreQuota"
    simplenode.io_config = '{"quota_group": "qgroup"}'

    with pytest.raises(RuntimeError):
        UpdateableNode(None, simplenode)


def test_quota_group(node):
    """Test instantiating I/O."""

    assert isinstance(node.io, LustreQuotaNodeIO)


@pytest.mark.lfs_quota_remaining(1234)
def test_bytes_avail(node):
    """Test LustreQuotaNodeIO.bytes_avail"""

    assert node.io.bytes_avail() == 1234
