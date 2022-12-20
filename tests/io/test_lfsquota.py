"""Test LFSQuotaNodeIO."""

import pytest

from alpenhorn.io.LFSQuota import LFSQuotaNodeIO


@pytest.fixture
def node(simplenode, mock_lfs):
    """A LFSQuota node for testing"""
    simplenode.io_class = "LFSQuota"
    simplenode.io_config = '{"quota_group": "qgroup"}'

    return simplenode


def test_no_quota_group(simplenode):
    """Test instantiating I/O without specifying quota_group."""

    simplenode.io_class = "LFSQuota"

    with pytest.raises(KeyError):
        simplenode.io


def test_no_lfs(simplenode):
    """Test crashing if lfs(1) can't be found."""

    simplenode.io_class = "LFSQuota"
    simplenode.io_config = '{"quota_group": "qgroup"}'

    with pytest.raises(RuntimeError):
        simplenode.io


def test_quota_group(node):
    """Test instantiating I/O."""

    assert isinstance(node.io, LFSQuotaNodeIO)


@pytest.mark.lfs_quota_remaining(1234)
def test_bytes_avail(node):
    """Test LFSQuotaNodeIO.bytes_avail"""

    assert node.io.bytes_avail() == 1234
