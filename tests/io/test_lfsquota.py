"""Test LFSQuotaNodeIO."""

import pytest

from alpenhorn.io.LFSQuota import LFSQuotaNodeIO


@pytest.fixture
def node(genericnode, mock_lfs):
    """A LFSQuota node for testing"""
    genericnode.io_class = "LFSQuota"
    genericnode.io_config = '{"quota_group": "qgroup"}'

    return genericnode


def test_no_quota_group(genericnode):
    """Test instantiating I/O without specifying quota_group."""

    genericnode.io_class = "LFSQuota"

    with pytest.raises(KeyError):
        genericnode.io


def test_no_lfs(genericnode):
    """Test crashing if lfs(1) can't be found."""

    genericnode.io_class = "LFSQuota"
    genericnode.io_config = '{"quota_group": "qgroup"}'

    with pytest.raises(RuntimeError):
        genericnode.io


def test_quota_group(node):
    """Test instantiating I/O."""

    assert isinstance(node.io, LFSQuotaNodeIO)


@pytest.mark.lfs_quota_remaining(1234)
def test_bytes_avail(node):
    """Test LFSQuotaNodeIO.bytes_avail"""

    assert node.io.bytes_avail() == 1234
