"""Test LustreQuotaNodeIO."""

import pytest

from alpenhorn.daemon.update import UpdateableNode
from alpenhorn.io.lustrequota import LustreQuotaNodeIO


@pytest.fixture
def node(simplenode, mock_lfs):
    """A LustreQuota node for testing"""
    simplenode.io_class = "LustreQuota"
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "group"}'

    return UpdateableNode(None, simplenode)


def test_no_quota_id(simplenode):
    """Test instantiating I/O without specifying quota_id."""

    simplenode.io_class = "LustreQuota"

    with pytest.raises(KeyError):
        UpdateableNode(None, simplenode)


def test_no_lfs(simplenode):
    """Test crashing if lfs(1) can't be found."""

    simplenode.io_class = "LustreQuota"
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "group"}'

    with pytest.raises(RuntimeError):
        UpdateableNode(None, simplenode)


def test_quota_id(node):
    """Test instantiating I/O."""

    assert isinstance(node.io, LustreQuotaNodeIO)


@pytest.mark.lfs_quota_remaining(1234)
def test_bytes_avail(node):
    """Test LustreQuotaNodeIO.bytes_avail"""

    assert node.io.bytes_avail() == 1234


@pytest.mark.lfs_dont_mock("quota_remaining")
def test_quota_type(mock_lfs, simplenode, mock_run_command):
    """Test quota_type handling."""

    simplenode.io_class = "LustreQuota"

    # User quota
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "user"}'
    unode = UpdateableNode(None, simplenode)
    unode.io.bytes_avail()
    assert "-u qid" in " ".join(mock_run_command()["cmd"])

    # Group quota
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "group"}'
    unode = UpdateableNode(None, simplenode)
    unode.io.bytes_avail()
    assert "-g qid" in " ".join(mock_run_command()["cmd"])

    # Project quota
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "project"}'
    unode = UpdateableNode(None, simplenode)
    unode.io.bytes_avail()
    assert "-p qid" in " ".join(mock_run_command()["cmd"])


def test_bad_quota_type(mock_lfs, simplenode):
    """Test bad quota_type handling."""

    simplenode.io_class = "LustreQuota"
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "pool"}'
    with pytest.raises(ValueError):
        UpdateableNode(None, simplenode)
