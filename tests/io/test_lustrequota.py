"""Test LustreQuotaNodeIO."""

import pytest

from alpenhorn.daemon.update import UpdateableNode
from alpenhorn.io.lustrequota import LustreQuotaNodeIO


@pytest.fixture
def node(simplenode, queue, mock_lfs):
    """A LustreQuota node for testing"""
    simplenode.io_class = "LustreQuota"
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "group"}'

    return UpdateableNode(queue, simplenode)


def test_no_quota_id(simplenode, queue):
    """Test instantiating I/O without specifying quota_id."""

    simplenode.io_class = "LustreQuota"

    with pytest.raises(KeyError):
        UpdateableNode(queue, simplenode)


def test_no_lfs(simplenode, queue):
    """Test crashing if lfs(1) can't be found."""

    simplenode.io_class = "LustreQuota"
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "group"}'

    with pytest.raises(RuntimeError):
        UpdateableNode(queue, simplenode)


def test_quota_id(node):
    """Test instantiating I/O."""

    assert isinstance(node.io, LustreQuotaNodeIO)


@pytest.mark.lfs_quota_remaining(1234)
def test_bytes_avail(node):
    """Test LustreQuotaNodeIO.bytes_avail"""

    assert node.io.bytes_avail() == 1234


@pytest.mark.lfs_dont_mock("quota_remaining")
def test_quota_type(mock_lfs, queue, simplenode, mock_run_command):
    """Test quota_type handling."""

    simplenode.io_class = "LustreQuota"

    # User quota
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "user"}'
    unode = UpdateableNode(queue, simplenode)
    unode.io.bytes_avail()
    assert "-u qid" in " ".join(mock_run_command()["cmd"])

    # Group quota
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "group"}'
    unode = UpdateableNode(queue, simplenode)
    unode.io.bytes_avail()
    assert "-g qid" in " ".join(mock_run_command()["cmd"])

    # Project quota
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "project"}'
    unode = UpdateableNode(queue, simplenode)
    unode.io.bytes_avail()
    assert "-p qid" in " ".join(mock_run_command()["cmd"])


def test_bad_quota_type(mock_lfs, queue, simplenode):
    """Test bad quota_type handling."""

    simplenode.io_class = "LustreQuota"
    simplenode.io_config = '{"quota_id": "qid", "quota_type": "pool"}'
    with pytest.raises(ValueError):
        UpdateableNode(queue, simplenode)
