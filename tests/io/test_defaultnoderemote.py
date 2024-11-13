"""Test DefaultNodeRemote."""

import pytest

from alpenhorn.daemon.update import RemoteNode


def test_file_addr(simplenode, simplefile):
    """Test DefaultNodeRemote.file_addr"""

    simplenode.address = None
    simplenode.username = "user"

    with pytest.raises(ValueError):
        assert RemoteNode(simplenode).io.file_addr(simplefile) is None

    simplenode.address = "addr"
    simplenode.username = None

    with pytest.raises(ValueError):
        assert RemoteNode(simplenode).io.file_addr(simplefile) is None

    simplenode.address = "addr"
    simplenode.username = "user"

    assert (
        RemoteNode(simplenode).io.file_addr(simplefile)
        == "user@addr:/node/simplefile_acq/simplefile"
    )


def test_file_path(simplecopy):
    """Test DefaultNodeRemote.file_name"""

    assert RemoteNode(simplecopy.node).io.file_path(simplecopy.file) == str(
        simplecopy.path
    )
