"""Test DefaultNodeRemote."""

import pytest

from alpenhorn.daemon.update import RemoteNode


def test_file_addr(simplehost, simplenode, simplefile):
    """Test DefaultNodeRemote.file_addr"""

    # No host
    with pytest.raises(ValueError):
        assert RemoteNode(simplenode).io.file_addr(simplefile) is None

    simplenode.host = simplehost

    # Host has set neither address nor user
    with pytest.raises(ValueError):
        assert RemoteNode(simplenode).io.file_addr(simplefile) is None

    simplehost.address = None
    simplehost.username = "user"

    with pytest.raises(ValueError):
        assert RemoteNode(simplenode).io.file_addr(simplefile) is None

    simplehost.address = "addr"
    simplehost.username = None

    with pytest.raises(ValueError):
        assert RemoteNode(simplenode).io.file_addr(simplefile) is None

    simplehost.address = "addr"
    simplehost.username = "user"

    assert (
        RemoteNode(simplenode).io.file_addr(simplefile)
        == "user@addr:/node/simplefile_acq/simplefile"
    )


def test_file_path(simplecopy):
    """Test DefaultNodeRemote.file_name"""

    assert RemoteNode(simplecopy.node).io.file_path(simplecopy.file) == str(
        simplecopy.path
    )
