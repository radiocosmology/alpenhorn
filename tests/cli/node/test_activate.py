"""Test CLI: alpenhorn node activate"""

from alpenhorn.db import StorageGroup, StorageHost, StorageNode


def test_no_node(clidb, cli):
    """Test activating a non-existent node."""

    cli(1, ["node", "activate", "TEST"])


def test_activate_default(clidb, cli):
    """Test activating a node without changing other parameters."""

    group = StorageGroup.create(name="GROUP")
    host = StorageHost.create(name="HOST")
    StorageNode.create(
        name="TEST",
        group=group,
        active=False,
        host=host,
        root="ROOT",
    )

    cli(0, ["node", "activate", "TEST"])

    # Check
    node = StorageNode.get(name="TEST")
    assert node.active
    assert node.host == host
    assert node.root == "ROOT"


def test_activate_no_host(clidb, cli):
    """Test trying to use a non-existent host."""

    group = StorageGroup.create(name="GROUP")
    host = StorageHost.create(name="HOST")
    StorageNode.create(
        name="TEST",
        group=group,
        active=False,
        host=host,
        root="ROOT",
    )

    cli(1, ["node", "activate", "TEST", "--host=NEWHOST"])


def test_activate_set(clidb, cli):
    """Test activating a node and setting other parameters."""

    group = StorageGroup.create(name="GROUP")
    host = StorageHost.create(name="HOST")
    StorageHost.create(name="NEWHOST")
    StorageNode.create(
        name="TEST",
        group=group,
        active=False,
        host=host,
        root="ROOT",
    )

    cli(
        0,
        [
            "node",
            "activate",
            "TEST",
            "--host=NEWHOST",
            "--root=NEWROOT",
        ],
    )

    # Check
    node = StorageNode.get(name="TEST")
    assert node.active
    assert node.host.name == "NEWHOST"
    assert node.root == "NEWROOT"


def test_activate_clear(clidb, cli):
    """Test activating a node and clearing other parameters."""

    host = StorageHost.create(name="HOST")
    group = StorageGroup.create(name="GROUP")
    StorageNode.create(
        name="TEST",
        group=group,
        active=False,
        host=host,
        root="ROOT",
    )

    cli(
        0,
        [
            "node",
            "activate",
            "TEST",
            "--host=",
            "--root=",
        ],
    )

    # Check
    node = StorageNode.get(name="TEST")
    assert node.active
    assert node.host is None
    assert node.root is None


def test_already_active(clidb, cli):
    """Test activating a node that is already active."""

    host = StorageHost.create(name="HOST")
    StorageHost.create(name="NEWHOST")
    group = StorageGroup.create(name="GROUP")
    StorageNode.create(
        name="TEST",
        group=group,
        active=True,
        host=host,
        root="ROOT",
    )

    cli(
        0,
        [
            "node",
            "activate",
            "TEST",
            "--host=NEWHOST",
            "--root=NEWROOT",
        ],
    )

    # None of the parameters were updated
    node = StorageNode.get(name="TEST")
    assert node.active
    assert node.host == host
    assert node.root == "ROOT"
