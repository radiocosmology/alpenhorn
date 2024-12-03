"""Test CLI: alpenhorn node activate"""

from alpenhorn.db import StorageGroup, StorageNode


def test_no_node(clidb, cli):
    """Test activating a non-existent node."""

    cli(1, ["node", "activate", "TEST"])


def test_activate_default(clidb, cli):
    """Test activating a node without changing other parameters."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(
        name="TEST",
        group=group,
        active=False,
        address="ADDR",
        host="HOST",
        root="ROOT",
        username="USER",
    )

    cli(0, ["node", "activate", "TEST"])

    # Check
    node = StorageNode.get(name="TEST")
    assert node.active
    assert node.host == "HOST"
    assert node.root == "ROOT"
    assert node.address == "ADDR"
    assert node.username == "USER"


def test_activate_set(clidb, cli):
    """Test activating a node and setting other parameters."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(
        name="TEST",
        group=group,
        active=False,
        address="ADDR",
        host="HOST",
        root="ROOT",
        username="USER",
    )

    cli(
        0,
        [
            "node",
            "activate",
            "TEST",
            "--address=NEWADDR",
            "--host=NEWHOST",
            "--root=NEWROOT",
            "--username=NEWUSER",
        ],
    )

    # Check
    node = StorageNode.get(name="TEST")
    assert node.active
    assert node.host == "NEWHOST"
    assert node.root == "NEWROOT"
    assert node.address == "NEWADDR"
    assert node.username == "NEWUSER"


def test_activate_clear(clidb, cli):
    """Test activating a node and clearing other parameters."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(
        name="TEST",
        group=group,
        active=False,
        address="ADDR",
        host="HOST",
        root="ROOT",
        username="USER",
    )

    cli(
        0,
        [
            "node",
            "activate",
            "TEST",
            "--address=",
            "--host=",
            "--root=",
            "--username=",
        ],
    )

    # Check
    node = StorageNode.get(name="TEST")
    assert node.active
    assert node.host is None
    assert node.root is None
    assert node.address is None
    assert node.username is None


def test_already_active(clidb, cli):
    """Test activating a node that is already active."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(
        name="TEST",
        group=group,
        active=True,
        address="ADDR",
        host="HOST",
        root="ROOT",
        username="USER",
    )

    cli(
        0,
        [
            "node",
            "activate",
            "TEST",
            "--address=NEWADDR",
            "--host=NEWHOST",
            "--root=NEWROOT",
            "--username=NEWUSER",
        ],
    )

    # None of the parameters were updated
    node = StorageNode.get(name="TEST")
    assert node.active
    assert node.host == "HOST"
    assert node.root == "ROOT"
    assert node.address == "ADDR"
    assert node.username == "USER"
