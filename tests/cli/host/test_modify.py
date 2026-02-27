"""Test CLI: alpenhorn host modify"""

from alpenhorn.db import StorageHost


def test_no_modify(clidb, cli):
    """Test modifying a non-existent host."""

    cli(1, ["host", "modify", "TEST", "--notes=Notes"])

    # Still nothing.
    assert StorageHost.select().count() == 0


def test_modify_empty(clidb, cli):
    """Test not modifing a host."""

    # Add the host
    StorageHost.create(name="TEST", notes="Note")

    # Do nothing successfully.
    cli(0, ["host", "modify", "TEST"])


def test_modify_no_change(clidb, cli):
    """Test modify with no change."""

    StorageHost.create(name="TEST", notes="Note", io_class=None)

    cli(0, ["host", "modify", "TEST", "--notes=Note"])

    assert StorageHost.get(name="TEST").notes == "Note"


def test_modify(clidb, cli):
    """Test modify."""

    StorageHost.create(name="TEST", notes="Note")

    cli(
        0,
        [
            "host",
            "modify",
            "TEST",
            "--notes=New Note",
            "--address=Addr",
            "--username=User",
        ],
    )

    host = StorageHost.get(name="TEST")
    assert host.notes == "New Note"
    assert host.address == "Addr"
    assert host.username == "User"


def test_modify_delete(clidb, cli):
    """Test deleting metadata with modify."""

    StorageHost.create(name="TEST", notes="Note", address="Addr", username="User")

    cli(0, ["host", "modify", "TEST", "--notes=", "--address=", "--username="])

    host = StorageHost.get(name="TEST")
    assert host.notes is None
    assert host.address is None
    assert host.username is None
