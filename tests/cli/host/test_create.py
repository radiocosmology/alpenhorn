"""Test CLI: alpenhorn host create"""

from alpenhorn.db import StorageHost


def test_create(clidb, cli):
    """Test creating a host."""

    cli(
        0,
        [
            "host",
            "create",
            "TEST",
            "--address=Addr",
            "--notes=Notes",
            "--username=User",
        ],
    )

    # Check created host
    host = StorageHost.get(name="TEST")
    assert host.address == "Addr"
    assert host.notes == "Notes"
    assert host.username == "User"


def test_default(clidb, cli):
    """Test creating a host with defaults."""

    cli(0, ["host", "create", "TEST"])

    # Check created host
    host = StorageHost.get(name="TEST")
    assert host.address is None
    assert host.notes is None
    assert host.username is None


def test_create_existing(clidb, cli):
    """Test creating a host that already exists."""

    # Add the host
    StorageHost.create(name="TEST")

    cli(1, ["host", "create", "TEST", "--notes=Notes"])

    # Check that no extra hoshost was created
    assert StorageHost.select().count() == 1
