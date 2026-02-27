"""Test CLI: alpenhorn host rename"""

from alpenhorn.db import StorageHost


def test_no_rename(clidb, cli):
    """Test rename on a missing host."""

    cli(1, ["host", "rename", "NAME", "NEWNAME"])

    assert StorageHost.select().count() == 0


def test_rename(clidb, cli):
    """Test renaming a host."""

    # Add the host
    StorageHost.create(name="NAME")

    cli(0, ["host", "rename", "NAME", "NEWNAME"])

    # Check that the rename happened
    assert StorageHost.get(id=1).name == "NEWNAME"


def test_idemrename(clidb, cli):
    """Test renaming a host to it current name."""

    StorageHost.create(name="NAME")

    cli(0, ["host", "rename", "NAME", "NAME"])


def test_rename_exists(clidb, cli):
    """Test renaming a host to an exising name."""

    # Add the hosts
    StorageHost.create(name="NAME")
    StorageHost.create(name="NEWNAME")

    cli(1, ["host", "rename", "NAME", "NEWNAME"])

    # Check that the rename didn't happen
    assert StorageHost.get(id=1).name == "NAME"
    assert StorageHost.get(id=2).name == "NEWNAME"
