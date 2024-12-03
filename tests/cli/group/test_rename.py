"""Test CLI: alpenhorn group rename"""

from alpenhorn.db import StorageGroup


def test_no_rename(clidb, cli):
    """Test rename on a missing group."""

    cli(1, ["group", "rename", "NAME", "NEWNAME"])

    assert StorageGroup.select().count() == 0


def test_rename(clidb, cli):
    """Test renaming a group."""

    # Add the group
    StorageGroup.create(name="NAME")

    cli(0, ["group", "rename", "NAME", "NEWNAME"])

    # Check that the rename happened
    assert StorageGroup.get(id=1).name == "NEWNAME"


def test_idemrename(clidb, cli):
    """Test renaming a group to it current name."""

    StorageGroup.create(name="NAME")

    cli(0, ["group", "rename", "NAME", "NAME"])


def test_rename_exists(clidb, cli):
    """Test renaming a group to an exising name."""

    # Add the groups
    StorageGroup.create(name="NAME")
    StorageGroup.create(name="NEWNAME")

    cli(1, ["group", "rename", "NAME", "NEWNAME"])

    # Check that the rename didn't happen
    assert StorageGroup.get(id=1).name == "NAME"
    assert StorageGroup.get(id=2).name == "NEWNAME"
