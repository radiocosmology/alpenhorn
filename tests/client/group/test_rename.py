"""Test CLI: alpenhorn group rename"""

import json
import pytest
from alpenhorn.db import StorageGroup


def test_no_rename(clidb, client):
    """Test rename on a missing group."""

    client(1, ["group", "rename", "NAME", "NEWNAME"])

    assert StorageGroup.select().count() == 0


def test_rename(clidb, client):
    """Test renaming a group."""

    # Add the group
    StorageGroup.create(name="NAME")

    client(0, ["group", "rename", "NAME", "NEWNAME"])

    # Check that the rename happened
    assert StorageGroup.get(id=1).name == "NEWNAME"


def test_idemrename(clidb, client):
    """Test renaming a group to it current name."""

    StorageGroup.create(name="NAME")

    client(0, ["group", "rename", "NAME", "NAME"])


def test_rename_exists(clidb, client):
    """Test renaming a group to an exising name."""

    # Add the groups
    StorageGroup.create(name="NAME")
    StorageGroup.create(name="NEWNAME")

    client(1, ["group", "rename", "NAME", "NEWNAME"])

    # Check that the rename didn't happen
    assert StorageGroup.get(id=1).name == "NAME"
    assert StorageGroup.get(id=2).name == "NEWNAME"
