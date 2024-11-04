"""Test CLI: alpenhorn node rename"""

import json
import pytest
from alpenhorn.db import StorageNode, StorageGroup


def test_no_rename(clidb, client):
    """Test rename on a missing node."""

    client(1, ["node", "rename", "NAME", "NEWNAME"])

    assert StorageNode.select().count() == 0


def test_rename(clidb, client):
    """Test renaming a node."""

    # Add the node
    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="NAME", group=group)

    client(0, ["node", "rename", "NAME", "NEWNAME"])

    # Check that the rename happened
    assert StorageNode.get(id=1).name == "NEWNAME"


def test_idemrename(clidb, client):
    """Test renaming a node to it current name."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="NAME", group=group)

    client(0, ["node", "rename", "NAME", "NAME"])


def test_rename_exists(clidb, client):
    """Test renaming a node to an exising name."""

    # Add the nodes
    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="NAME", group=group)
    StorageNode.create(name="NEWNAME", group=group)

    client(1, ["node", "rename", "NAME", "NEWNAME"])

    # Check that the rename didn't happen
    assert StorageNode.get(id=1).name == "NAME"
    assert StorageNode.get(id=2).name == "NEWNAME"
