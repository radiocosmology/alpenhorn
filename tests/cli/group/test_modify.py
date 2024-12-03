"""Test CLI: alpenhorn group modify"""

import json

import pytest

from alpenhorn.db import StorageGroup


def test_no_modify(clidb, cli):
    """Test modifying a non-existent group."""

    cli(1, ["group", "modify", "TEST", "--notes=Notes"])

    # Still nothing.
    assert StorageGroup.select().count() == 0


def test_modify_empty(clidb, cli):
    """Test not modifing a group."""

    # Add the group
    StorageGroup.create(name="TEST", notes="Note", io_class=None)

    # Do nothing successfully.
    cli(0, ["group", "modify", "TEST"])


def test_modify_no_change(clidb, cli):
    """Test modify with no change."""

    StorageGroup.create(name="TEST", notes="Note", io_class=None)

    cli(0, ["group", "modify", "TEST", "--notes=Note"])

    assert StorageGroup.get(name="TEST").notes == "Note"


def test_modify(clidb, cli):
    """Test modify."""

    StorageGroup.create(name="TEST", notes="Note", io_class=None)

    cli(0, ["group", "modify", "TEST", "--notes=New Note", "--class=NewClass"])

    group = StorageGroup.get(name="TEST")
    assert group.notes == "New Note"
    assert group.io_class == "NewClass"


def test_modify_delete(clidb, cli):
    """Test deleting metadata with modify."""

    StorageGroup.create(name="TEST", notes="Note", io_class=None)

    cli(0, ["group", "modify", "TEST", "--notes=", "--class="])

    group = StorageGroup.get(name="TEST")
    assert group.notes is None
    assert group.io_class is None


def test_modify_ioconfig(clidb, cli):
    """Test updating I/O config with modify."""

    StorageGroup.create(name="TEST", io_config='{"a": 1, "b": 2, "c": 3}')

    cli(
        0,
        [
            "group",
            "modify",
            "TEST",
            '--io-config={"a": 4, "b": 5, "c": 6, "d": 7}',
            "--io-var",
            "b=8",
            "--io-var=c=",
        ],
    )

    group = StorageGroup.get(name="TEST")
    assert json.loads(group.io_config) == {"a": 4, "b": 8, "d": 7}


def test_modify_iovar_bad_json(clidb, cli):
    """--io-var can't be used if the existing I/O config is invalid."""

    StorageGroup.create(name="TEST", io_config="rawr")

    # Verify the I/O config is invalid
    group = StorageGroup.get(name="TEST")
    with pytest.raises(json.JSONDecodeError):
        json.loads(group.io_config)

    cli(1, ["group", "modify", "TEST", "--io-var=a=9"])

    # I/O config is still broken
    group = StorageGroup.get(name="TEST")
    with pytest.raises(json.JSONDecodeError):
        json.loads(group.io_config)


def test_modify_fix_json(clidb, cli):
    """--io-config can be used to replace invalid JSON in the database."""

    StorageGroup.create(name="TEST", io_config="rawr")

    # Verify the I/O config is invalid
    group = StorageGroup.get(name="TEST")
    with pytest.raises(json.JSONDecodeError):
        json.loads(group.io_config)

    cli(0, ["group", "modify", "TEST", '--io-config={"a": 10}'])

    # We have fixed the I/O config
    group = StorageGroup.get(name="TEST")
    assert json.loads(group.io_config) == {"a": 10}
