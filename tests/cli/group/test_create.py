"""Test CLI: alpenhorn group create"""

import json

from alpenhorn.db import StorageGroup


def test_create(clidb, cli):
    """Test creating a group."""

    cli(0, ["group", "create", "TEST", "--notes=Notes"])

    # Check created group
    group = StorageGroup.get(name="TEST")
    assert group.notes == "Notes"
    assert group.io_class == "Default"
    assert group.io_config is None


def test_create_existing(clidb, cli):
    """Test creating a group that already exists."""

    # Add the group
    StorageGroup.create(name="TEST")

    cli(1, ["group", "create", "TEST", "--notes=Notes"])

    # Check that no extra group was created
    assert StorageGroup.select().count() == 1


def test_create_ioconifg(clidb, cli):
    """Test create with --io-config."""

    cli(0, ["group", "create", "TEST", '--io-config={"a": 3, "b": 4}'])

    # Check created io_config
    group = StorageGroup.get(name="TEST")
    io_config = json.loads(group.io_config)
    assert io_config == {"a": 3, "b": 4}


def test_create_iovar(clidb, cli):
    """Test create with --io-var."""

    cli(
        0,
        ["group", "create", "TEST", "--io-var=a=3", "--io-var", "b=4", "--io-var=a=5"],
    )

    # Check created io_config
    group = StorageGroup.get(name="TEST")
    io_config = json.loads(group.io_config)
    assert io_config == {"a": 5, "b": 4}


def test_create_ioconfig_var(clidb, cli):
    """Test create with --io-config AND --io-var."""

    cli(
        0,
        [
            "group",
            "create",
            "TEST",
            '--io-config={"a": 6, "b": 7}',
            "--io-var=a=8",
            "--io-var=c=8.5",
        ],
    )

    # Check created io_config
    group = StorageGroup.get(name="TEST")
    io_config = json.loads(group.io_config)
    assert io_config == {"a": 8, "b": 7, "c": 8.5}
