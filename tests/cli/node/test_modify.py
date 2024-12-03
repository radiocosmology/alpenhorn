"""Test CLI: alpenhorn node modify"""

import json

import pytest

from alpenhorn.db import StorageGroup, StorageNode


def test_no_node(clidb, cli):
    """Test modifying a non-existent node."""

    cli(1, ["node", "modify", "TEST"])


def test_modify_nothing(clidb, cli):
    """Test modifying a node without modifying anything.

    This is pointless but explicitly allowed."""

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

    # Re-get so defaults are set.
    node_in = StorageNode.get(name="TEST")

    cli(0, ["node", "modify", "TEST"])

    # Check
    node_out = StorageNode.get(name="TEST")
    assert node_in.__data__ == node_out.__data__


def test_modify_all(clidb, cli):
    """Test modifying everything.

    Well, most of the things..."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(
        name="TEST",
        group=group,
        address="ADDR",
        host="HOST",
        io_class="IOCLASS",
        max_total_gb=1.0,
        min_avail_gb=2.0,
        notes="NOTES",
        root="ROOT",
        username="USER",
    )
    newgroup = StorageGroup.create(name="NEWGROUP")

    cli(
        0,
        [
            "node",
            "modify",
            "TEST",
            "--group=NEWGROUP",
            "--address=NEWADDR",
            "--host=NEWHOST",
            "--class=NEWIOCLASS",
            "--max-total=4",
            "--min-avail=3",
            "--notes=NEWNOTES",
            "--root=NEWROOT",
            "--username=NEWUSER",
        ],
    )

    # Check
    node = StorageNode.get(name="TEST")
    assert node.group == newgroup
    assert node.address == "NEWADDR"
    assert node.host == "NEWHOST"
    assert node.io_class == "NEWIOCLASS"
    assert node.max_total_gb == 4
    assert node.min_avail_gb == 3
    assert node.notes == "NEWNOTES"
    assert node.root == "NEWROOT"
    assert node.username == "NEWUSER"


def test_clear_all(clidb, cli):
    """Test clearing everything."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(
        name="TEST",
        group=group,
        address="ADDR",
        host="HOST",
        io_class="IOCLASS",
        max_total_gb=1.0,
        notes="NOTES",
        root="ROOT",
        username="USER",
    )

    cli(
        0,
        [
            "node",
            "modify",
            "TEST",
            "--address=",
            "--host=",
            "--class=",
            "--no-max-total",
            "--notes=",
            "--root=",
            "--username=",
        ],
    )

    # Check
    node = StorageNode.get(name="TEST")
    assert node.address is None
    assert node.host is None
    assert node.io_class is None
    assert node.max_total_gb is None
    assert node.notes is None
    assert node.root is None
    assert node.username is None


def test_modify_ioconfig(clidb, cli):
    """Test updating I/O config with modify."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group, io_config='{"a": 1, "b": 2, "c": 3}')

    cli(
        0,
        [
            "node",
            "modify",
            "TEST",
            '--io-config={"a": 4, "b": 5, "c": 6, "d": 7}',
            "--io-var",
            "b=8",
            "--io-var=c=",
        ],
    )

    node = StorageNode.get(name="TEST")
    assert json.loads(node.io_config) == {"a": 4, "b": 8, "d": 7}


def test_modify_iovar_bad_json(clidb, cli):
    """--io-var can't be used if the existing I/O config is invalid."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group, io_config="rawr")

    # Verify the I/O config is invalid
    node = StorageNode.get(name="TEST")
    with pytest.raises(json.JSONDecodeError):
        json.loads(node.io_config)

    cli(1, ["node", "modify", "TEST", "--io-var=a=9"])

    # I/O config is still broken
    node = StorageNode.get(name="TEST")
    with pytest.raises(json.JSONDecodeError):
        json.loads(node.io_config)


def test_modify_fix_json(clidb, cli):
    """--io-config can be used to replace invalid JSON in the database."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group, io_config="rawr")

    # Verify the I/O config is invalid
    node = StorageNode.get(name="TEST")
    with pytest.raises(json.JSONDecodeError):
        json.loads(node.io_config)

    cli(0, ["node", "modify", "TEST", '--io-config={"a": 10}'])

    # Client has fixed the I/O config
    node = StorageNode.get(name="TEST")
    assert json.loads(node.io_config) == {"a": 10}


def test_max_total_no_max_total(clidb, cli):
    """Can't specify both --max-total and --no-max-total"""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group)

    cli(2, ["node", "modify", "TEST", "--max-total=4", "--no-max-total"])


def test_bad_max_total(clidb, cli):
    """--max-total must be positive."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group)

    cli(2, ["node", "modify", "TEST", "--max-total=0"])
    cli(2, ["node", "modify", "TEST", "--max-total=-1"])


def test_bad_auto_verify(clidb, cli):
    """--auto-verify must be non-negative."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group, auto_verify=3)

    cli(2, ["node", "modify", "TEST", "--auto-verify=-3"])

    node = StorageNode.get(name="TEST")
    assert node.auto_verify == 3

    cli(0, ["node", "modify", "TEST", "--auto-verify=0"])

    node = StorageNode.get(name="TEST")
    assert node.auto_verify == 0


def test_bad_min_avail(clidb, cli):
    """--min-avail must be non-negative."""

    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group, min_avail_gb=3)

    cli(2, ["node", "modify", "TEST", "--min-avail=-3"])
    cli(0, ["node", "modify", "TEST", "--min-avail=0"])

    node = StorageNode.get(name="TEST")
    assert node.min_avail_gb == 0
