"""Test CLI: alpenhorn node create"""

import json

from alpenhorn.db import ArchiveFileImportRequest, StorageGroup, StorageNode


def test_no_group(clidb, cli):
    """Test creating a node with no group."""

    cli(2, ["node", "create", "TEST"])

    # No node created
    assert StorageNode.select().count() == 0


def test_group_missing(clidb, cli):
    """Test creating a node with a missing group."""

    cli(1, ["node", "create", "TEST", "--group=MISSING"])

    # No node created
    assert StorageNode.select().count() == 0


def test_group_existing(clidb, cli):
    """Test creating a node and group, but group already exists."""

    # Add the group
    StorageGroup.create(name="TEST")

    cli(1, ["node", "create", "TEST", "--create-group"])

    # Check that no extra group/node was created
    assert StorageGroup.select().count() == 1
    assert StorageNode.select().count() == 0


def test_node_existing(clidb, cli):
    """Test creating a node that already exists."""

    # Add the node
    group = StorageGroup.create(name="GROUP")
    StorageNode.create(name="TEST", group=group)

    cli(1, ["node", "create", "TEST", "--create-group"])

    # Check that no extra group/node was created
    assert StorageGroup.select().count() == 1
    assert StorageNode.select().count() == 1


def test_create_default(clidb, cli):
    """Test creating a node and group with default parameters"""

    cli(0, ["node", "create", "TEST", "--create-group"])

    # Check
    assert StorageGroup.select().count() == 1
    group = StorageGroup.get(name="TEST")
    assert group.io_class is None

    assert StorageNode.select().count() == 1
    node = StorageNode.get(name="TEST")
    assert node.group == group
    assert node.host is None
    assert node.root is None
    assert node.address is None
    assert node.username is None
    assert node.io_class is None
    assert node.io_config is None
    assert node.max_total_gb is None
    assert node.notes is None
    assert not node.auto_import
    assert node.auto_verify == 0
    assert node.min_avail_gb == 0
    assert node.storage_type == "F"


def test_with_group(clidb, cli):
    """Test creating a node with an existing group"""

    group = StorageGroup.create(name="GROUP")

    cli(0, ["node", "create", "TEST", "--group=GROUP"])

    # Check
    assert StorageGroup.select().count() == 1
    node = StorageNode.get(name="TEST")
    assert node.group == group


def test_group_create_group(clidb, cli):
    """Test passing both --group and --create-group"""

    StorageGroup.create(name="GROUP")

    cli(2, ["node", "create", "TEST", "--group=GROUP", "--create-group"])

    assert StorageGroup.select().count() == 1
    assert StorageNode.select().count() == 0


def test_create_ioconifg(clidb, cli):
    """Test create with --io-config."""

    cli(0, ["node", "create", "TEST", "--create-group", '--io-config={"a": 3, "b": 4}'])

    # Check created io_config
    node = StorageNode.get(name="TEST")
    io_config = json.loads(node.io_config)
    assert io_config == {"a": 3, "b": 4}


def test_create_iovar(clidb, cli):
    """Test create with --io-var."""

    cli(
        0,
        [
            "node",
            "create",
            "TEST",
            "--create-group",
            "--io-var=a=3",
            "--io-var",
            "b=4",
            "--io-var=a=5",
        ],
    )

    # Check created io_config
    node = StorageNode.get(name="TEST")
    io_config = json.loads(node.io_config)
    assert io_config == {"a": 5, "b": 4}


def test_create_ioconfig_var(clidb, cli):
    """Test create with --io-config AND --io-var."""

    cli(
        0,
        [
            "node",
            "create",
            "TEST",
            "--create-group",
            '--io-config={"a": 6, "b": 7}',
            "--io-var=a=8",
            "--io-var=c=8.5",
        ],
    )

    # Check created io_config
    node = StorageNode.get(name="TEST")
    io_config = json.loads(node.io_config)
    assert io_config == {"a": 8, "b": 7, "c": 8.5}


def test_archive(clidb, cli):
    """Test creating an archive node"""

    cli(0, ["node", "create", "TEST", "--archive", "--create-group"])

    node = StorageNode.get(name="TEST")
    assert node.storage_type == "A"


def test_field(clidb, cli):
    """Test creating a field node with explicit --field"""

    cli(0, ["node", "create", "TEST", "--field", "--create-group"])

    node = StorageNode.get(name="TEST")
    assert node.storage_type == "F"


def test_transport(clidb, cli):
    """Test creating a transport node"""

    cli(0, ["node", "create", "TEST", "--transport", "--create-group"])

    node = StorageNode.get(name="TEST")
    assert node.storage_type == "T"


def test_multirole(clidb, cli):
    """Test various combinations of multiple role flags."""

    cli(2, ["node", "create", "TEST", "--archive", "--field", "--create-group"])
    cli(2, ["node", "create", "TEST", "--archive", "--transport", "--create-group"])
    cli(2, ["node", "create", "TEST", "--field", "--transport", "--create-group"])
    cli(
        2,
        [
            "node",
            "create",
            "TEST",
            "--archve",
            "--field",
            "--transport",
            "--create-group",
        ],
    )


def test_set_all(clidb, cli):
    """Test setting everyting.

    Well, not io_config.  That's tested elsewhere."""

    cli(
        0,
        [
            "node",
            "create",
            "TEST",
            "--create-group",
            "--activate",
            "--address=ADDRESS",
            "--auto-import",
            "--activate",
            "--auto-verify=2",
            "--class=IOCLASS",
            "--host=HOSTNAME",
            "--max-total=3",
            "--min-avail=4.5",
            "--notes=NOTES",
            "--root=PATH",
            "--username=USERNAME",
        ],
    )

    node = StorageNode.get(name="TEST")
    assert node.active
    assert node.address == "ADDRESS"
    assert node.auto_import
    assert node.auto_verify == 2
    assert node.io_class == "IOCLASS"
    assert node.host == "HOSTNAME"
    assert node.max_total_gb == 3.0
    assert node.min_avail_gb == 4.5
    assert node.notes == "NOTES"
    assert node.root == "PATH"
    assert node.username == "USERNAME"


def test_bad_auto_verify(clidb, cli):
    """Test invalid --auto-verify values."""

    cli(2, ["node", "create", "TEST", "--create-group", "--auto-verify=-1"])
    cli(2, ["node", "create", "TEST", "--create-group", "--auto-verify=Yes, please"])


def test_bad_max_total(clidb, cli):
    """Test invalid --max-total values."""

    cli(2, ["node", "create", "TEST", "--create-group", "--max-total=-1"])
    cli(2, ["node", "create", "TEST", "--create-group", "--max-total=0"])
    cli(2, ["node", "create", "TEST", "--create-group", "--max-total=all"])


def test_bad_min_avail(clidb, cli):
    """Test invalid --min-avail values."""

    cli(2, ["node", "create", "TEST", "--create-group", "--min-avail=-1"])
    cli(2, ["node", "create", "TEST", "--create-group", "--min-avail=none"])


def test_init(clidb, cli):
    """Test create with --init"""

    cli(0, ["node", "create", "TEST", "--init", "--create-group"])

    node = StorageNode.get(name="TEST")
    afir = ArchiveFileImportRequest.get(id=1)

    assert afir.node == node
    assert afir.path == "ALPENHORN_NODE"
