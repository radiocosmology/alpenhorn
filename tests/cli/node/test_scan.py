"""Test CLI: alpenhorn node scan"""

from alpenhorn.db import ArchiveFileImportRequest, StorageGroup, StorageNode


def test_no_node(clidb, cli):
    """Test a missing NODE name"""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["node", "scan"])


def test_bad_node(clidb, cli):
    """Test a bad node name"""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["node", "scan", "MISSING"])


def test_default(clidb, cli):
    """Test the default invocation."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, root="/root")

    cli(0, ["node", "scan", "Node"])

    # Request created
    afir = ArchiveFileImportRequest.get(id=1)

    assert afir.node == node
    assert afir.path == "."
    assert afir.recurse == 1
    assert afir.register == 0


def test_relpath(clidb, cli):
    """Test a relative PATH."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, root="/root")

    cli(0, ["node", "scan", "Node", "relative/path"])

    # Request created
    afir = ArchiveFileImportRequest.get(id=1)

    assert afir.node == node
    assert afir.path == "relative/path"
    assert afir.recurse == 1
    assert afir.register == 0


def test_abspath(clidb, cli):
    """Test an absolute PATH."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, root="/root")

    cli(0, ["node", "scan", "Node", "/root/absolute/path"])

    # Request created
    afir = ArchiveFileImportRequest.get(id=1)

    assert afir.node == node
    assert afir.path == "absolute/path"
    assert afir.recurse == 1
    assert afir.register == 0


def test_bad_abspath(clidb, cli):
    """Test an invalid absolute PATH."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group, root="/root")

    cli(1, ["node", "scan", "Node", "/absolute/path"])

    # No request created
    assert ArchiveFileImportRequest.select().count() == 0


def test_abspath_no_root(clidb, cli):
    """Test an absolute PATH but NODE has no root."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group, root=None)

    cli(1, ["node", "scan", "Node", "/absolute/path"])

    # No request created
    assert ArchiveFileImportRequest.select().count() == 0


def test_new(clidb, cli):
    """Test --register-new."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group, root=None)

    cli(0, ["node", "scan", "Node", "--register-new"])

    # Request created
    afir = ArchiveFileImportRequest.get(id=1)

    assert afir.node == node
    assert afir.path == "."
    assert afir.recurse == 1
    assert afir.register == 1
