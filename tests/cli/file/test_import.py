"""Test CLI: alpenhorn file import"""

from alpenhorn.db import ArchiveFileImportRequest, StorageGroup, StorageNode, utcnow


def test_bad_node(clidb, cli):
    """Test a bad node."""

    cli(1, ["file", "import", "path", "MISSING"])


def test_abspath(clidb, cli):
    """Test an absolute path."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(2, ["file", "import", "/path", "Node"])


def test_improt(clidb, cli):
    """Test a normal import."""

    before = utcnow().replace(microsecond=0)

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    cli(0, ["file", "import", "path", "Node"])

    # Request created
    afir = ArchiveFileImportRequest.get(id=1)
    assert afir.node == node
    assert afir.path == "path"
    assert not afir.recurse
    assert not afir.register
    assert not afir.completed
    assert afir.timestamp >= before


def test_register(clidb, cli):
    """Test import --register-new."""

    before = utcnow().replace(microsecond=0)

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    cli(0, ["file", "import", "path", "Node", "--register-new"])

    # Request created
    afir = ArchiveFileImportRequest.get(id=1)
    assert afir.node == node
    assert afir.path == "path"
    assert not afir.recurse
    assert afir.register
    assert not afir.completed
    assert afir.timestamp >= before
