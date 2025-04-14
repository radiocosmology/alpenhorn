"""Test CLI: alpenhorn group list"""

from alpenhorn.db import StorageGroup


def test_schema_mismatch(clidb, cli, cli_wrong_schema):
    """Test schema mismatch."""

    cli(1, ["group", "list"])


def test_list(clidb, cli):
    """Test listing groups."""

    # Make some StorageGroups to list
    StorageGroup.create(name="Group1", notes="Note1")
    StorageGroup.create(name="Group2", notes="Note2", io_class="Class2")

    result = cli(0, ["group", "list"])
    assert "Group1" in result.output
    assert "Group2" in result.output
    assert "Note1" in result.output
    assert "Note2" in result.output
    assert "Default" in result.output
    assert "Class2" in result.output


def test_no_list(clidb, cli):
    """Test listing no groups."""

    result = cli(0, ["group", "list"])

    # i.e. the header hasn't been printed
    assert "I/O class" not in result.output
