"""
test_client_group
----------------------------------

Tests for `alpenhorn.client.group` module.
"""

import re

import pytest
from click.testing import CliRunner

import alpenhorn.client as cli
import alpenhorn.db as db
import alpenhorn.storage as st
import test_import as ti

# XXX: client is broken
pytest.skip("client is broken", allow_module_level=True)


@pytest.fixture
def fixtures(tmpdir):
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db.init()
    db.connect()

    yield ti.load_fixtures(tmpdir)

    db.database_proxy.close()


@pytest.fixture(autouse=True)
def no_cli_init(monkeypatch):
    monkeypatch.setattr(cli.group, "config_connect", lambda: None)


def test_create_group(fixtures):
    """Test the create group command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["group", "create", "--help"])
    assert help_result.exit_code == 0
    assert "Create a storage GROUP" in help_result.output

    tmpdir = fixtures["root"]
    tmpdir.chdir()
    result = runner.invoke(cli.cli, args=["group", "create", "group_x"])

    assert result.exit_code == 0
    assert result.output == 'Added group "group_x" to database.\n'
    this_group = st.StorageGroup.get(name="group_x")
    assert this_group.name == "group_x"

    # create an already existing node
    result = runner.invoke(cli.cli, args=["group", "create", "foo"])
    assert result.exit_code == 1
    assert result.output == 'Group name "foo" already exists! Try a different name!\n'


def test_list_groups(fixtures):
    """Test the group list command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["group", "list", "--help"])
    assert help_result.exit_code == 0
    assert "List known storage groups" in help_result.output

    result = runner.invoke(cli.cli, args=["group", "list"])
    assert result.exit_code == 0
    assert re.match(
        r"Name +Notes\n" r"-+ +-+\n" r"foo\n" r"bar +Some bar!\n" r"transport\n",
        result.output,
        re.DOTALL,
    )


def test_rename_group(fixtures):
    """Test the group rename command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["group", "rename", "--help"])
    assert help_result.exit_code == 0
    assert "Change the name of a storage GROUP to NEW-NAME" in help_result.output

    result = runner.invoke(cli.cli, args=["group", "rename", "foo", "bar"])
    assert result.exit_code == 1
    assert result.output == 'Group "bar" already exists.\n'

    old_group = st.StorageGroup.get(name="foo")
    result = runner.invoke(cli.cli, args=["group", "rename", "foo", "bla"])
    assert result.exit_code == 0
    assert result.output == "Updated.\n"

    new_group = st.StorageGroup.get(name="bla")
    assert old_group.id == new_group.id


def test_modify_group(fixtures):
    """Test the group modify command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["group", "modify", "--help"])
    assert help_result.exit_code == 0
    assert "Change the properties of a storage GROUP" in help_result.output

    result = runner.invoke(cli.cli, args=["group", "modify", "bla"])
    assert result.exit_code == 1
    assert result.output == 'Group "bla" does not exist!\n'

    result = runner.invoke(
        cli.cli, args=["group", "modify", "foo", "--notes=Test test test"]
    )
    assert result.exit_code == 0
    assert result.output == "Updated.\n"

    foo_group = st.StorageGroup.get(name="foo")
    assert foo_group.notes == "Test test test"

    result = runner.invoke(cli.cli, args=["group", "modify", "foo"])
    assert result.exit_code == 0
    assert result.output == "Nothing to do.\n"

    foo_group = st.StorageGroup.get(name="foo")
    assert foo_group.notes == "Test test test"

    result = runner.invoke(cli.cli, args=["group", "modify", "foo", "--notes="])
    assert result.exit_code == 0
    assert result.output == "Updated.\n"

    foo_group = st.StorageGroup.get(name="foo")
    assert foo_group.notes is None
