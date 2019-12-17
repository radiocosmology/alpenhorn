"""
test_client_group
----------------------------------

Tests for `alpenhorn.client.group` module.
"""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import pytest
from click.testing import CliRunner

try:
    from unittest.mock import patch, call
except ImportError:
    from mock import patch, call


import alpenhorn.db as db
import alpenhorn.client as cli
import alpenhorn.storage as st

import test_import as ti


@pytest.fixture
def fixtures(tmpdir):
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db._connect()

    yield ti.load_fixtures(tmpdir)

    db.database_proxy.close()


@pytest.fixture(autouse=True)
def no_cli_init(monkeypatch):
    monkeypatch.setattr(cli.group, 'config_connect', lambda: None)


def test_create_group(fixtures):
    """Test the create group command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ['create-group', '--help'])
    assert help_result.exit_code == 0
    assert 'Create a storage GROUP' in help_result.output

    tmpdir = fixtures['root']
    tmpdir.chdir()
    result = runner.invoke(cli.cli, args=['create-group', 'group_x'])

    assert result.exit_code == 0
    assert result.output == 'Added group "group_x" to database.\n'
    this_group = st.StorageGroup.get(name='group_x')
    assert this_group.name == "group_x"

    # create an already existing node
    result = runner.invoke(cli.cli, args=['create-group', 'foo'])
    assert result.exit_code == 1
    assert result.output == 'Group name "foo" already exists! Try a different name!\n'
