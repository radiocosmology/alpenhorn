"""
test_client_acq
----------------------------------

Tests for `alpenhorn.client.acq` module.
"""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import pytest
from click.testing import CliRunner
import re

import alpenhorn.db as db
import alpenhorn.acquisition as ac
import alpenhorn.archive as ar
import alpenhorn.client as cli
import alpenhorn.storage as st
import alpenhorn.util as util


@pytest.fixture
def fixtures(tmpdir):
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db._connect()

    import test_import as ti

    yield ti.load_fixtures(tmpdir)

    db.database_proxy.close()


@pytest.fixture(autouse=True)
def no_cli_init(monkeypatch):
    monkeypatch.setattr(cli.acq, 'config_connect', lambda: None)


def test_help(fixtures):
    """Test the acq command help"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ['acq', '--help'])
    assert help_result.exit_code == 0
    assert """Commands operating on archival data products.""" in help_result.output


def test_list(fixtures):
    """Test the 'acq list' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ['acq', 'list', '--help'])
    assert help_result.exit_code == 0
    assert "List known acquisitions." in help_result.output

    # List all registered acquisitions (there is only one, 'x', with files 'red', 'jim', and 'sheila')
    result = runner.invoke(cli.cli, ['acq', 'list'])
    assert result.exit_code == 0
    assert re.match(
        r'Name +Files\n'
        r'-+  -+\n'
        r'x +3 *\n',
        result.output, re.DOTALL)

    result = runner.invoke(cli.cli, ['acq', 'list', 'y'])
    assert result.exit_code == 1
    assert "No such storage node: y" in result.output

    # Check acquisitions present on node 'x' (only 'fred' and 'sheila' from 'x', 'jim' is missing)
    result = runner.invoke(cli.cli, ['acq', 'list', 'x'])
    assert result.exit_code == 0
    assert re.match(
        r'Name +Files\n'
        r'-+  -+\n'
        r'x +2 *\n',
        result.output, re.DOTALL)


def test_files(fixtures):
    """Test the 'acq files' command"""
    runner = CliRunner()

    # Check help output
    help_result = runner.invoke(cli.cli, ['acq', 'files', '--help'])
    assert help_result.exit_code == 0
    assert "List files that are in the ACQUISITION." in help_result.output

    # Fail when given a non-existent acquisition
    result = runner.invoke(cli.cli, ['acq', 'files', 'z'])
    assert result.exit_code == 1
    assert "No such acquisition: z" in result.output

    # Check regular case
    result = runner.invoke(cli.cli, ['acq', 'files', 'x'])
    assert result.exit_code == 0
    assert re.match(
        r'.*Name +Size\n'
        r'-+  -+\n'
        r'fred *\n'
        r'jim +0 *\n'
        r'sheila *\n',
        result.output, re.DOTALL)
