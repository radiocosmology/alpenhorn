"""
test_client
----------------------------------

Tests for `alpenhorn.client` module.
"""

import pytest
from click.testing import CliRunner
import re

try:
    from unittest.mock import patch, call
except ImportError:
    from mock import patch, call


import alpenhorn.db as db
import alpenhorn.client as cli
import alpenhorn.acquisition as ac
import alpenhorn.archive as ar
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
    monkeypatch.setattr(cli, 'config_connect', lambda: None )
    monkeypatch.setattr(cli.node, 'config_connect', lambda: None )


def test_import_schema(fixtures):
    assert set(db.database_proxy.get_tables()) == {
        u'storagegroup', u'storagenode',
        u'acqtype', u'archiveacq',
        u'filetype', u'archivefile',
        u'archivefilecopyrequest', u'archivefilecopy',
        u'zabinfo', u'quuxinfo', u'zxcinfo', u'spqrinfo', u'loginfo'
    }
    groups = set(st.StorageGroup.select(st.StorageGroup.name).tuples())
    assert groups == { ( 'foo', ), ( 'bar', ), ( 'transport', ) }
    assert st.StorageGroup.get(st.StorageGroup.name == 'bar').notes == 'Some bar!'


def test_command_line_interface(fixtures):
    """Check the output when no commands are invoked"""
    runner = CliRunner()
    result = runner.invoke(cli.cli)
    assert result.exit_code == 0
    assert 'Client interface for alpenhorn' in result.output
    assert '-h, --help  Show this message and exit.' in result.output


def test_sync(fixtures):
    """Test the 'sync' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.sync, ['--help'])
    assert help_result.exit_code == 0
    assert 'Copy all files from NODE to GROUP that are not already present.' in help_result.output
    assert 'Options:\n  --acq ACQ              Sync only this acquisition.' in help_result.output

    result = runner.invoke(cli.sync, args=['--target', 'doesnotexist', 'x', 'bar'])
    assert result.exit_code == 1
    assert result.output == 'Target group "doesnotexist" does not exist in the DB.\n'

    result = runner.invoke(cli.sync, args=['doesnotexist', 'bar'])
    assert result.exit_code == 1
    assert result.output == 'Node "doesnotexist" does not exist in the DB.\n'

    result = runner.invoke(cli.sync, args=['x', 'doesnotexist'])
    assert result.exit_code == 1
    assert result.output == 'Group "doesnotexist" does not exist in the DB.\n'

    result = runner.invoke(cli.sync, args=['x', 'bar'])
    assert result.exit_code == 0
    assert result.output == 'No files to copy from node x.\n'

    # now pretend node 'x' has a copy of 'fred', 1 GB in size
    file_copy = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where(ac.ArchiveFile.name == 'fred')
                 .get())
    file_copy.has_file = 'Y'
    file_copy.file.size_b = 1073741824.0
    file_copy.save()
    file_copy.file.save()

    result = runner.invoke(cli.sync, args=['--force', '--show_acq', '--show_files', 'x', 'bar'])
    assert result.exit_code == 0
    assert re.match(r'x \[1 files\]\n' +
                    r'x/fred\n' +
                    r'Will request that 1 files \(1\.0 GB\) be copied from node x to group bar\.\n' +
                    r'Adding 1 new requests\.\n$',
                    result.output, re.DOTALL)

    ## verify that there is a copy request for 'fred' from node 'x' to group 'bar'
    copy_request = (ar.ArchiveFileCopyRequest
                    .select()
                    .join(ac.ArchiveFile)
                    .where(ac.ArchiveFile.name == 'fred')).get()
    assert copy_request.node_from.name == 'x'
    assert copy_request.group_to.name == 'bar'
    assert not copy_request.completed
    assert not copy_request.cancelled

    ## if we run sync again, the copy request will simply update the timestamp of the latest request
    result = runner.invoke(cli.sync, args=['--force', '--show_acq', '--show_files', 'x', 'bar'])
    assert result.exit_code == 0
    assert re.match(r'x \[1 files\]\n' +
                    r'x/fred\n' +
                    r'Will request that 1 files \(1\.0 GB\) be copied from node x to group bar\.\n' +
                    r'Adding no new requests, keeping 1 already existing\.\n$',
                    result.output, re.DOTALL)

    # there should still be only one copy request for fred, just it's timestamp should have changed
    copy_requests = (ar.ArchiveFileCopyRequest
                    .select()
                    .join(ac.ArchiveFile)
                    .where(ac.ArchiveFile.name == 'fred'))
    assert len(copy_requests) == 1
    for req in copy_requests:
        assert req.node_from.name == 'x'
        assert req.group_to.name == 'bar'
        assert not req.completed
        assert not req.cancelled
        assert req.timestamp == copy_request.timestamp

    # adding a second request after the first one was cancelled should add it,
    # while leaving the first request untouched
    copy_request.cancelled = True
    copy_request.save(only=copy_request.dirty_fields)

    result = runner.invoke(cli.sync, args=['--force', '--show_acq', '--show_files', 'x', 'bar'])
    assert result.exit_code == 0
    assert re.match(r'x \[1 files\]\n' +
                    r'x/fred\n' +
                    r'Will request that 1 files \(1\.0 GB\) be copied from node x to group bar\.\n' +
                    r'Adding 1 new requests\.\n$',
                    result.output, re.DOTALL)
    copy_requests = (ar.ArchiveFileCopyRequest
                    .select()
                    .join(ac.ArchiveFile)
                    .where(ac.ArchiveFile.name == 'fred'))
    assert len(copy_requests) == 2
    for req in copy_requests:
        if req.id == copy_request.id:
            assert req.timestamp == copy_request.timestamp
            assert not req.completed
            assert req.cancelled
        else:
            assert not req.completed
            assert not req.cancelled


def test_status(fixtures):
    """Test the output of the 'status' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.status, ['--help'])
    assert help_result.exit_code == 0
    assert 'Summarise the status of alpenhorn storage nodes.' in help_result.output
    assert 'Options:\n  --all   Show the status of all nodes,' in help_result.output

    # we start off with no good file copies, so the output should only contain
    # the header
    result = runner.invoke(cli.status, args=['--all'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 4
    assert re.match(r'^Node\s+Files\s+Size \[TB\]\s+', output[0])
    assert re.match(r'^[- ]+$', output[1])
    assert re.match(r'x\s+0\s+0\.0\s+foo.example.com:', output[2])
    assert re.match(r'z\s+0\s+0\.0\s+bar.example.com:None', output[3])

    # now pretend node 'x' has a copy of 'fred'
    file_copy = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where(ac.ArchiveFile.name == 'fred')
                 .get())
    file_copy.has_file = 'Y'
    file_copy.file.size_b = 1
    file_copy.save()
    file_copy.file.save()

    result = runner.invoke(cli.status, args=['--all'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 4
    assert re.match(r'x\s+1\s+0\.0\s+33\.3\s+100\.0\s+foo.example.com:', output[2])
    assert re.match(r'z\s+0\s+0\.0\s+bar.example.com:None', output[3])
