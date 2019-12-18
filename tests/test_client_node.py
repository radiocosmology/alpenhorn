"""
test_client_node
----------------------------------

Tests for `alpenhorn.client.node` module.
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
    monkeypatch.setattr(cli.node, 'config_connect', lambda: None)


def test_create_node(fixtures):
    """Test the create node command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ['node', 'create', '--help'])
    assert help_result.exit_code == 0
    assert "Create a storage NODE within storage GROUP with a ROOT directory on\n  HOSTNAME." in help_result.output

    tmpdir = fixtures['root']
    tmpdir.chdir()

    result = runner.invoke(cli.cli, ['node', 'create', 'y', 'root', 'hostname', 'bar'])
    assert result.exit_code == 0
    assert result.output == 'Added node "y" belonging to group "bar" in the directory "root" at host "hostname" to database.\n'

    node = st.StorageNode.get(name='y')

    assert result.exit_code == 0
    assert node.group.name == 'bar'
    assert node.name == 'y'
    assert node.root == 'root'
    assert node.host == 'hostname'

    result = runner.invoke(cli.cli, ['node', 'create', 'y', 'root', 'hostname', 'baba'])

    assert result.exit_code == 1
    assert result.output == 'Requested group "baba" does not exit in DB.\n'

    result = runner.invoke(cli.cli, ['node', 'create', 'x', 'root', 'hostname', 'bar'])
    assert result.exit_code == 1
    assert result.output == 'Node name "x" already exists! Try a different name!\n'

    result = runner.invoke(cli.cli, ['node', 'create', '--storage_type=Z', 'z', 'root', 'hostname', 'bar'])
    assert result.exit_code == 2  # Click usage error
    assert 'Invalid value for "--storage_type": invalid choice: Z. (choose from A, T, F)' in result.output


def test_activate(fixtures):
    """Test the 'activate' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ['node', 'activate', '--help'])
    assert help_result.exit_code == 0
    assert 'Interactive routine for activating a storage node located at ROOT.' in help_result.output
    assert 'Options:\n  --path TEXT      Root path for this node' in help_result.output

    # test for error when mounting a non-existent node
    result = runner.invoke(cli.cli, ['node', 'activate', 'nonexistent'])
    assert result.exit_code == 1
    output = result.output.splitlines()
    assert len(output) == 1
    assert 'Storage node "nonexistent" does not exist. I quit.' in output[0]

    # test for error when trying to mount a node that's already mounted
    result = runner.invoke(cli.cli, ['node', 'activate', 'x'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert 'Node "x" is already active.' in output[0]

    # now pretend the node is inactive so we can try to activate it
    node = st.StorageNode.get(name='x')
    node.active = False
    node.save()

    # test for error when check for ALPENHORN_NODE fails
    result = runner.invoke(cli.cli, ['node', 'activate', 'x'])
    assert result.exit_code == 1
    assert 'Node "x" does not match ALPENHORN_NODE' in result.output
    assert not st.StorageNode.get(name='x').active

    # test for success when check for ALPENHORN_NODE passes and the node is
    # mounted
    x_root = fixtures['root'].join('x')
    x_root.join('ALPENHORN_NODE').write('x')
    result = runner.invoke(cli.cli,
                           args=['node', 'activate',
                                 '--path=' + str(x_root),
                                 '--user=bozo',
                                 '--address=foobar.example.com',
                                 'x'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 2
    assert re.match(r'^I will set the host to ".+"\.$', output[0])
    assert 'Successfully activated "x".' == output[1]

    node = st.StorageNode.get(name='x')
    assert node.active
    assert node.root == x_root
    assert node.username == 'bozo'
    assert node.address == 'foobar.example.com'
    assert node.host == output[0].split('"')[1] == util.get_short_hostname()


def test_deactivate(fixtures):
    """Test the 'deactivate' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ['node', 'deactivate', '--help'])
    assert help_result.exit_code == 0
    assert 'Deactivate a storage node with location or named ROOT_OR_NAME.' in help_result.output
    assert 'Options:\n  -h, --help  Show this message and exit.' in help_result.output

    result = runner.invoke(cli.cli, args=['node', 'deactivate', 'x'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert 'Node successfully deactivated.' in output[0]
    node = st.StorageNode.get(name='x')
    assert not node.active

    # deactivate already deactivated node
    result = runner.invoke(cli.cli,
                           args=['node', 'deactivate', 'x'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert 'There is no active node there any more' in output[0]

    # deactivate an unknown node
    result = runner.invoke(cli.cli,
                           args=['node', 'deactivate', 'y'])
    assert result.exit_code == 1
    output = result.output.splitlines()
    assert 'That is neither a node name, nor a path on this host. I quit.' == output[0]


def test_verify(fixtures):
    """Test the output of the 'verify' command"""
    tmpdir = fixtures['root']

    runner = CliRunner()

    # test for error when mounting a non-existent node
    result = runner.invoke(cli.cli, ['node', 'verify', 'foo'])
    assert result.exit_code == 1
    assert 'Storage node "foo" does not exist.' in result.output

    # test for error when check when the node is not active
    node = st.StorageNode.get(name='x')
    node.active = False
    node.save()

    result = runner.invoke(cli.cli, ['node', 'verify', 'x'])
    assert result.exit_code == 1
    assert 'Node "x" is not active.' in result.output

    # test for error when check for ALPENHORN_NODE fails
    node.active = True
    node.root = str(tmpdir)
    node.save()

    result = runner.invoke(cli.cli, ['node', 'verify', 'x'])
    assert result.exit_code == 1
    assert 'Node "x" does not match ALPENHORN_NODE: '.format(node.root) in result.output

    # test for 'x' when it is mounted, but contains no files
    tmpdir.join('ALPENHORN_NODE').write('x')

    result = runner.invoke(cli.cli, ['node', 'verify', 'x'])
    assert result.exit_code == 0
    assert re.match(r'.*\n=== Summary ===\n' +
                    '  0 total files\n' +
                    '  0 missing files\n' +
                    '  0 corrupt files',
                    result.output, re.DOTALL)

    # now pretend node 'x' has a copy of 'fred'
    file_copy = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where(ac.ArchiveFile.name == 'fred')
                 .get())
    file_copy.has_file = 'Y'
    file_copy.save()
    result = runner.invoke(cli.cli, ['node', 'verify', 'x'])
    assert result.exit_code == 2
    assert re.match(r'.*\n=== Missing files ===\n' +
                    str(tmpdir.join(file_copy.file.acq.name, file_copy.file.name)),
                    result.output, re.DOTALL)
    assert re.match(r'.*\n=== Summary ===\n' +
                    '  1 total files\n' +
                    '  1 missing files\n' +
                    '  0 corrupt files',
                    result.output, re.DOTALL)

    ## now add a known file ('fred')
    tmpdir.join('x', 'fred').write('')
    result = runner.invoke(cli.cli, ['node', 'verify', '--md5', 'x'])
    assert result.exit_code == 1
    assert re.match(r'.*\n=== Corrupt files ===\n' +
                    '/.*/ROOT/x/fred\n'
                    '.*\n=== Summary ===\n' +
                    '  1 total files\n' +
                    '  0 missing files\n' +
                    '  1 corrupt files',
                    result.output, re.DOTALL)


def test_clean(fixtures):
    """Test the 'clean' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ['node', 'clean', '--help'])
    assert help_result.exit_code == 0
    assert 'Clean up NODE by marking older files as potentially removable.' in help_result.output
    assert 'Options:\n  -d, --days INTEGER     Clean files older than <days>.' in help_result.output

    ## pretend 'fred' is 1 GB in size
    f = ac.ArchiveFile.get(ac.ArchiveFile.name == 'fred')
    f.size_b = 1073741824.0
    f.save()

    # By default a FileCopy is set to has_file='N'
    file_copy = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where(ac.ArchiveFile.name == 'fred')).get()
    file_copy.has_file = 'Y'
    file_copy.save()

    tmpdir = fixtures['root']
    tmpdir.chdir()
    result = runner.invoke(cli.cli, args=['node', 'clean', '-f', 'x'])
    assert result.exit_code == 0
    assert re.match(r'.*\nMark 1 files \(1\.0 GB\) from "x" available for removal\.\n.*' +
                    r'Marked 1 files available for removal.\n',
                    result.output, re.DOTALL)

    ## by default, the cleaned copy should be marked as 'maybe wanted'
    file_copy = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where(ac.ArchiveFile.name == 'fred')).get()
    assert file_copy.wants_file == 'M'

    ## if we clean with the '--now' option, the copy should be marked as 'not wanted'
    file_copy.wants_file = 'Y'
    file_copy.save()
    result = runner.invoke(cli.cli, args=['node', 'clean', '-f', '--now', 'x'])
    assert result.exit_code == 0
    assert re.match(r'.*\nMark 1 files \(1\.0 GB\) from "x" available for removal\.\n.*' +
                    r'Marked 1 files available for removal.\n',
                    result.output, re.DOTALL)

    file_copy = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where(ac.ArchiveFile.name == 'fred')).get()
    assert file_copy.wants_file == 'N'

    ## if we clean with the '--cancel' option, all unwanted copies should again be marked wanted
    result = runner.invoke(cli.cli, args=['node', 'clean', '-f', '--cancel', 'x'])
    assert result.exit_code == 0
    assert re.match(r'.*\nMark 1 files \(1\.0 GB\) from "x" for keeping\.\n.*' +
                    r'Marked 1 files for keeping.\n',
                    result.output, re.DOTALL)

    file_copy = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where(ac.ArchiveFile.name == 'fred')).get()
    assert file_copy.wants_file == 'Y'

    ## '--cancel' and '--now' are mutually exclusive options
    result = runner.invoke(cli.cli, args=['node', 'clean', '--now', '--cancel', 'x'])
    assert result.exit_code == 1
    assert 'Options --cancel and --now are mutually exclusive.' in result.output

    # using a non-existent node should be reported as an error
    result = runner.invoke(cli.cli, args=['node', 'clean', '--force', '--cancel', 'y'])
    assert result.exit_code == 1
    assert 'Storage node "y" does not exist.' in result.output

    # cleaning an archive node without the force flag or interactive
    # confirmation should be an error
    result = runner.invoke(cli.cli, args=['node', 'clean', 'z'])
    assert result.exit_code == 1
    assert 'Cannot clean archive node "z" without forcing.' in result.output

