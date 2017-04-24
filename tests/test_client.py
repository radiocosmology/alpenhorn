
"""
test_client
----------------------------------

Tests for `alpenhorn.clien` module.
"""

import pytest
from click.testing import CliRunner
import os
import re
from io import StringIO
from mock import patch, call

# TODO: use Pytest's directory used for tmpdir/basedir, not '/tmp'
os.environ['ALPENHORN_LOG_FILE'] = '/tmp' + '/alpenhornd.log'

import alpenhorn.db as db
import alpenhorn.client as cli
import alpenhorn.acquisition as ac
import alpenhorn.archive as ar
import alpenhorn.storage as st

import test_import as ti

@pytest.fixture
def fixtures(tmpdir):
    return ti.fixtures(tmpdir)

def test_import_schema(fixtures):
    assert set(db.database_proxy.get_tables() )== {
        u'storagegroup', u'storagenode',
        u'acqtype', u'archiveinst', u'archiveacq',
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


def test_sync(fixtures):
    """Test the 'sync' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.sync, ['--help'])
    assert help_result.exit_code == 0
    assert 'Copy all files from NODE to GROUP that are not already present.' in help_result.output
    assert 'Options:\n  --acq ACQ              Sync only this acquisition.' in help_result.output

    result = runner.invoke(cli.sync, args = ['x', 'bar'])
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

    result = runner.invoke(cli.sync, args = ['--force', '--show_acq', '--show_files', 'x', 'bar'])
    assert result.exit_code == 0
    assert re.match(r'x \[1 files\]\n' +
                    r'x/fred\n' +
                    r'Will request that 1 files \(1\.0 GB\) be copied from node x to group bar\.\n' +
                    r'Updating 0 existing requests and inserting 1 new ones\.\n$',
                    result.output, re.DOTALL)

    ## by default, the cleaned copy should be marked as 'maybe wanted'
    copy_request = (ar.ArchiveFileCopyRequest
                    .select()
                    .join(ac.ArchiveFile)
                    .where(ac.ArchiveFile.name == 'fred')).get()
    assert copy_request.node_from.name == 'x'
    assert copy_request.group_to.name == 'bar'
    assert not copy_request.completed
    assert not copy_request.cancelled
    assert copy_request.n_requests == 1

    ## if we run sync again, the copy request will simply get the 'n_requests' count incremented by 1
    result = runner.invoke(cli.sync, args = ['--force', '--show_acq', '--show_files', 'x', 'bar'])
    assert result.exit_code == 0
    assert re.match(r'x \[1 files\]\n' +
                    r'x/fred\n' +
                    r'Will request that 1 files \(1\.0 GB\) be copied from node x to group bar\.\n' +
                    r'Updating 1 existing requests and inserting 0 new ones\.\n$',
                    result.output, re.DOTALL)

    ## by default, the cleaned copy should be marked as 'maybe wanted'
    copy_request = (ar.ArchiveFileCopyRequest
                    .select()
                    .join(ac.ArchiveFile)
                    .where(ac.ArchiveFile.name == 'fred')).get()
    assert copy_request.node_from.name == 'x'
    assert copy_request.group_to.name == 'bar'
    assert not copy_request.completed
    assert not copy_request.cancelled
    assert copy_request.n_requests == 2


def test_status(fixtures):
    """Test the output of the 'status' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.status, ['--help'])
    assert help_result.exit_code == 0
    assert 'Summarise the status of alpenhorn storage nodes.' in help_result.output
    assert 'Options:\n  --all   Show the status of all nodes,' in help_result.output

    # we start off with no good file copies, so the output should only contain
    # the header
    result = runner.invoke(cli.status, args = ['--all'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 2
    assert re.match(r'^Node\s+Files\s+Size \[TB\]\s+', output[0])
    assert re.match(r'^[- ]+$', output[1])

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

    result = runner.invoke(cli.status, args = ['--all'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 3
    assert re.match(r'x\s+1\s+0\.0\s+33\.3\s+100\.0\s+foo.example.com:', output[2])


def test_verify(fixtures):
    """Test the output of the 'verify' command"""
    tmpdir = fixtures['root']

    runner = CliRunner()
    result = runner.invoke(cli.verify, ['x'])
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
    result = runner.invoke(cli.verify, ['x'])
    assert result.exit_code == 0
    assert re.match(r'.*\n=== Missing files ===\n' +
                    str(tmpdir.join(file_copy.file.acq.name, file_copy.file.name)),
                    result.output, re.DOTALL)
    assert re.match(r'.*\n=== Summary ===\n' +
                    '  1 total files\n' +
                    '  1 missing files\n' +
                    '  0 corrupt files',
                    result.output, re.DOTALL)


def test_clean(fixtures):
    """Test the 'clean' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.clean, ['--help'])
    assert help_result.exit_code == 0
    assert 'Clean up NODE by marking older files as potentially removable.' in help_result.output
    assert 'Options:\n  -d, --days INTEGER     Clean files older than <days>.' in help_result.output

    ## pretend 'fred' is 1 GB in size
    f = ac.ArchiveFile.get(ac.ArchiveFile.name == 'fred')
    f.size_b = 1073741824.0
    f.save()

    tmpdir = fixtures['root']
    tmpdir.chdir()
    result = runner.invoke(cli.clean, args = ['-f', 'x'])
    assert result.exit_code == 0
    assert re.match(r'.*\nCleaning up 1 files \(1\.0 GB\) from x\.\n.*' +
                    r'Marked 1 files for cleaning\n',
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
    result = runner.invoke(cli.clean, args = ['-f', '--now', 'x'])
    assert result.exit_code == 0
    assert re.match(r'.*\nCleaning up 1 files \(1\.0 GB\) from x\.\n.*' +
                    r'Marked 1 files for cleaning\n',
                    result.output, re.DOTALL)

    file_copy = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where(ac.ArchiveFile.name == 'fred')).get()
    assert file_copy.wants_file == 'N'


def test_mounted(fixtures):
    """Test the output of the 'mounted' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.mounted, ['--help'])
    assert help_result.exit_code == 0
    assert 'List the nodes mounted on this' in help_result.output
    assert 'Options:\n  -H, --host TEXT  use specified host' in help_result.output

    result = runner.invoke(cli.mounted, args = ['--host', 'foo.example.com'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert re.match(r'^x\s+' + str(fixtures['root']) + '\s+2 files$', output[0])


@patch('alpenhorn.client.get_e2label', return_value=None)
@patch('os.path.realpath', return_value=None)
@patch('os.mkdir')
@patch('os.popen', side_effect=[StringIO(u'Number  Start   End     Size    Type     File system  Flags\nfoobar'), StringIO(u'')])
@patch('glob.glob', return_value=['/dev/disk/by-id/fake-12-34-56-78'])
@patch('os.getuid', return_value=0)
@patch('os.system', return_value=None)
def test_format_transport(system_mock, getuid_mock, glob_mock, popen_mock, mkdir_mock, realpath_mock, get_e2label_mock, fixtures):
    """Test the 'mount_transport' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.format_transport, ['--help'])
    assert help_result.exit_code == 0
    assert 'Interactive routine for formatting a transport disc as a storage node' in help_result.output
    assert 'Options:\n  --help  Show this message and exit.' in help_result.output

    result = runner.invoke(cli.format_transport, args = ['12-34-56-78'])
    assert result.exit_code == 0
    assert re.match(r'.*\nDisc is already formatted\.\n' +
                    r'Labelling the disc as "CH-12-34-56-78"'+
                    r'.*\nSuccessfully created storage node.\n' +
                    r'Node created but not mounted. Run alpenhorn mount_transport for that.',
                    result.output, re.DOTALL)
    assert system_mock.mock_calls == [call('/sbin/e2label /dev/disk/by-id/fake-12-34-56-78-part1 CH-12-34-56-78')]
    assert popen_mock.mock_calls == [call('parted -s /dev/disk/by-id/fake-12-34-56-78 print'), call('df')]
    assert mkdir_mock.mock_calls == [call('/mnt/CH-12-34-56-78')]
    node = st.StorageNode.get(name='CH-12-34-56-78')
    assert node.group.name == 'transport'
    assert node.root == '/mnt/CH-12-34-56-78'
    assert node.storage_type == 'T'


@patch('os.system')
def test_mount_transport(mock, fixtures):
    """Test the 'mount_transport' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.mount_transport, ['--help'])
    assert help_result.exit_code == 0
    assert 'Mount a transport disk into the system and then make it available' in help_result.output
    assert 'Options:\n  --user TEXT     username to access this node' in help_result.output

    result = runner.invoke(cli.mount_transport, args = ['z'])
    assert result.exit_code == 0
    assert mock.mock_calls == [call('mount /mnt/z')]
    assert re.match(r'Mounting disc at /mnt/z',
                    result.output, re.DOTALL)

@patch('os.system')
def test_unmount_transport(mock, fixtures):
    """Test the 'mount_transport' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.unmount_transport, ['--help'])
    assert help_result.exit_code == 0
    assert 'Unmount a transport disk from the system and then remove it from alpenhorn.' in help_result.output
    assert 'Options:\n  --help  Show this message and exit.' in help_result.output

    result = runner.invoke(cli.unmount_transport, args = ['x'])
    assert result.exit_code == 0
    assert mock.mock_calls == [call('umount /mnt/x')]
    assert re.match(r'Unmounting disc at /mnt/x',
                    result.output, re.DOTALL)


def test_mount(fixtures):
    """Test the 'mount' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.mount, ['--help'])
    assert help_result.exit_code == 0
    assert 'Interactive routine for mounting a storage node located at ROOT.' in help_result.output
    assert 'Options:\n  --path TEXT      Root path for this node' in help_result.output

    result = runner.invoke(cli.mount, args = ['x'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert 'Node "x" is already mounted.' in output[0]

    # now pretend the node is unmounted and mount it again
    node = st.StorageNode.get(name='x')
    node.mounted = False
    node.save()

    result = runner.invoke(cli.mount,
                           args = ['--path=/bla',
                                   '--user=bozo',
                                   '--address=foobar.example.com',
                                   'x'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 2
    assert re.match(r'^I will set the host to ".+"\.$', output[0])
    assert 'Successfully mounted "x".' == output[1]

    import socket
    node = st.StorageNode.get(name='x')
    assert node.mounted
    assert node.root == '/bla'
    assert node.username == 'bozo'
    assert node.address == 'foobar.example.com'
    assert node.host == output[0].split('"')[1] == socket.gethostname()


def test_unmount(fixtures):
    """Test the 'unmount' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.unmount, ['--help'])
    assert help_result.exit_code == 0
    assert 'Unmount a storage node with location or named ROOT_OR_NAME.' in help_result.output
    assert 'Options:\n  --help  Show this message and exit.' in help_result.output

    result = runner.invoke(cli.unmount, args = ['x'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert 'Node successfully unmounted.' in output[0]
    node = st.StorageNode.get(name='x')
    assert not node.mounted

    # unmount already unmounted node
    result = runner.invoke(cli.unmount,
                           args = ['x'])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert 'There is no node mounted there any more' in output[0]

    # unmount an unknown node
    result = runner.invoke(cli.unmount,
                           args = ['y'])
    assert result.exit_code == 1
    output = result.output.splitlines()
    assert 'That is neither a node name, nor a path on this host. I quit.' == output[0]


def test_import_files(fixtures):
    """Test the 'import_files' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.import_files, ['--help'])
    assert help_result.exit_code == 0
    assert 'Scan the current directory for known acquisition files' in help_result.output
    assert 'Options:\n  -v, --verbose\n  --acq TEXT     Limit import to specified acquisition directories.' in help_result.output

    tmpdir = fixtures['root']
    tmpdir.chdir()
    result = runner.invoke(cli.import_files, args = ['-vv', 'x'])

    assert result.exit_code == 0
    assert re.match(r'.*\n==== Summary ====\n\n' +
                    r'Added 0 files\n\n' +
                    r'1 corrupt files\.\n' +
                    r'0 files already registered\.\n' +
                    r'1 files not known\n' +
                    r'2 directories were not acquisitions\.\n\n' +
                    r'Added files:\n\n' +
                    r'Corrupt:\n' +
                    r'x/jim\n\n' +
                    r'Unknown files:\n' +
                    r'x/foo\.log\n\n' +
                    r'Unknown acquisitions:\n' +
                    r'12345678T000000Z_inst_zab\n' +
                    r'alp_root\n\n$',
                    result.output, re.DOTALL)

    ## now add a known file ('fred') and pretend 'jim' should be added
    tmpdir.join('x', 'fred').write('')
    f = ac.ArchiveFile.get(ac.ArchiveFile.name == 'jim')
    f.size_b = 0
    f.save()


    result = runner.invoke(cli.import_files, args = ['-vv', '--dry', 'x'])
    assert result.exit_code == 0
    assert re.match(r'.*\n==== Summary ====\n\n' +
                    r'Added 1 files\n\n' +
                    r'0 corrupt files\.\n' +
                    r'1 files already registered\.\n' +
                    r'1 files not known\n' +
                    r'2 directories were not acquisitions\.\n\n' +
                    r'Added files:\n' +
                    r'x/jim\n\n' +
                    r'Corrupt:\n\n' +
                    r'Unknown files:\n' +
                    r'x/foo\.log\n\n' +
                    r'Unknown acquisitions:\n' +
                    r'12345678T000000Z_inst_zab\n' +
                    r'alp_root\n\n$',
                    result.output, re.DOTALL)
    ## Because we're running in dry mode the database is not updated
    assert (ar.ArchiveFileCopy
            .select()
            .join(ac.ArchiveFile)
            .where(ac.ArchiveFile.name == 'jim')
            .count()) == 0

    ## now repeat but allowing database change
    result = runner.invoke(cli.import_files, args = ['-vv', 'x'])
    assert result.exit_code == 0
    assert re.match(r'.*\n==== Summary ====\n\n' +
                    r'Added 1 files\n\n' +
                    r'0 corrupt files\.\n' +
                    r'1 files already registered\.\n' +
                    r'1 files not known\n' +
                    r'2 directories were not acquisitions\.\n\n' +
                    r'Added files:\n' +
                    r'x/jim\n\n' +
                    r'Corrupt:\n\n' +
                    r'Unknown files:\n' +
                    r'x/foo\.log\n\n' +
                    r'Unknown acquisitions:\n' +
                    r'12345678T000000Z_inst_zab\n' +
                    r'alp_root\n\n$',
                    result.output, re.DOTALL)
    ## check the database state
    jims = list(ar.ArchiveFileCopy
                .select(ac.ArchiveFile.name,
                        ar.ArchiveFileCopy.has_file,
                        ar.ArchiveFileCopy.wants_file)
                .join(ac.ArchiveFile)
                .where(ac.ArchiveFile.name == 'jim')
                .dicts())
    assert jims == [
        {'name': 'jim', 'has_file': 'Y', 'wants_file': 'Y'}
    ]


def test_nested_import_files(fixtures):
    """Test the 'import_files' command"""
    runner = CliRunner()

    tmpdir = fixtures['root']
    tmpdir.chdir()

    ## pretend 'jim' should be added
    inst = ac.ArchiveInst.get(ac.ArchiveInst.name == 'foo')
    acq_type = ac.AcqType.create(name='zab')
    acq = ac.ArchiveAcq.create(name='alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab', type=acq_type, inst=inst)
    file_type = ac.FileType.get(name='zxc')
    acq_file = ac.ArchiveFile.create(
        name='acq_data/x_123_1_data/raw/acq_123_1.zxc',
        acq=acq,
        type=file_type,
        size_b=len(fixtures['files']['alp_root']['2017']['03']['21']['acq_xy1_45678901T000000Z_inst_zab']['acq_data']['x_123_1_data']['raw']['acq_123_1.zxc']['contents']),
        md5sum=fixtures['files']['alp_root']['2017']['03']['21']['acq_xy1_45678901T000000Z_inst_zab']['acq_data']['x_123_1_data']['raw']['acq_123_1.zxc']['md5'])

    result = runner.invoke(cli.import_files, args = ['-vv', 'x'])
    print result.output
    assert result.exit_code == 0
    assert re.match(r'.*\n==== Summary ====\n\n' +
                    r'Added 1 files\n\n' +
                    r'1 corrupt files\.\n' +
                    r'0 files already registered\.\n' +
                    r'9 files not known\n' +
                    r'1 directories were not acquisitions\.\n\n' +
                    r'Added files:\n' +
                    r'alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/raw/acq_123_1.zxc\n\n' +
                    r'Corrupt:\n' +
                    r'x/jim\n\n' +
                    r'Unknown files:\n' +
                    r'alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/proc/.acq_123_proc.zxc.lock\n' +
                    r'alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/proc/acq_123_1_proc.zxc\n' +
                    r'alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_2_data/proc/.acq_123_2_proc.zxc.lock\n' +
                    r'alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_2_data/proc/acq_123_2_proc.zxc\n' +
                    r'alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_2_data/raw/acq_123_2.zxc\n' +
                    r'alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/housekeeping_data/hk_123.zxc\n' +
                    r'alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/housekeeping_data/.hk_123.zxc.lock\n' +
                    r'alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/summary.txt\n' +
                    r'x/foo\.log\n\n' +
                    r'Unknown acquisitions:\n' +
                    r'12345678T000000Z_inst_zab\n$',
                    result.output, re.DOTALL)
    ## check the database state
    acq_files = list(ar.ArchiveFileCopy
                .select(ac.ArchiveFile.name,
                        ar.ArchiveFileCopy.has_file,
                        ar.ArchiveFileCopy.wants_file)
                .join(ac.ArchiveFile)
                .where(ac.ArchiveFile.name == acq_file.name)
                .dicts())
    assert acq_files == [
        {'name': acq_file.name, 'has_file': 'Y', 'wants_file': 'Y'}
    ]
