"""
test_import
------------------

Tests for `alpenhorn.auto_import` module.
"""

import pytest
import yaml
import os

# TODO: use Pytest's directory used for tmpdir/basedir, not '/tmp'
os.environ['ALPENHORN_LOG_FILE'] = '/tmp' + '/alpenhornd.log'

import alpenhorn.db as db
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.acquisition as ac
import alpenhorn.auto_import as auto_import
import alpenhorn.generic as ge

import test_archive_model as ta


tests_path = os.path.abspath(os.path.dirname(__file__))


# Create handlers for the acquisition and file types
class ZabInfo(ge.GenericAcqInfo):
    _acq_type = 'zab'
    _file_types = ['zxc', 'log']
    patterns = ['*zab']

class QuuxInfo(ge.GenericAcqInfo):
    _acq_type = 'quux'
    _file_types = ['zxc', 'log']
    patterns = ['*quux', 'x']

class ZxcInfo(ge.GenericFileInfo):
    _file_type = 'zxc'
    patterns = ['*.zxc', 'jim*', 'sheila']

class SpqrInfo(ge.GenericFileInfo):
    _file_type = 'spqr'
    patterns = ['*spqr*']

class LogInfo(ge.GenericFileInfo):
    _file_type = 'log'
    patterns = ['*.log']

@pytest.fixture
def fixtures(tmpdir):
    fs = ta.fixtures().next()

    p = tmpdir.mkdir("ROOT")

    (st.StorageNode
     .update(root=str(p))
     .where(st.StorageNode.name == 'x')
     .execute())

    # Register new handlers
    ac.AcqType.register_type(ZabInfo)
    ac.AcqType.register_type(QuuxInfo)
    ac.FileType.register_type(ZxcInfo)
    ac.FileType.register_type(SpqrInfo)
    ac.FileType.register_type(LogInfo)

    db.database_proxy.create_tables([ZabInfo, QuuxInfo, ZxcInfo, SpqrInfo, LogInfo])

    with open(os.path.join(tests_path, 'fixtures/files.yml')) as f:
        fixtures = yaml.load(f)

    for dir_name, files in fixtures.iteritems():
        d = p.mkdir(dir_name)
        for file_name, file_data in files.iteritems():
            f = d.join(file_name)
            f.write(file_data['contents'])

    return {'root': p, 'files': fixtures}


def test_schema(fixtures):
    """Basic sanity test of fixtures used"""
    assert set(db.database_proxy.get_tables()) == {
        u'storagegroup', u'storagenode',
        u'acqtype', u'archiveinst', u'archiveacq',
        u'filetype', u'archivefile',
        u'archivefilecopyrequest', u'archivefilecopy',
        u'zabinfo', u'quuxinfo', u'zxcinfo', u'spqrinfo', u'loginfo'
    }
    assert fixtures['root'].basename == 'ROOT'
    assert st.StorageNode.get(st.StorageNode.name == 'x').root == fixtures['root']

    tmpdir = fixtures['root']
    assert len(tmpdir.listdir()) == 2
    acq_dir = tmpdir.join("12345678T000000Z_inst_zab")
    assert len(acq_dir.listdir()) == 2


def test_import(fixtures):
    tmpdir = fixtures['root']

    acq_dir = tmpdir.join('12345678T000000Z_inst_zab')

    node = st.StorageNode.get(st.StorageNode.name == 'x')

    # import for hello.txt should be ignored while creating the acquisition
    # because 'zab' acq type only tracks *.zxc and *.log files
    auto_import.import_file(node, node.root, acq_dir.basename, 'hello.txt')
    assert ac.ArchiveInst.get(ac.ArchiveInst.name == 'inst') is not None
    assert ac.AcqType.get(ac.AcqType.name == 'zab') is not None

    # the acquisition is still created
    acq = ac.ArchiveAcq.get(ac.ArchiveAcq.name == acq_dir.basename)
    assert acq is not None
    assert acq.name == acq_dir.basename
    assert acq.inst.name == 'inst'
    assert acq.type.name == 'zab'

    # while no file has been imported yet
    assert (ac.ArchiveFile
            .select()
            .where(ac.ArchiveFile.acq == acq)
            .count()) == 0

    # now import 'ch_master.log', which should succeed
    auto_import.import_file(node, node.root, acq_dir.basename, 'ch_master.log')
    file = ac.ArchiveFile.get(ac.ArchiveFile.name == 'ch_master.log')
    assert file is not None
    assert file.acq == acq
    assert file.type.name == 'log'
    assert file.size_b == len(fixtures['files'][acq_dir.basename][file.name]['contents'])
    assert file.md5sum == fixtures['files'][acq_dir.basename][file.name]['md5']

    file_copy = ar.ArchiveFileCopy.get(ar.ArchiveFileCopy.file == file,
                                       ar.ArchiveFileCopy.node == node)
    assert file_copy is not None
    assert file_copy.file == file
    assert file_copy.has_file == 'Y'
    assert file_copy.wants_file == 'Y'

    # re-importing ch_master.log should be a no-op
    auto_import.import_file(node, node.root, acq_dir.basename, 'ch_master.log')

    assert list(ac.ArchiveFile
                .select()
                .where(ac.ArchiveFile.name == 'ch_master.log')
               ) == [ file ]

    assert list(ar.ArchiveFileCopy
                .select()
                .where(ar.ArchiveFileCopy.file == file,
                       ar.ArchiveFileCopy.node == node)
               ) == [ file_copy ]


def test_import_existing(fixtures):
    """Checks for importing from an acquisition that is already in the archive"""
    tmpdir = fixtures['root']

    acq_dir = tmpdir.join('x')

    node = st.StorageNode.get(st.StorageNode.name == 'x')

    assert (ac.ArchiveAcq
            .select()
            .where(ac.ArchiveAcq.name == 'x')
            .count()) == 1

    ## import an unknown file
    auto_import.import_file(node, node.root, acq_dir.basename, 'foo.log')
    assert (ar.ArchiveFileCopy
            .select()
            .join(ac.ArchiveFile)
            .where(ac.ArchiveFile.name == 'foo.log')
            .count()
    ) == 1

    ## import file for which ArchiveFile entry exists but not ArchiveFileCopy
    assert (ar.ArchiveFileCopy  # no ArchiveFileCopy for 'jim'
            .select()
            .join(ac.ArchiveFile)
            .where(ac.ArchiveFile.name == 'jim')
            .count()
    ) == 0
    auto_import.import_file(node, node.root, acq_dir.basename, 'jim')
    assert (ar.ArchiveFileCopy  # now we have an ArchiveFileCopy for 'jim'
            .select()
            .join(ac.ArchiveFile)
            .where(ac.ArchiveFile.name == 'jim')
            .count()
    ) == 1


def test_import_corrupted(fixtures):
    """Checks for importing from an acquisition that is already in the archive"""
    tmpdir = fixtures['root']

    acq_dir = tmpdir.join('x')

    node = st.StorageNode.get(st.StorageNode.name == 'x')
    ## reimport a file for which we have a copy that is corrupted
    assert list(ar.ArchiveFileCopy
                .select(ar.ArchiveFileCopy.has_file,
                        ar.ArchiveFileCopy.wants_file)
                .join(ac.ArchiveFile)
                .where(ac.ArchiveFile.name == 'sheila')
                .dicts()) == [
                    {'has_file': 'X', 'wants_file': 'M'}
    ]
    auto_import.import_file(node, node.root, acq_dir.basename, 'sheila')
    assert list(ar.ArchiveFileCopy
                .select(ar.ArchiveFileCopy.has_file,
                        ar.ArchiveFileCopy.wants_file)
                .join(ac.ArchiveFile)
                .where(ac.ArchiveFile.name == 'sheila')
                .dicts()) == [
                    {'has_file': 'M', 'wants_file': 'Y'}
    ]
