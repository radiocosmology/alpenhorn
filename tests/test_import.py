"""
test_import
------------------

Tests for `alpenhorn.auto_import` module.
"""

import pytest
import yaml
import os

import alpenhorn.db as db
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.acquisition as ac

import test_archive_model as ta


# TODO: use Pytest's directory used for tmpdir/basedir, not '/tmp'
os.environ['ALPENHORN_LOG_FILE'] = '/tmp' + '/alpenhornd.log'
import alpenhorn.auto_import as auto_import

tests_path = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def fixtures(tmpdir):
    fs = ta.fixtures().next()

    p = tmpdir.mkdir("ROOT")

    (st.StorageNode
     .update(root=str(p))
     .where(st.StorageNode.name == 'x')
     .execute())

    # TODO: we need to either handle some built-in types; or maybe move to the YAML fixtures
    ac.FileType.create(name='log')

    return {'root': p}


def test_schema(fixtures):
    """Basic sanity test of fixtures used"""
    assert set(db.database_proxy.get_tables()) == {
        u'storagegroup', u'storagenode',
        u'acqtype', u'archiveinst', u'archiveacq',
        u'filetype', u'archivefile',
        u'archivefilecopyrequest', u'archivefilecopy'
    }
    assert fixtures['root'].basename == 'ROOT'
    assert st.StorageNode.get(st.StorageNode.name == 'x').root == fixtures['root']


def test_import(fixtures):
    tmpdir = fixtures['root']

    # TODO: these should move to fixtures
    acq_dir = tmpdir.mkdir('12345678T000000Z_inst_zab')
    f = acq_dir.join("hello.txt")
    f.write('Hello world!')
    assert f.read() == 'Hello world!'
    g = acq_dir.join("ch_master.log")
    g.write('I''m the master of the world!')
    assert g.read() == 'I''m the master of the world!'
    assert len(tmpdir.listdir()) == 1
    assert len(acq_dir.listdir()) == 2

    print "hello.txt: %s"
    for arf in ac.ArchiveFile.select().where(ac.ArchiveFile.name == f.basename):
        print "  - id: %s" % arf.id

    node = st.StorageNode.get(st.StorageNode.name == 'x')

    # import for hello.txt should be ignored while creating the acquisition
    auto_import.import_file(node, node.root, acq_dir.basename, f.basename)
    assert ac.ArchiveInst.get(ac.ArchiveInst.name == 'inst') is not None
    assert ac.AcqType.get(ac.AcqType.name == 'zab') is not None

    acq = ac.ArchiveAcq.get(ac.ArchiveAcq.name == '12345678T000000Z_inst_zab')
    assert acq is not None
    assert acq.name == '12345678T000000Z_inst_zab'
    assert acq.inst.name == 'inst'
    assert acq.type.name == 'zab'

    assert (ac.ArchiveFile
            .select()
            .where(ac.ArchiveFile.name == f.basename)
            .count()) == 0

    # now import 'ch_master.log', which should succeed
    auto_import.import_file(node, node.root, acq_dir.basename, g.basename)
    file = ac.ArchiveFile.get(ac.ArchiveFile.name == 'ch_master.log')
    assert file is not None
    assert file.acq == acq
    assert file.type.name == 'log'
    assert file.size_b == 27
    assert file.md5sum == '1fb43de52e12f866a8a444e9f786e1c0'

    file_copy = ar.ArchiveFileCopy.get(ar.ArchiveFileCopy.file == file,
                                       ar.ArchiveFileCopy.node == node)
    assert file_copy is not None
    assert file_copy.file == file
    assert file_copy.has_file == 'Y'
    assert file_copy.wants_file == 'Y'
