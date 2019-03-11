"""
test_update
------------------

Tests for `alpenhorn.update` module.
"""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from datetime import datetime
import pytest
import yaml
import os

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import alpenhorn.db as db
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.acquisition as ac
import alpenhorn.update as update
import alpenhorn.generic as ge

import test_import as ti


tests_path = os.path.abspath(os.path.dirname(__file__))

@pytest.fixture
def fixtures(tmpdir):
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db._connect()

    fixtures = ti.load_fixtures(tmpdir)

    # create a valid ALPENHORN_NODE
    node_file = fixtures['root'].join('ALPENHORN_NODE')
    node_file.write('x')
    assert node_file.check()

    yield fixtures

    db.database_proxy.close()


def test_update_node_active(fixtures):
    tmpdir = fixtures['root']
    node = st.StorageNode.get(name='x')
    assert node.active

    # if there is a valid ALPENHORN_NODE, `update_node_active` should not
    # change node's active status
    node_file = tmpdir.join('ALPENHORN_NODE')
    assert node_file.check()
    update.update_node_active(node)
    node = st.StorageNode.get(name='x')
    assert node.active

    # rename ALPENHORN_NODE, and check that `update_node_active` unmounts the node
    node_file.rename(tmpdir.join('SOMETHING_ELSE'))
    update.update_node_active(node)
    node = st.StorageNode.get(name='x')
    assert not node.active


def test_update_node_active_no_node_file(fixtures):
    tmpdir = fixtures['root']
    node = st.StorageNode.get(name='x')
    assert node.active

    # we start off with no ALPENHORN_NODE, and so `update_node_active` should unmount it
    node_file = tmpdir.join('ALPENHORN_NODE')
    node_file.remove()
    assert not node_file.check()

    update.update_node_active(node)
    node = st.StorageNode.get(name='x')
    assert not node.active


@patch('os.statvfs')
def test_update_node_free_space(mock_statvfs, fixtures):
    node = st.StorageNode.get(name='x')
    assert node.avail_gb is None

    mock_statvfs.return_value.f_bavail = 42
    mock_statvfs.return_value.f_bsize = 2**30
    update.update_node_free_space(node)
    node = st.StorageNode.get(name='x')
    assert node.avail_gb == 42


def test_update_node_integrity(fixtures):
    tmpdir = fixtures['root']

    # we already have some files created by `import`'s fixtures
    files = os.listdir(str(tmpdir))
    assert files

    node = st.StorageNode.get(name='x')

    # create a local copy of 'jim' with the correct contents
    jim_file = ac.ArchiveFile.get(name='jim')
    jim_file.md5sum = fixtures['files']['x']['jim']['md5']
    jim_file.save(only=jim_file.dirty_fields)

    jim = ar.ArchiveFileCopy(file=jim_file, node=node, has_file='M')
    jim.save()

    # create a local copy of 'fred', but with a corrupt contents
    fred = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where((ac.ArchiveFile.name == 'fred') & (ar.ArchiveFileCopy.node == node))
                 .get())
    tmpdir.join(fred.file.acq.name, 'fred').write('')
    fred.has_file = 'M'
    fred.save(only=fred.dirty_fields)

    # create a local copy of 'sheila' but *not* in the local filesystem
    sheila = (ar.ArchiveFileCopy
                 .select()
                 .join(ac.ArchiveFile)
                 .where((ac.ArchiveFile.name == 'sheila') & (ar.ArchiveFileCopy.node == node))
                 .get())
    sheila.has_file = 'M'
    sheila.save(only=sheila.dirty_fields)

    # update_node_integrity should make 'jim' good, 'fred' corrupted, and 'sheila' missing
    update.update_node_integrity(node)
    jim = ar.ArchiveFileCopy.get(id=jim.id)
    assert jim.has_file == 'Y'
    fred = ar.ArchiveFileCopy.get(id=fred.id)
    assert fred.has_file == 'X'
    sheila = ar.ArchiveFileCopy.get(id=sheila.id)
    assert sheila.has_file == 'N'


def test_update_node_delete(fixtures):
    tmpdir = fixtures['root']

    # we already have some files created by `import`'s fixtures
    files = os.listdir(str(tmpdir))
    assert files

    node = st.StorageNode.get(name='x')
    node.avail_gb = 10
    node.save()

    # create two fake archival nodes where we will keep copies of 'fred' and 'sheila'
    node2 = st.StorageNode.create(name='w', group=node.group, storage_type='A', min_avail_gb=1, max_avail_gb=2)
    node3 = st.StorageNode.create(name='z', group=node.group, storage_type='A', min_avail_gb=1, max_avail_gb=2)

    copies = (ar.ArchiveFileCopy
                 .select()
                 .join(st.StorageNode)
                 .where(ar.ArchiveFileCopy.node == node))
    for c in copies:
        # create a local copy of the file
        tmpdir.join(c.file.acq.name, c.file.name).write('')
        c.has_file = 'Y'

        # mark the file not wanted locally
        c.wants_file = 'N'
        c.save(only=c.dirty_fields)

        # add copies of the file on the archival nodes
        ar.ArchiveFileCopy.create(file=c.file, node=node2, has_file='Y').save()
        ar.ArchiveFileCopy.create(file=c.file, node=node3, has_file='Y').save()

    # update_node_delete should mark these files not present or wanted on the
    # node, and delete them from the filesystem
    update.update_node_delete(node)
    for c in copies:
        x = ar.ArchiveFileCopy.get(id=c.id)
        assert x.has_file == 'N'
        assert x.wants_file == 'N'

        # check that the file has been deleted
        assert not tmpdir.join(c.file.acq.name, c.file.name).check()

    # the rest of `test_import`'s fixtures should be left untouched
    files = os.listdir(str(tmpdir.join('x')))
    assert files


def test_update_node_requests(tmpdir, fixtures):
    # various joins break if 'address' is NULL
    x = st.StorageNode.get(name='x')
    x.address = 'foo'
    x.save()

    # register a copy of 'jim' on 'x' in the database
    jim = ac.ArchiveFile.get(name='jim')
    jim.size_b = 0
    jim.md5sum = fixtures['files']['x']['jim']['md5']
    jim.save(only=jim.dirty_fields)
    ar.ArchiveFileCopy(file=jim, node=x, has_file='Y').save()

    # make the 'z' node available locally
    root_z = tmpdir.join("ROOT_z")
    root_z.mkdir()
    z = st.StorageNode.get(name='z')
    z.root = str(root_z)
    z.avail_gb = 300
    z.host = x.host
    z.save()

    # after catching up with file requests, check that the file has been
    # created and the request marked completed
    update.update_node_requests(z)
    req = ar.ArchiveFileCopyRequest.get(file=jim, group_to=z.group, node_from=x)
    assert req.completed

    assert root_z.join('x', 'jim').check()
    assert root_z.join('x', 'jim').read() == fixtures['root'].join('x', 'jim').read()
