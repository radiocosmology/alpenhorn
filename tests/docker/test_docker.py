

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import time
import os
from os.path import join, dirname, exists

import pytest
pytestmark = pytest.mark.skipif(
    'RUN_DOCKER_TESTS' not in os.environ,
    reason=('Docker tests must be enabled by setting the RUN_DOCKER_TEST environment variable')
)

import yaml

from alpenhorn import acquisition as ac
from alpenhorn import archive as ar
from alpenhorn import storage as st


# Try and import docker.
try:
    import docker
    client = docker.from_env()
except ImportError:
    pass


# ====== Fixtures for controlling Docker ======

@pytest.fixture(scope='module')
def images():
    """Build the images for the tests."""

    import os.path

    context = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))

    print('Building docker images from location %s...' % context)

    # Build base image
    client.images.build(
        path=context, tag='jrs65/python-mysql', rm=True, forcerm=True,
        dockerfile='tests/docker/Dockerfile.base'
    )

    # Build alpenhorn image
    client.images.build(
        path=context, tag='alpenhorn', rm=True, forcerm=True,
        dockerfile='tests/docker/Dockerfile.alpenhorn'
    )


@pytest.fixture(scope='module')
def network():
    """Set up the network."""
    # Note to connect to this network you need to pass network_mode=networks to
    # .run(). See https://github.com/docker/docker-py/issues/1433

    print('Setting up the network...')

    network = client.networks.create("alpenhorn-net", driver="bridge")

    yield network.name

    network.remove()


@pytest.fixture(scope='module')
def db(network, images):
    """Set up the database and create the tables for alpenhorn.

    Also connect peewee to this database, so we can query its state."""

    from alpenhorn import db

    print ('Creating the database...')

    # Create the database container
    db_container = client.containers.run(
        'mysql:latest', name='db', detach=True,
        network_mode=network, ports={'3306/tcp': 63306},
        environment={'MYSQL_ALLOW_EMPTY_PASSWORD': 'yes'}
    )

    # Wait until the MySQL instance is properly up
    client.containers.run(
        'alpenhorn', remove=True, detach=False, network_mode=network,
        command="bash -c 'while ! mysqladmin ping -h db --silent; do sleep 3; done'"
    )

    # Create the database
    client.containers.run(
        'alpenhorn', remove=True, detach=False, network_mode=network,
        command="mysql -h db -e 'CREATE DATABASE alpenhorn_db'"
    )

    print('Creating the tables...')

    # Initialise alpenhorn
    client.containers.run(
        'alpenhorn', remove=True, detach=False, network_mode=network,
        command="alpenhorn init"
    )

    # Connect our peewee models to the database
    db._connect(url='mysql://root@127.0.0.1:63306/alpenhorn_db')

    yield db_container

    # Take down the peewee connection
    db.database_proxy.close()

    print('Cleaning up db container...')
    _stop_or_kill(db_container)
    db_container.remove()


@pytest.fixture(scope='module')
def workers(db, network, images, tmpdir_factory):
    """Create a group of alpenhorn entries."""

    workers = []

    for i in range(3):

        hostname = 'container-%i' % i
        print('Creating alpenhorn container %s' % hostname)

        # Create db entries for the alpenhorn instance
        group = st.StorageGroup.create(name=('group_%i' % i))
        node = st.StorageNode.create(
            name=('node_%i' % i), root='/data', group=group, host=hostname,
            address=hostname, mounted=True, auto_import=True, min_avail_gb=0.0
        )

        # Create a temporary directory on the host to store the data, which will
        # get mounted into the container
        data_dir = str(tmpdir_factory.mktemp(hostname))
        print('Node directory (on host): %s' % str(data_dir))

        container = client.containers.run(
            'alpenhorn', name=hostname, detach=True, network_mode=network,
            volumes={data_dir: {'bind': '/data', 'mode': 'rw'}}
        )

        workers.append({'node': node, 'container': container, 'dir': data_dir})

    yield workers

    # Cleanup
    for worker in workers:
        container = worker['container']
        print('Stopping and removing alpenhorn container %s' % container.name)
        _stop_or_kill(container, timeout=1)
        container.remove()


def _stop_or_kill(container, timeout=10):
    # Work around for:
    # https://github.com/docker/docker-py/issues/1374
    import requests.exceptions

    try:
        container.stop(timeout=timeout)
    except requests.exceptions.ReadTimeout:
        container.kill()


# ====== Fixtures for generating test files ======

@pytest.fixture(scope='module')
def test_files():
    """Get a set of test files.

    Read the test files config, and structure it into acquisitions and files,
    labelling each with their respective types.
    """

    files = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'fixtures', 'files.yml'))

    with open(files, 'r') as f:
        fs = yaml.safe_load(f.read())

    acqs = _recurse_acq(fs)

    return acqs


def _recurse_acq(f, root=''):
    """Recurse over a dictionary based tree, and find the acquisitions and their files.
    """

    def _type(x):
        if 'zab' in x:
            return 'zab'
        elif 'quux' in x or x == 'x':
            return 'quux'
        else:
            return None

    acqlist = []

    for name, sub in f.items():

        new_root = join(root, name)

        if _type(new_root) is not None:
            acqlist.append({
                'name': new_root, 'type': _type(new_root),
                'files': _recurse_files(sub)
            })
        else:
            acqlist += _recurse_acq(sub, root=join(root, name))

    return acqlist


def _recurse_files(f, root=''):
    """Recurse over a dictionary tree at the acq root, and get the files."""

    def _type(x):
        if x[-4:] == '.log':
            return 'log'
        elif x[-4:] == '.zxc' or x == 'jim':
            return 'zxc'
        elif x[-5:] == '.lock':
            return 'lock'

    filelist = []

    for name, sub in f.items():

        new_root = join(root, name)

        if 'md5' in sub:
            fileprop = {'name': new_root, 'type': _type(new_root)}
            fileprop.update(sub)
            filelist.append(fileprop)
        else:
            filelist += _recurse_files(sub, root=new_root)

    return filelist


def _make_files(acqs, base, skip_lock=True):

    for acq in acqs:

        for file_ in acq['files']:

            path = join(base, acq['name'], file_['name'])

            if not exists(dirname(path)):
                os.makedirs(dirname(path))

            if not skip_lock or file_['type'] != 'lock':
                with open(path, 'w') as fh:
                    fh.write(file_['contents'])


# ===== Test the auto_import behaviour =====

def test_import(workers, test_files):

    # Create the files
    _make_files(test_files, workers[0]['dir'], skip_lock=True)

    time.sleep(2)

    node = workers[0]['node']

    for acq in test_files:

        # Test that the acquisition exists
        acq_query = ac.ArchiveAcq.select().where(ac.ArchiveAcq.name == acq['name'])
        assert acq_query.count() == 1
        acq_obj = acq_query.get()

        # Test that it has the correct type
        assert acq_obj.type.name == acq['type']

        for file_ in acq['files']:

            # Test that the file exists
            file_query = ac.ArchiveFile.select().where(
                ac.ArchiveFile.acq == acq_obj,
                ac.ArchiveFile.name == file_['name']
            )

            assert file_query.count() == 1
            file_obj = file_query.get()

            # Test that it has the correct type
            assert file_obj.type.name == file_['type']

            # Test that this node has a copy
            copy_query = ar.ArchiveFileCopy.select().where(
                ar.ArchiveFileCopy.file == file_obj,
                ar.ArchiveFileCopy.node == node
            )

            assert copy_query.count() == 1
            copy_obj = copy_query.get()

            assert copy_obj.has_file == 'Y'
            assert copy_obj.wants_file == 'Y'


def test_stuff(workers):

    import time

    #time.sleep(10)
    raw_input('Press a key.')
