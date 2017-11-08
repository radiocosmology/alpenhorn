

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import time
import os
from os.path import join, dirname, exists

import pytest


pytestmark = pytest.mark.skipif(
    (('RUN_DOCKER_TESTS' not in os.environ) and ('PLAYGROUND' not in os.environ)),
    reason=('Docker tests must be enabled by setting the RUN_DOCKER_TESTS environment variable')
)


import yaml

from alpenhorn import acquisition as ac
from alpenhorn import archive as ar
from alpenhorn import storage as st


# Try and import docker.
try:
    import docker
    client = docker.from_env()
except (ImportError, AttributeError):
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
        'alpenhorn', remove=True, detach=False, network=network,
        command="bash -c 'while ! mysqladmin ping -h db --silent; do sleep 3; done'"
    )

    # Create the database
    client.containers.run(
        'alpenhorn', remove=True, detach=False, network=network,
        command="mysql -h db -e 'CREATE DATABASE alpenhorn_db'"
    )

    print('Creating the tables...')

    # Initialise alpenhorn
    client.containers.run(
        'alpenhorn', remove=True, detach=False, network=network,
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
            name=('node_%i' % i), root='/data', username='root',
            group=group, host=hostname, address=hostname, mounted=True,
            auto_import=(i == 0), min_avail_gb=0.0
        )

        # Create a temporary directory on the host to store the data, which will
        # get mounted into the container
        data_dir = str(tmpdir_factory.mktemp(hostname))
        print('Node directory (on host): %s' % str(data_dir))

        container = client.containers.run(
            'alpenhorn', name=hostname, hostname=hostname, network_mode=network,
            detach=True, volumes={data_dir: {'bind': '/data', 'mode': 'rw'}}
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


# ====== Helper routines for checking the database ======

def _verify_db(acqs, copies_on_node=None, wants_on_node='Y', has_on_node='Y'):
    """Verify that files are in the database.

    Parameters
    ----------
    acqs : dict
        Set of acquisitions and files as output by test_files.
    copies_on_node : StorageNode, optional
        Verify that what the database believes is on this node. If
        `None` skip this test.
    has_on_node : str, optional
        'Has' state of files to check for. Default 'Y'.
        `None` to skip test.
    wants_on_node : str, optional
        'Wants' state of files to check for. Default 'Y'.
        `None` to skip test.
    """

    # Loop over all acquisitions and files and check that they have been
    # correctly added to the database
    for acq in acqs:

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

            # Check that we haven't imported types we don't want
            if file_['type'] in [None, 'lock']:
                assert file_query.count() == 0
                continue

            assert file_query.count() == 1
            file_obj = file_query.get()

            # Test that it has the correct type
            assert file_obj.type.name == file_['type']

            if copies_on_node is not None:
                # Test that this node has a copy
                copy_query = ar.ArchiveFileCopy.select().where(
                    ar.ArchiveFileCopy.file == file_obj,
                    ar.ArchiveFileCopy.node == copies_on_node
                )

                assert copy_query.count() == 1
                copy_obj = copy_query.get()

                if has_on_node is not None: assert copy_obj.has_file == has_on_node
                if wants_on_node is not None: assert copy_obj.wants_file == wants_on_node


def _verify_files(worker):
    """Verify the files are in place using the alpenhorn verify command.
    """

    # Run alpenhron verify and return the exit status as a string
    output = worker['container'].exec_run(
        "bash -c 'alpenhorn verify %s &> /dev/null; echo $?'" %
        worker['node'].name
    )

    # Convert the output back to an exit status
    assert not int(output)


# ====== Test the auto_import behaviour ======

def test_import(workers, test_files):
    # Add a bunch of files onto node_0, wait for them to be picked up by the
    # auto_import, and then verify that they all got imported to the db
    # correctly.

    # Create the files
    _make_files(test_files, workers[0]['dir'], skip_lock=True)

    # Wait for the auto_import to catch them (it polls at 30s intervals)
    time.sleep(3)

    node = workers[0]['node']

    _verify_db(test_files, copies_on_node=node)

    _verify_files(workers[0])


# ====== Test that the sync between nodes works ======

def test_sync_all(workers, network, test_files):

    # Request sync onto a different node
    client.containers.run(
        'alpenhorn', remove=True, detach=False, network_mode=network,
        command="alpenhorn sync -f node_0 group_1"
    )

    time.sleep(3)

    _verify_db(test_files, copies_on_node=workers[1]['node'])

    _verify_files(workers[1])


def test_sync_acq(workers, network, test_files):

    for acq in test_files:

        # Request sync of a single acq onto a different node
        client.containers.run(
            'alpenhorn', remove=True, detach=False, network_mode=network,
            command=("alpenhorn sync -f node_0 group_2 --acq=%s" % acq['name'])
        )

        time.sleep(3)

        # Verify that the requested files hve been copied
        _verify_db([acq], copies_on_node=workers[1]['node'])

        _verify_files(workers[2])


# ====== Test that the clean command works ======

def _verify_clean(acqs, worker, unclean=False, check_empty=False):
    """ Check the clean command has been executed as expected on the node associated with 'worker'.
        If 'unclean' is set to True, check that files are not wanted but still present (until
        additional copies on other archive nodes are found).
    """
    # Check files are set to deleted / not deleted but not wanted in database
    for acq in acqs:
        if unclean:
            _verify_db([acq], copies_on_node=worker['node'], has_on_node='Y', wants_on_node='N')
        else:
            _verify_db([acq], copies_on_node=worker['node'], has_on_node='N', wants_on_node='N')

    # Check files are in fact gone / still there
    for acq in acqs:
        for f in acq['files']:
            # Ignore files not tracked by the database
            if f['type'] is not None and f['type'] != 'lock':
                file_exists = os.path.exists(os.path.join(worker['dir'], acq['name'], f['name']))
                assert (file_exists and unclean) or (not file_exists and not unclean)

    # If specified, check no files or directories are left over
    if not unclean and check_empty:
        assert len(os.listdir(worker['dir'])) == 0


def test_clean(workers, network, test_files):

    # Simplest clean request
    node_to_clean = workers[1]['node']
    client.containers.run(
        'alpenhorn', remove=True, detach=False, network_mode=network,
        command=("alpenhorn clean -f {}".format(node_to_clean.name))
    )

    # Check files set to 'M'
    for acq in test_files:
        _verify_db([acq], copies_on_node=node_to_clean, has_on_node='Y', wants_on_node='M')

    # Changed my mind, delete them NOW
    client.containers.run(
        'alpenhorn', remove=True, detach=False, network_mode=network,
        command=("alpenhorn clean -nf {}".format(node_to_clean.name))
    )

    # Check files have been deleted
    time.sleep(3)
    _verify_clean(test_files, workers[1])
    # Since no untracked files should be present, check root is empty
    _verify_clean(test_files, workers[1], check_empty=True)

    # Request clean on a node when only one other archive node has a copy
    # Files should not be deleted
    node_to_clean = workers[2]['node']
    client.containers.run(
        'alpenhorn', remove=True, detach=False, network_mode=network,
        command=("alpenhorn clean -nf {}".format(node_to_clean.name))
    )

    # Check files are still present
    time.sleep(3)
    _verify_clean(test_files, workers[2], unclean=True)


@pytest.mark.skipif(
    'PLAYGROUND' not in os.environ,
    reason=('Set PLAYGROUND to leave alpenhorn alive for interactive fun.')
)
def test_playground(workers):

    print("""
To connect the alpenhorn database to this instance run:

>>> from alpenhorn import db
>>> db._connect(url='mysql://root@127.0.0.1:63306/alpenhorn_db')

To interact with the individual alpenhorn instances use docker exec, e.g.

$ docker exec container_0 alpenhorn status

When you are finished playing, press enter to close the docker containers and
clean up everything.""")

    try:
        raw_input('')
    except:
        input('')
