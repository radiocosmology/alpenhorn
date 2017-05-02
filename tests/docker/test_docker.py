

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import pytest
import docker

client = docker.from_env()


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

    from alpenhorn import storage as st

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
        data_dir = tmpdir_factory.mktemp(hostname)
        print('Node directory (on host): %s' % str(data_dir))

        container = client.containers.run(
            'alpenhorn', name=hostname, detach=True, network_mode=network,
            volumes={str(data_dir): {'bind': '/data', 'mode': 'rw'}}
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



def test_stuff(workers):

    import time

    #time.sleep(10)
    raw_input('Press a key.')
