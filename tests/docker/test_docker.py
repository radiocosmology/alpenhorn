

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import pytest
import docker

client = docker.from_env()


@pytest.fixture(scope='module')
def images():
    """Build the images for the tests."""

    print('Building docker images...')

    # Build base image
    client.images.build(
        path='../../', tag='jrs65/python-mysql', rm=True, forcerm=True,
        dockerfile='tests/docker/Dockerfile.base'
    )

    # Build alpenhorn image
    client.images.build(
        path='../../', tag='alpenhorn', rm=True, forcerm=True,
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
    """Set up the database and create the tables for alpenhorn."""

    print ('Creating the database...')

    # Create the database container
    db = client.containers.run(
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

    yield db

    print('Cleaning up db container...')
    _stop_or_kill(db)
    db.remove()


@pytest.fixture(scope='module')
def workers(db, network, images):
    """Create a group of alpenhorn entries."""

    node_names = ['node_a', 'node_b', 'node_c']

    def create_worker(name):
        print('Creating alpenhorn container %s' % name)
        return client.containers.run('alpenhorn', name=name, detach=True,
                                     network_mode=network)

    nodes = {name: create_worker(name) for name in node_names}

    yield nodes

    for container in nodes.values():
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

    time.sleep(10)
