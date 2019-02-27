"""Alpenhorn service."""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import sys
import socket
import logging

import click

from . import (extensions, db, config, logger,
               auto_import, storage, update, util)


log = logging.getLogger(__name__)


# Register Hook to Log Exception
# ==============================

def log_exception(*args):
    log.error("Fatal error!", exc_info=args)


sys.excepthook = log_exception


@click.command()
def cli():
    """Alpenhorn data management service.
    """

    # Load the configuration for alpenhorn
    config.load_config()

    # Set up logging
    logger.start_logging()

    # Attempt to load any alpenhor extensions
    extensions.load_extensions()

    # Connect to the database using the loaded config
    db.config_connect()

    # Regsiter any extension types
    extensions.register_type_extensions()

    # Get the name of this host
    host = util.get_short_hostname()

    # Get the list of nodes currently mounted
    node_list = list(storage.StorageNode.select().where(
        storage.StorageNode.host == host, storage.StorageNode.mounted
    ))

    # Warn if there are no mounted nodes. We used to exit here, but actually
    # it's useful to keep alpenhornd running for nodes where we exclusively use
    # transport disks (e.g. jingle)
    if len(node_list) == 0:
        log.warn("No nodes on this host (\"%s\") registered in the DB!" % host)

    # Setup the observers to watch the nodes for new files
    auto_import.setup_observers(node_list)

    # Now catch up with the existing files to see if there are any new ones
    # that should be imported
    auto_import.catchup(node_list)

    # Enter main loop performing node updates
    try:
        update.update_loop(host)

    # Exit cleanly on a keyboard interrupt
    except KeyboardInterrupt:
        log.info('Exiting...')
        auto_import.stop_observers()

    # Wait for watchdog threads to terminate
    auto_import.join_observers()
