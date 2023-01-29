"""Alpenhorn service."""

import sys
import click
import logging

from .queue import FairMultiFIFOQueue

from . import (
    auto_import,
    config,
    db,
    extensions,
    logger,
    pool,
    storage,
    update,
    util,
)

log = logging.getLogger(__name__)

# Register Hook to Log Exception
# ==============================


def log_exception(*args):
    log.error("Fatal error!", exc_info=args)


sys.excepthook = log_exception


@click.command()
def cli():
    """Alpenhorn data management service."""

    # Load the configuration for alpenhorn
    config.load_config()

    # Set up logging
    logger.start_logging()

    # Load alpenhorn extensions
    extensions.load_extensions()

    # Initialise the database framework
    db.init()

    # Connect to the database
    db.connect()

    # Set up the task queue
    queue = FairMultiFIFOQueue()

    # If we can be multithreaded, start the worker pool
    if db.threadsafe:
        wpool = pool.WorkerPool(
            num_workers=config.config["service"]["num_workers"], queue=queue
        )
    else:
        # EmptyPool acts like WorkerPool, but always has zero workers
        wpool = pool.EmptyPool()

    # Set up worker increment/decrement signals
    pool.setsignals(wpool)

    # Get the name of this host
    host = util.get_hostname()

    # Get the list of currently nodes active
    node_list = list(
        storage.StorageNode.select().where(
            storage.StorageNode.host == host, storage.StorageNode.active
        )
    )

    # Setup the observers to watch the nodes for new files
    # See https://github.com/radiocosmology/alpenhorn/issues/15
    auto_import.setup_observers(node_list)

    # Now catch up with the existing files to see if there are any new ones
    # that should be imported
    auto_import.catchup(node_list)

    # Enter main loop
    try:
        update.update_loop(host, queue, wpool)
    # Catch keyboard interrupt
    except KeyboardInterrupt:
        log.info("Exiting due to SIGINT")

    # Attempt to exit cleanly
    auto_import.stop_observers()
    auto_import.join_observers()
    wpool.shutdown()
