"""Alpenhorn service."""

import sys
import click
import logging

from .queue import FairMultiFIFOQueue

from . import (
    acquisition,
    auto_import,
    config,
    db,
    extensions,
    logger,
    pool,
    queue,
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

    # Load acquisition & file info classes
    acquisition.import_info_classes()

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

    # Enter main loop.
    try:
        update.update_loop(host, queue, wpool)

        # Global abort
        if pool.global_abort.is_set():
            log.warning("Exiting due to global abort")

    # Catch keyboard interrupt
    except KeyboardInterrupt:
        log.info("Exiting due to SIGINT")

    # Attempt to exit cleanly
    auto_import.stop_observers()
    wpool.shutdown()
