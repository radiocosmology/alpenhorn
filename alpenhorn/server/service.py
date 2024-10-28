"""Alpenhorn service."""

import sys
import click
import logging

from .. import db
from ..common import config, extensions, logger
from ..scheduler import FairMultiFIFOQueue, pool
from . import auto_import, update

log = logging.getLogger(__name__)

# Register Hook to Log Exception
# ==============================


def log_exception(*args):
    log.error("Fatal error!", exc_info=args)


sys.excepthook = log_exception


@click.command()
def cli():
    """Alpenhorn data management service."""

    # Initialise logging
    logger.init_logging()

    # Load the configuration for alpenhorn
    config.load_config()

    # Set up logging based on config
    logger.configure_logging()

    # Load alpenhorn extensions
    extensions.load_extensions()

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

    # Enter main loop
    try:
        update.update_loop(queue, wpool)
    # Catch keyboard interrupt
    except KeyboardInterrupt:
        log.info("Exiting due to SIGINT")

    # Attempt to exit cleanly
    auto_import.stop_observers()
    wpool.shutdown()
