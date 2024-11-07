"""Alpenhorn service."""

import sys
import click
import logging

from .. import db
from ..common import config
from ..common.util import start_alpenhorn, version_option
from ..scheduler import FairMultiFIFOQueue, pool
from . import auto_import, update

log = logging.getLogger(__name__)

# Register Hook to Log Exception
# ==============================


def log_exception(*args):
    log.error("Fatal error!", exc_info=args)


sys.excepthook = log_exception


@click.command()
@click.option(
    "--conf",
    "-c",
    type=click.Path(exists=True),
    help="Configuration file to read.",
    default=None,
    metavar="FILE",
)
@version_option
def cli(conf):
    """Alpenhorn data management service."""

    # Initialise alpenhorn
    start_alpenhorn(conf, client=False)

    # Connect to the database
    db.connect()

    # Set up the task queue
    queue = FairMultiFIFOQueue()

    # If we can be multithreaded, start the worker pool
    if db.threadsafe():
        wpool = pool.WorkerPool(
            num_workers=config.config["service"]["num_workers"], queue=queue
        )
    else:
        log.warning("Database is not threadsafe: forcing serial I/O.")
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
