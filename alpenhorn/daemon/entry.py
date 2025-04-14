"""Alpenhorn daemon entry point."""

import logging
import sys

import click

from .. import db
from ..common import config, metrics
from ..common.util import help_config_option, start_alpenhorn, version_option
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
@click.option(
    "once",
    "--exit-after-update",
    "-o",
    "--once",
    is_flag=True,
    help="Run the update loop once, wait for updates to complete, and then exit.",
)
@click.option(
    "--test-isolation",
    is_flag=True,
    help=(
        "Enable test isolation.  Using this option prevents alpenhornd "
        "from reading config from the standard config paths."
    ),
)
@version_option
@help_config_option
@click.pass_context
def entry(ctx, conf, once, test_isolation):
    """Alpenhornd: data management daemon.

    The alpenhorn daemon can be used to manage Storage Nodes.  See the alpenhorn
    documentation for details on how to run the daemon.

    By default, the daemon will keep running until killed, but you can instead tell
    it to run only a single update pass and then exit after updates have completed
    by using the "--exit-after-update" flag.
    """

    # Turn on test isolation, if requested
    config.test_isolation(enable=test_isolation)

    # Initialise alpenhorn
    start_alpenhorn(conf, cli=False)

    # Connect to the database
    db.connect()

    # Check the data index schema.  This doesn't return on mismatch
    db.schema_version(check=True)

    # Start the prometheus client, if appropriate.
    if not once:
        metrics.start_promclient()

    # Set up the task queue
    queue = FairMultiFIFOQueue()

    # If we can be multithreaded, start the worker pool
    if db.threadsafe():
        wpool = pool.WorkerPool(
            num_workers=config.config["daemon"]["num_workers"], queue=queue
        )
    else:
        log.warning("Database is not threadsafe: forcing serial I/O.")
        # EmptyPool acts like WorkerPool, but always has zero workers
        wpool = pool.EmptyPool()

    # Set up worker increment/decrement signals
    pool.setsignals(wpool)

    # Enter main loop
    try:
        result = update.update_loop(queue, wpool, once)
    # Catch keyboard interrupt
    except KeyboardInterrupt:
        log.info("Exiting due to SIGINT")
        result = 1

    # Attempt to exit cleanly
    auto_import.stop_observers()
    wpool.shutdown()

    # Exit with result
    ctx.exit(result)
