"""Alpenhorn service."""

import logging
import sys

import click

from .fairmultififo import FairMultiFIFOQueue
from .update import update_loop

from . import auto_import, config, db, extensions, logger, storage, util, workers

log = logging.getLogger(__name__)

# Default number of workers.
default_num_workers = 4

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

    # Attempt to load any alpenhor extensions
    extensions.load_extensions()

    # Initialise the database framework
    db.init()

    # Connect to the database
    db.connect()

    # Regsiter any extension types
    extensions.register_type_extensions()

    # Get the name of this host
    host = util.get_short_hostname()

    # Get the list of currently nodes active
    node_list = list(
        storage.StorageNode.select().where(
            storage.StorageNode.host == host, storage.StorageNode.active
        )
    )

    # Warn if there are no active nodes. We used to exit here, but actually
    # it's useful to keep alpenhornd running for nodes where we exclusively use
    # transport disks (e.g. jingle)
    if len(node_list) == 0:
        log.warn(f"No nodes on this host ({host}) registered in the DB!")

    # Setup the observers to watch the nodes for new files
    # See https://github.com/radiocosmology/alpenhorn/issues/15
    auto_import.setup_observers(node_list, queue)

    # Now catch up with the existing files to see if there are any new ones
    # that should be imported
    auto_import.catchup(node_list)

    # Set up the task queue
    queue = FairMultiFIFOQueue()

    # If we can be multithreaded, start the worker pool
    if db.threadsafe:
        pool = workers.WorkerPool(num_workers=default_num_workers, queue=queue)
    else:
        # EmptyPool acts like WorkerPool, but always has zero workers
        pool = workers.EmptyPool()

    # Set up worker increment/decrement signals
    workers.setsignals(pool)

    # Enter main loop
    try:
        update_loop(host, queue, pool)

        # Global abort
        if workers.global_abort.is_set():
            log.warning("Exiting due to global abort")
            break

    # Exit on a keyboard interrupt
    except KeyboardInterrupt:
        log.info("Exiting due to SIGINT")

    # Attempt to exit cleanly
    auto_import.stop_observers()
    auto_import.join_observers()
    pool.shutdown()
