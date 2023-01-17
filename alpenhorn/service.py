"""Alpenhorn service."""

import logging
import sys

import click

from .queue import FairMultiFIFOQueue

from . import (
    acquisition,
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
        wpool = pool.WorkerPool(num_workers=default_num_workers, queue=queue)
    else:
        # EmptyPool acts like WorkerPool, but always has zero workers
        wpool = pool.EmptyPool()

    # Set up worker increment/decrement signals
    pool.setsignals(wpool)

    # Get the name of this host
    host = util.get_hostname()

    # Loop over nodes active on this host looking for auto-imported ones
    for node in storage.StorageNode.select().where(
        storage.StorageNode.host == host, storage.StorageNode.active
    ):
        # Skip all this if not auto importing
        if node.auto_import:
            # Init the I/O layer
            node.io.set_queue(queue)

            # Start the observer to watch the nodes for new files
            auto_import.update_observer(node, queue)

            # Now catch up with the existing files to see if there are any new ones
            # that should be imported
            auto_import.catchup(node, queue)

    # Enter main loop.
    #
    # At this point, if any auto_import.catchup happened, the queue may be
    # crammed full of import_file tasks.  If that's the case, it may take a
    # few main loops to work through the backlog during which time not much
    # actual updating is going to happen, but that's not worse than the old
    # way which would simply delay starting the main loop until the crawl
    # was complete.
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
