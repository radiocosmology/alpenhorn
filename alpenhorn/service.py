"""Alpenhorn service."""

import logging
import sys

import click

from .queue import FairMultiFIFOQueue
from .update import update_loop

from . import auto_import, config, db, extensions, logger, storage, util, pool

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
    node_have = False
    for node in storage.StorageNode.select().where(
        storage.StorageNode.host == host, storage.StorageNode.active
    ):
        node_have = True  # we have node

        # Skip all this if not auto importing
        if node.auto_import:
            # Init the I/O layer
            node.io.set_queue(queue)

            # Start the observer to watch the nodes for new files
            auto_import.update_observer(node, queue)

            # Now catch up with the existing files to see if there are any new ones
            # that should be imported
            auto_import.catchup(node, queue)

    # Warn if there are no active nodes. We used to exit here, but actually
    # it's useful to keep alpenhornd running for nodes where we exclusively use
    # transport disks (e.g. jingle)
    if not node_have:
        log.warn(f"No nodes on this host ({host}) registered in the DB!")

    # Enter main loop.
    #
    # At this point, if any auto_import.catchup happened, the queue may be
    # crammed full of import_file tasks.  If that's the case, it may take a
    # few main loops to work through the backlog during which time not much
    # actual updating is going to happen, but that's not worse than the old
    # way which would simply delay starting the main loop until the crawl
    # was complete.
    try:
        update_loop(host, queue, wpool)

        # Global abort
        if pool.global_abort.is_set():
            log.warning("Exiting due to global abort")

    # Catch keyboard interrupt
    except KeyboardInterrupt:
        log.info("Exiting due to SIGINT")

    # Attempt to exit cleanly
    auto_import.stop()
    wpool.shutdown()
