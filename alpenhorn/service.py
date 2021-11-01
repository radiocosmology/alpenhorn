"""Alpenhorn service."""

import logging
import sys

import click

from . import auto_import, config, db, extensions, logger, storage, update, util, Task

log = logging.getLogger(__name__)

# Parameters.
max_queue_size = 2048
num_task_threads = 4

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

    # Connect to the database using the loaded config
    db.config_connect()

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
        log.warn('No nodes on this host ("%s") registered in the DB!' % host)

    # Setup the observers to watch the nodes for new files
    auto_import.setup_observers(node_list)

    # Now catch up with the existing files to see if there are any new ones
    # that should be imported
    auto_import.catchup(node_list)

    # Setup the task queue
    task_queue = Task.TaskQueue(max_queue_size, num_task_threads)

    # Enter main loop performing node updates
    try:
        update.update_loop(host, task_queue)

    # Exit cleanly on a keyboard interrupt
    except KeyboardInterrupt:
        log.info("Exiting...")
        auto_import.stop_observers()

    # Wait for watchdog threads to terminate
    auto_import.join_observers()
