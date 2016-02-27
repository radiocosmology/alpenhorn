import sys
import socket

import click

from alpenhorn import logger
from ch_util import data_index as di
from alpenhorn import update, auto_import

log = logger.get_log()


# Register Hook to Log Exception
# ==============================

def log_exception(*args):
    log.error("Fatal error!", exc_info=args)


sys.excepthook = log_exception


@click.command()
def cli():
    """Alpenhorn data management service.
    """
    # We need write access to the DB.
    di.connect_database(read_write=True)

    # Get the name of this host
    host = socket.gethostname().split(".")[0]

    # Get the list of nodes currently mounted
    node_list = list(di.StorageNode.select().where(di.StorageNode.host == host, di.StorageNode.mounted))

    # Exit if there are no mounted nodes
    if not len(node_list):
        raise Exception("No nodes on this host (\"%s\") registered in the DB!" % host)

    # Load the cache of already imported files
    auto_import.load_import_cache()

    # Setup the observers to watch the nodes for new files
    auto_import.setup_observers(node_list)

    # Enter main loop performing node updates
    try:
        update.update_loop(node_list)

    # Exit cleanly on a keyboard interrupt
    except KeyboardInterrupt:
        auto_import.stop_observers()

    # Wait for watchdog threads to terminate
    auto_import.join_observers()
