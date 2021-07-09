"""Routines for updating the state of a node.
"""

import datetime as dt
import logging
import os
import re
import time

import peewee as pw
from peewee import fn

from . import acquisition as ac
from . import archive as ar
from . import config, db
from . import storage as st
from . import util
from . import Task
from .Task import *

log = logging.getLogger(__name__)

# Parameters.
RSYNC_OPTS = (
    "--quiet --times --protect-args --perms --group --owner " + "--copy-links --sparse"
)

# Globals.
done_transport_this_cycle = False


def update_loop(host, task_queue):
    """Loop over nodes performing any updates needed."""
    global done_transport_this_cycle

    while True:
        loop_start = time.time()
        done_transport_this_cycle = False

        # Iterate over nodes and perform each update (perform a new query
        # each time in case we get a new node, e.g. transport disk)
        for node in st.StorageNode.select().where(st.StorageNode.host == host):
            update_node(node, task_queue)

        # Check the time spent so far, and wait if needed
        loop_time = time.time() - loop_start
        log.info("Main loop execution was %d sec.", loop_time)
        remaining = config.config["service"]["update_interval"] - loop_time
        if remaining > 1:
            time.sleep(remaining)


def update_node(node, task_queue):
    """Update the status of the node, and process eligible transfers onto it."""

    # TODO: bring back HPSS support
    # Check if this is an HPSS node, and if so call the special handler
    # if is_hpss_node(node):
    #     update_node_hpss_inbound(node)
    #     return

    # Make sure this node is usable.
    if not node.active:
        log.debug('Skipping inactive node "%s".', node.name)
        return
    if node.suspect:
        log.debug('Skipping suspected node "%s".', node.name)

    log.info('Updating node "%s".', node.name)

    # Check if the node is actually active
    check_node = update_node_active(node)

    if not check_node:
        return

    # Check and update the amount of free space then reload the instance for use
    # in later routines
    update_node_free_space(node)

    # Check the integrity of any questionable files (has_file=M)
    update_node_integrity(node, task_queue)

    # Delete any upwanted files to cleanup space
    update_node_delete(node, task_queue)

    # Process any regular transfers requests onto this node
    update_node_requests(node, task_queue)

    # TODO: bring back HPSS support
    # Process any tranfers out of HPSS onto this node
    # update_node_hpss_outbound(node)


def update_node_active(node):
    """Check if a node is actually active in the filesystem"""

    if node.active:
        if util.alpenhorn_node_check(node):
            return True
        else:
            log.error(
                'Node "%s" does not have the expected ALPENHORN_NODE file', node.name
            )
    else:
        log.error('Node "%s" is not active', node.name)

    # Mark the node as inactive in the database
    node.active = False
    node.save(only=node.dirty_fields)  # save only fields that have been updated
    log.info('Correcting the database: node "%s" is now set to inactive.', node.name)

    return False


def update_node_free_space(node):
    """Calculate the free space on the node and update the database with it."""

    # Check with the OS how much free space there is
    x = os.statvfs(node.root)
    avail_gb = float(x.f_bavail) * x.f_bsize / 2 ** 30.0

    # Update the DB with the free space. Save only the dirty fields to ensure we
    # don't clobber changes made manually to the database
    node.avail_gb = avail_gb
    node.avail_gb_last_checked = dt.datetime.now()
    node.save(only=node.dirty_fields)

    log.info('Node "%s" has %.2f GB available.' % (node.name, avail_gb))


def update_node_integrity(node, task_queue):
    """Check the integrity of file copies on the node."""

    task_queue.addTask(IntegrityTask(node))


def update_node_delete(node, task_queue):
    """Process this node for files to delete."""

    task_queue.addTask(DeletionTask(node))


def update_node_requests(node, task_queue):
    """Process file copy requests onto this node."""

    # Check which type of node this is and create an appropriate task
    if node.address == "HPSS":
        task_queue.addTask(HPSSTransferTask(node))
    elif node.address == "NEARLINE":
        task_queue.addTask(NearlineTransferTask(node))
    else:
        task_queue.addTask(RegularTransferTask(node))
