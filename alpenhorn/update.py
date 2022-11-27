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
from . import auto_import, util
from .task import Task
from .workers import global_abort

log = logging.getLogger(__name__)


def update_loop(host, queue, pool):
    """Loop over nodes performing any updates needed.

    Asynchronous I/O tasks may be executed by putting them on the queue.  The
    will be performed by workers in the pool."""
    while not global_abort.is_set():
        loop_start = time.time()

        # Iterate over nodes and groups and perform I/O updates.
        #
        # nodes and groups are re-queried every loop iteration so we can
        # detect changes in available storage media
        for node in st.StorageNode.select().where(st.StorageNode.host == host):
            update_node(node, queue)

        for group in (
            st.StorageGroup.join(st.StorageGroup)
            .select()
            .where(st.StorageNode.host == host)
        ):
            update_group(group, queue)

        # Respawn workers that have exited (due to DB error)
        pool.check()

        # If we have no workers, handle some queued I/O tasks
        if len(pool) == 0:
            serial_io()

        # Check the time spent so far
        loop_time = time.time() - loop_start
        log.info(f"Main loop execution was {loop_time} sec.")

        # Pool and queue info
        log.info(
            f"Tasks: {queue.qsize()} queued, {queue.inprogress_size()} in-progress on {len(pool)} workers"
        )

        # Avoid looping too fast.
        remaining = config.config["service"]["update_interval"] - loop_time
        if remaining > 0:
            global_abort.wait(remaining)  # Stops waiting if a global abort is triggered


def serial_io(queue):
    """Execute I/O tasks from the queue

    This function is only called when alpenhorn has no worker threads.  It runs
    I/O tasks in the main loop for a limited period of time.
    """
    start_time = time.time()

    # Handle tasks for, say, 15 minutes at most
    while time.time() - start_time < 900:
        # Get a task
        item = queue.get(timeout=1)

        # Out of things to do
        if item is None:
            break

        # Run the task
        key, task = item

        log.debug(f"Beginning task {task}")
        task()
        queue.task_done(key)
        log.debug(f"Finished task {task}")


def update_group(group, queue):
    """Perform I/O updates on a StorageGroup"""

    # Find all active nodes in this group on this host
    nodes_in_group = list(
        st.StorageNode.select().where(
            st.StorageNode.active == True,
            st.StorageNode.host == host,
            st.StorageNode.group == group,
        )
    )

    # Skip groups for which there are no active nodes
    if len(nodes_in_group) < 1:
        log.debug("No active nodes in group f{group.name} during update.")
        return

    # Skip groups which have ongoing asynchronous I/O tasks
    for node in nodes_in_group:
        if queue.fifo_size(node.name) > 0:
            log.info(
                "Skipping update for StorageGroup f{group.name} due to on-going I/O for constituent node f{node.name}"
            )
            return

    log.info(f'Updating group "{group.name}".')

    # Check whether the set of available nodes are sufficient to continue the update.
    if not group.io.check_available_nodes(nodes_in_group):
        return

    group.io.set_queue(queue)

    # Process pulls into this group
    for req in ar.ArchiveFileCopyRequest.select().where(
        ar.ArchiveFileCopyRequest.completed == 0,
        ar.ArchiveFileCopyRequest.cancelled == 0,
        ar.ArchiveFileCopyRequest.group_to == self.group,
    ):
        # If the destination file is already present in this group, cancel the request.
        if self.group.copy_present(req.file):
            log.info(
                f"Cancelling pull request for {req.file.acq.name}/{req.file.name}: "
                f"already present in group {group.name}."
            )
            ar.ArchiveFileCopyRequest.update(cancelled=True).where(
                ar.ArchiveFileCopyRequest.id == req.id
            ).execute()
            continue

        # Skip request unless the source node is active
        if not req.node_from.active:
            log.error(
                f"Skipping request for {req.file.acq.name}/{req.file.name}: "
                f"source node {req.node_from.name} is not active."
            )
            continue

        # If the source file doesn't exist, skip the request.
        #
        # XXX Cancel instead?
        if not req.node_from.copy_present(req.file):
            log.error(
                f"Skipping request for {req.file.acq.name}/{req.file.name}: "
                f"not available on node {req.node_from.name}. [file_id={req.file.id}]"
            )
            continue

        # If the source file is not ready, skip the request.
        if not req.node_from.remote.pull_ready(req.file):
            log.info(
                f"Skipping request for {req.file.acq.name}/{req.file.name}: "
                f"not ready on node {req.node_from.name}."
            )
            continue

        # Early checks passed: dispatch this request to the Group I/O layer
        group.io.pull(req)


def update_node(node, queue):
    """Perform I/O updates on a StorageNode"""

    # Make sure this node is usable.
    if not node.active:
        log.debug(f'Skipping inactive node "{node.name}".')
        return

    log.info(f'Updating node "{node.name}".')

    # Init I/O, if necessary.
    node.io.set_queue(queue)

    # Check if the node is actually active
    if not update_node_active(node):
        return

    # Update (start or stop) an auto-import observer for this node if needed
    auto_import.update_observer(node, queue)

    # Check and update the amount of free space
    update_node_free_space(node)

    # Check the integrity of any questionable files (has_file=M)
    for copy in ar.ArchiveFileCopy.select().where(
        ar.ArchiveFileCopy.node == self.node, ar.ArchiveFileCopy.has_file == "M"
    ):
        log.info(
            f'Checking copy "{copy.file.acq.name}/{copy.file.name}" on node {node.name}.'
        )

        # Dispatch integrity check to I/O layer
        node.io.check(copy)

    # Delete any unwanted files to cleanup space
    update_node_delete(node)

    # Process any regular transfers requests from this node
    update_node_ready_copies(node, queue)


def update_node_active(node):
    """Check if the node is actually active in the system.  Returns True if it is."""

    if node.active:
        if node.io.check_active():
            return True
        else:
            # Mark the node as inactive in the database
            node.active = False
            node.save(only=node.dirty_fields)  # save only fields that have been updated
            log.info(
                f'Correcting the database: node "f{node.name}" is now set to inactive.'
            )
    else:
        log.warning(f'Attempted to update inactive node "f{node.name}"')

    return False


def update_node_free_space(node, fast=False):
    """Calculate the free space on the node and update the database with it.

    If fast is True, then this is a fast call, and I/O classes for which checking
    available space is expensive may skip it.

    """

    new_avail = node.io.bytes_avail(fast)

    # If this was a fast call and the result was None, ignore it.  (On a slow call,
    # None is honoured and written to the database.)
    if fast and new_avail is None:
        return

    # The value in the database is in GiB (2**30 bytes)
    if new_avail is None:
        node.avail_gb = None
    else:
        node.avail_gb = new_avail / 2**30

    # Update the DB with the free space but don't clobber changes made manually to the
    # database
    node.save(only=[st.StorageNode.avail_gb])

    if new_avail is None:
        log.info(f'Unable to determine available space for "{node.name}".')
    else:
        log.info(f'Node "{node.name}" has {node.avail_gb:.2f} GiB available.')


def update_node_delete(node):
    """Process this node for files to delete."""

    # Find all file copies needing deletion on this node
    #
    # If we have less than the minimum available space, we should consider all files
    # not explicitly wanted (i.e. wants_file != 'Y') as candidates for deletion, provided
    # the copy is not on an archive node. If we have more than the minimum space, or
    # we are on archive node then only those explicitly marked (wants_file == 'N')
    # will be removed.
    if self.node.avail_gb < self.node.min_avail_gb and not self.node.archive:
        log.info(
            f"Hit minimum available space on {node.name} -- considering all unwanted "
            "files for deletion!"
        )
        dfclause = ar.ArchiveFileCopy.wants_file != "Y"
    else:
        dfclause = ar.ArchiveFileCopy.wants_file == "N"

    # Search db for candidates on this node to delete.
    del_copies = list()
    for copy in (
        ar.ArchiveFileCopy.select()
        .where(
            dfclause,
            ar.ArchiveFileCopy.node == self.node,
            ar.ArchiveFileCopy.has_file == "Y",
        )
        .order_by(ar.ArchiveFileCopy.id)
    ):
        # Group a bunch of these together to reduce the number of I/O Tasks
        # created
        #
        # TODO figure out if this actually helps
        if len(del_copies) >= 10:
            node.io.delete(del_copies)
            del_copies = [copy]
        else:
            del_copies.append(copy)

    # Handle the partial group at the end (which may be empty)
    node.io.delete(del_copies)


def update_node_ready_copies(node, queue):
    """Process file copy requests from this node."""

    for req in ar.ArchiveFileCopyRequest.select().where(
        ar.ArchiveFileCopyRequest.completed == 0,
        ar.ArchiveFileCopyRequest.cancelled == 0,
        ar.ArchiveFileCopyRequest.node_from == self.node,
    ):
        node.io.ready(req)
