"""BaseIO classes.

All IO classes must sublcass from these base classes to be recognised as valid.
"""
import json
import os.path
from peewee import fn

from watchdog.observers.polling import PollingObserver

import alpenhorn.archive as ar
from ..config import merge_dict_tree
from ..storage import StorageNode, StorageGroup


# Comment from DVW:
#
# Separating BaseNodeIO and BaseNodeRemote is primarily to avoid the temptaiton of
# accidentally writing code that tries to perform I/O operations on non-local nodes.
class BaseNodeRemote:
    """alpenhorn.io.BaseNodeRemote is the base class for StorageNode remote I/O modules.

    The Remote I/O modules provide read-only information about a non-local StorageNode."""

    def __init__(self, node, config):
        self.node = node
        self.config = config

    def pull_ready(self, file):
        """Is the source file ready for pulling from the remote node?"""
        # By default, nothing is ready.
        return False

    def file_path(self, file):
        """Return a path on the remote system pointing to a file.

        By default, just path.joins the node root with the acq name and file name."""
        return os.path.join(self.node.root, file.acq.name, file.name)

    def file_addr(self, file):
        """Return a file address suitable for use as an rsync source or destination

        i.e., a string of the form <user>@<address>:<path>.

        Raises ValueError if username or address are not set."""
        if self.node.username is None:
            raise ValueError("missing username")
        if self.node.address is None:
            raise ValueError("missing address")

        return f"{self.node.username}@{self.node.address}:{self.file_path()}"


class BaseNodeIO:
    """alpenhorn.io.BaseNodeIO is the base class for StorageNode IO modules in alpenhorn.

    When subclassing BaseNodeIO, don't forget to set the class variable remote_class to
    an appropriate remote-I/O class."""

    # Subclasses should set this to a BaseNodeRemote-derived class.
    remote_class = BaseNodeRemote

    # Subclasses may redefine this to change how the auto_import observer works
    observer = PollingObserver  # The safe choice

    def __init__(self, node):
        self.node = node

        # Default config from the table
        self.base_config = {
            "min_delete_age_days": node.min_delete_age_days,
            "min_avail_gb": node.min_avail_gb,
        }

        # Mix in extra config if given
        if node.io_conifg is None:
            self.io_config = dict()
        else:
            self.io_config = json.loads(node.io_config)

        self.config = merge_dict_tree(self.base_config, self.io_config)

    def get_remote(self):
        """Returns an instance of the remote-I/O class for this node."""

        if issubclass(self.remote_class, BaseNodeRemote):
            return self.remote_class(self.node, self.config)

        raise TypeError(
            f'Remote I/O class "{self.remote_class}" for StorageNode {obj.name}'
            "does not descend from alpenhorn.io.BaseNodeRemote"
        )

    def set_queue(self, queue):
        """Use queue for asynchronous I/O tasks."""
        self._queue = queue

    def check_active(self):
        """check_active: Check whether a node is active.

        This check should be done by inspecting the storage system, rather than checking the
        database, because is meant to catch instances where the "active" bit in the
        database is incorrect.

        Returns True if the node is active, or False if inactive.

        If this can't be determined, it should return self.node.active (i.e. assume the database
        is correct), which is the default behaviour."""
        return self.node.active

    def bytes_avail(self, fast=False):
        """bytes_avail: Return amount of free space (in bytes) of the node, or None if
        that cannot be determined.

        Note: this is a measure of free space on the underlying storage system, not how
        close to node.max_total_gb the value of self.size_bytes() is.  The value returned
        may exceed node.max_total_gb.

        If fast is True, then this is a fast call, and I/O classes for which checking
        available space is expensive may skip it by returning None.
        """
        return None

    def pull_file(self, req, queue):
        """Perform ArchiveFileCopyRequest req.

        Pull req.file from req.node onto the local storage system."""
        raise NotImplementedError("method must be re-implemented in subclass.")

    def acq_walk(self):
        """Returns an iterator which returns the name of all acq-like directories
        on the storage system."""
        raise NotImplementedError("method must be re-implemented in subclass.")

    def file_walk(self, acqdir):
        """Returns an iterator which returns the name of all files in the acq-like
        directory called acq_name on the storage system.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")


class BaseGroupIO:
    """alpenhorn.io.BaseGroupIO is the base class for StorageGroup IO modules in alpenhorn."""

    def __init__(self, group):
        self.group = group
        if group.io_conifg is None:
            self.config = dict()
        else:
            self.config = json.loads(group.io_config)

    def check_available_nodes(self, nodes):
        """Check whether the provided list of nodes is good enough to perform an update.

        The alpenhorn daemon will pass in the list of local active nodes.  This function may
        return True to indicate an update should proceed, or else False if the update should
        be skipped based on the provided list of available nodes.

        The nodes list will never be empty.

        This method is called once per update loop.  After each call of this method, I/O should
        may occur on the nodes passed in.  So, the GroupIO class should remember the nodes passed
        to this function, assuming they are needed.
        """
        # By default, we remember nothing and cancel updating
        return False

    def set_queue(self, queue):
        """Use queue for asynchronous I/O tasks.

        Called after check_available_nodes() each update loop.  This method should call
        node.io.set_queue on any nodes which are going to perform I/O for the group during the
        current update loop."""
        pass

    def pull(self, req):
        """Handle ArchiveFileCopyRequest req whose destination is this group."""
        raise NotImplementedError("method must be re-implemented in subclass.")
