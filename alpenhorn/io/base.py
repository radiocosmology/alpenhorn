"""BaseIO classes.

All IO classes must sublcass from these base classes to be recognised as valid.

These are very low-level classes.  Any module implementing the I/O class for
something even remotely resembling a POSIX filesystem would be better served
by subclassing from DefaultIO instead of from here directly.
"""
import json
import os.path
from peewee import fn

from .. import archive as ar
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
        """Return a path on the remote system pointing to ArchiveFile file.

        By default, returns the path contcatenation of node.root and file.path."""
        return pathlib.PurePath(self.node.root, file.path)

    def file_addr(self, file):
        """Return a file address suitable for use as an rsync source or destination

        i.e., a string of the form: <user>@<address>:<path>

        Raises ValueError if username or address are not set."""
        if self.node.username is None:
            raise ValueError("missing username")
        if self.node.address is None:
            raise ValueError("missing address")

        return f"{self.node.username}@{self.node.address}:{self.file_path()}"


class BaseNodeIO:
    """alpenhorn.io.BaseNodeIO is the base class for StorageNode I/O modules in
    alpenhorn.
    """

    # Subclasses should set this to a BaseNodeRemote-derived class.
    remote_class = BaseNodeRemote

    # A class compatible with watchdog.observers.api.BaseObserver which will
    # be used as the auto import observer.
    #
    # This can be set to None if no observation is possible, though the
    # platform-independent watchdog.observers.polling.PollingObserver
    # will work in the vast majority of cases, if others do not.
    observer = None

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
        """Returns an instance of the remote-I/O class for this node.

        In general, this function should not be called outside of the
        StorageNode class internals.  Access the remote-I/O class via
        node.remote instead.
        """

        if issubclass(self.remote_class, BaseNodeRemote):
            return self.remote_class(self.node, self.config)

        raise TypeError(
            f'Remote I/O class "{self.remote_class}" for StorageNode {obj.name}'
            "does not descend from alpenhorn.io.BaseNodeRemote"
        )

    def set_queue(self, queue):
        """Set the queue used for asynchronous I/O tasks."""
        self._queue = queue

    def before_update(self, queue_empty):
        """Pre-update hook.

        Called each update loop before node updates happen.

        If queue_empty is False, updates for this node are going to be skipped
        loop cycle.

        This method should return a boolean indicating whether to cancel (skip) the update
        or not.  If this method returns True, the update is skipped.

        Whether or not the update occurs, the after_update() will be called.
        """
        # By default, we do nothing and allow the update to continue
        return False

    def after_update(self, queue_empty, cancelled):
        """Post-update hook.

        Parameters
        ----------
        - queue_empty : boolean
                If False, the update was skipped because the queue was not empty.
        - cancelled : boolean
                The value returned by before_update().  If True, the update was skipped.

        This method is called once per update loop, after all other processing has happened
        on the node.

        If queue_empty is True and cancelled is False, then the update occurred.  Otherwise
        it was skipped.

        The value returned by this function is ignored.
        """
        # Do nothing
        pass

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

    def file_walk(self):
        """Iterate over file copies

        Should successively yield a pathlib.PurePath for each file copy on the
        node.  The returned path may either be absolute (i.e have node.root
        pre-pended) or else be relative to node.root.  The former is preferred.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def lockfile_present(self, acqname, filename):
        """Is file `acqname`/`filename` locked by the presence of a lockfile called
        `acqname`/.`filename`.lock.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def md5sum_file(self, path):
        """Return the MD5 sum of file acqname/filename"""
        raise NotImplementedError("method must be re-implemented in subclass.")

    def filesize(self, path, actual=False):
        """Return size in bytes of the file given by path.

        If acutal is True, return the amount of space the file actually takes
        up on the storage system.  Otherwise return apparent size.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def pull(self, req):
        """Perform ArchiveFileCopyRequest req.

        Pull req.file from req.node onto the local storage system."""
        raise NotImplementedError("method must be re-implemented in subclass.")

    def check(self, copy):
        """Check whether ArchiveFileCopy `copy` is corrupt."""
        raise NotImplementedError("method must be re-implemented in subclass.")

    def delete(self, copies):
        """Delete the ArchiveFileCopy list `copies` from the node.

        len(copies) may be zero.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def ready(self, req):
        """Ready a remote pull specified by req on the source node.

        Passed the ArchiveFileCopyRequest req for the transfier
        (so req.node_from == self.node).

        This method is called for all pending requests, even ones that are
        impossible due to the file being corrupt, missing, or some other calamity.
        If such an impossibility arises, this method _may_ cancel the request,
        but that's not required.  (It's the responsibility of the pulling node
        to resolve the request.)
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

    def before_update(self, nodes, queue_empty):
        """Pre-update hook

        Parameters
        ----------
        - nodes : list of StorageNodes
                The list of local active nodes.  Will never be empty.
        - queue_empty : boolean
                If False, the update loop is going to be skipped.

        This method is called once per update loop, before any other processing happens
        on this group.

        If queue_empty is True, after each call of this method, I/O may occur on the
        nodes passed in.  So, the GroupIO class should remember the nodes passed to this
        function, if they are needed.

        If queue_emtpy is False, the update will be skipped and the after_update() hook
        will be immediately called next.

        This method should return a boolean indicating whether to cancel (skip) the update
        or not.  If this method returns True, the update is skipped and the after_update()
        hook is immediately called.

        """
        # By default, we do nothing and allow the update to continue
        return False

    def after_update(self, queue_empty, cancelled):
        """Post-update hook.

        Parameters
        ----------
        - queue_empty : boolean
                If False, the update was skipped because the queue was not empty.
        - cancelled : boolean
                The value returned by before_update().  If True, the update was skipped.

        This method is called once per update loop, after all other processing has happened
        on the group.

        If queue_empty is True and cancelled is False, then the update occurred.  Otherwise
        it was skipped.

        The value returned by this function is ignored.
        """
        # Do nothing
        pass

    def set_queue(self, queue):
        """Use queue for asynchronous I/O tasks.

        Called after check_available_nodes() each update loop.  This method should call
        node.io.set_queue on any nodes which are going to perform I/O for the group during the
        current update loop."""
        pass

    def pull(self, req):
        """Handle ArchiveFileCopyRequest req whose destination is this group."""
        raise NotImplementedError("method must be re-implemented in subclass.")
