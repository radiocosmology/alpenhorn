"""BaseIO classes.

Provides the basic infrastructure for StorageNode and StorageGroup I/O.

These are very low-level classes.  Any module implementing the I/O class for
something even remotely resembling a POSIX filesystem may be better served
by subclassing from DefaultIO instead of from here directly.
"""
import json
import os.path
from peewee import fn

from ..config import merge_dict_tree


# Comment from DVW:
#
# Separating BaseNodeIO and BaseNodeRemote is primarily to avoid the temptaiton
# of accidentally writing code that tries to perform I/O operations on non-local
# nodes.
class BaseNodeRemote:
    """alpenhorn.io.BaseNodeRemote is the base class for StorageNode remote I/O
    modules.

    The Remote I/O modules provide read-only information about a non-local
    StorageNode."""

    def __init__(self, node, config):
        self.node = node
        self.config = config

    def pull_ready(self, file):
        """Is the source file ready for pulling from the remote node?"""
        # By default, nothing is ready.
        return False

    def file_path(self, file):
        """Return a path on the remote system pointing to ArchiveFile file.

        By default, returns the path contcatenation of node.root and
        file.path."""
        return pathlib.PurePath(self.node.root, file.path)

    def file_addr(self, file):
        """Return a file address suitable for use with an rsync.

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

    @property
    def idle(self):
        """Boolean indicating whether node I/O is occurring.

        By default, True whenever the queue FIFO associated with this node is empty;
        False otherwise."""
        return self._queue.fifo_size(self.node.name) == 0

    def before_update(self, idle):
        """Pre-update hook.

        Called each update loop before node updates happen.

        If idle is False, updates for this node are going to be skipped loop cycle.

        This method should return a boolean indicating whether to proceed with the update or
        not (skip it.)  If this method returns False, the update is skipped.

        Whether or not the update occurs, the after_update() will be called.
        """
        # By default, we do nothing and allow the update to continue
        return True

    def idle_update(self):
        """Idle update hook.

        Called after a regular update that wasn't skipped, but only if after
        the regular update, there were no tasks pending or in progress this node
        (i.e. self.idle is True).

        This is the place to put low-priority tasks that should only happen
        if no other I/O is happening on the node.

        The return value of this function is ignored.
        """
        # By default do nothing.
        pass

    def after_update(self, update_result):
        """Post-update hook.  Called at the end of the update loop.

        Parameters
        ----------
        - update_result : boolean or None
            Indicates what parts of the update were performed this loop:
                - None: update was skipped
                - True: regular update followed by idle update
                - False: regular update only

        This method is called once per update loop, after all other processing has happened
        on the node.

        The value returned by this function is ignored.
        """
        # Do nothing
        pass

    def check_active(self):
        """check_active: Check whether a node is active.

        This check should be done by inspecting the storage system, rather than
        checking the database, because is meant to catch instances where the
        "active" bit in the database is incorrect.

        Returns True if the node is active, or False if inactive.

        If this can't be determined, it should return self.node.active (i.e.
        assume the database is correct), which is the default behaviour."""
        return self.node.active

    def bytes_avail(self, fast=False):
        """bytes_avail: Return amount of free space (in bytes) of the node, or
        None if that cannot be determined.

        Note: this is a measure of free space on the underlying storage system,
        not how close to node.max_total_gb the value of self.size_bytes() is.
        The value returned may exceed node.max_total_gb.

        If fast is True, then this is a fast call, and I/O classes for which
        checking available space is expensive may skip it by returning None.
        """
        return None

    def update_avail_gb(self, fast=False):
        """Calculate the free space on the node and update the database with it.

        The free space is found by calling self.bytes_avail().

        If fast is True, then this is a fast call, and I/O classes for which
        checking available space is expensive may skip it.
        """

        new_avail = self.bytes_avail(fast)

        # If this was a fast call and the result was None, ignore it.  (On a
        # slow call, None is honoured and written to the database.)
        if fast and new_avail is None:
            return

        # The value in the database is in GiB (2**30 bytes)
        if new_avail is None:
            self.node.avail_gb = None
        else:
            self.node.avail_gb = new_avail / 2**30
        self.node.avail_gb_last_checked = datetime.datetime.now()

        # Update the DB with the free space but don't clobber changes made
        # manually to the database
        self.node.save(
            only=[st.StorageNode.avail_gb, st.StorageNode.avail_gb_last_checked]
        )

        if new_avail is None:
            log.info(f'Unable to determine available space for "{node.name}".')
        else:
            log.info(f'Node "{node.name}" has {node.avail_gb:.2f} GiB available.')

        return new_avail

    def file_walk(self):
        """Iterate over file copies

        Should successively yield a pathlib.PurePath for each file copy on the
        node.  The returned path may either be absolute (i.e have node.root
        pre-pended) or else be relative to node.root.  The former is preferred.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def exists(self, path):
        """Returns a boolean indicating whether the file path exists or not.

        path is relative to the root."""
        raise NotImplementedError("method must be re-implemented in subclass.")

    def locked(self, acqname, filename):
        """Is file `acqname`/`filename` locked by the presence of a lockfile
        called `acqname`/.`filename`.lock.
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

    @property
    def idle(self):
        """Boolean indicating whether group I/O is occurring."""
        raise NotImplementedError("method must be re-implemented in subclass.")

    def before_update(self, nodes, idle):
        """Pre-update hook

        Parameters
        ----------
        - nodes : list of StorageNodes
                The list of local active nodes.  Will never be empty.
        - idle : boolean
                If False, the update loop is going to be skipped.

        This method is called once per update loop, before any other processing
        happens on this group.  Before this function is called,
        node.io.set_queue(), has been called on every StorageNode in the nodes
        to initialise their I/O layer.

        If idle is True, after each call of this method, I/O may occur on the
        nodes passed in.  So, the GroupIO class should remember the nodes passed
        to this function, if they are needed.

        NB: the `idle` value passed in here is the logical AND of all the
        node.io.idle properties of the list of nodes passed in evaluated at the
        top of the update loop (i.e. before the node updates happened).  It is
        _not_ the value of self.idle.

        This method should return a boolean indicating whether to proceed with
        the update or not (skip it.)  If this method returns False, the update
        is skipped.
        """
        # By default, we do nothing and allow the update to continue
        return True

    def exists(self, path):
        """Checks whether a file called path exists in this group.

        Returns the StorageNode containing the file, or None if no
        file was found.
        """
        raise NotImplementedError("method must be re-implemented in subclass.")

    def idle_update(self):
        """Idle update hook.

        Called after a regular update that wasn't skipped, but only if after
        the regular update, there were no tasks pending or in progress this
        group (i.e. self.idle is True).

        This is the place to put low-priority tasks that should only happen
        if no other I/O is happening on the group.

        The return value of this function is ignored.
        """
        # By default do nothing.
        pass

    def after_update(self, update_result):
        """Post-update hook.

        Parameters
        ----------
        - update_result : boolean or None
            Indicates what parts of the update were performed this loop:
                - None: update was skipped
                - True: regular update followed by idle update
                - False: regular update only

        This method is called once per update loop, after all other processing
        has happened on the group.  The only thing that will be called after
        this are the per-node after_update hooks.

        The value returned by this function is ignored.
        """
        # Do nothing
        pass

    def pull(self, req):
        """Handle ArchiveFileCopyRequest req whose destination is this group."""
        raise NotImplementedError("method must be re-implemented in subclass.")
