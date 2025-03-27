"""StorageNode and StorageGroup table models."""

# Type annotation shennanigans
from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

import peewee as pw
from peewee import fn

from ..common import util
from ._base import EnumField, base_model

if TYPE_CHECKING:
    from .acquisition import ArchiveFile
del TYPE_CHECKING

log = logging.getLogger(__name__)


class StorageGroup(base_model):
    """Storage group for the archive.

    Attributes
    ----------
    name : string
        The group that this node belongs to (Scinet, DRAO hut, . . .).
    io_class : string
        The I/O class for this node.  See below.  If this is NULL,
        the value "Default" is used.
    notes : string
        Any notes about this storage group.
    io_config : string
        An optional JSON blob of configuration data interpreted by the
        I/O class.  If given, must be a JSON object literal.

    If `io_class` is, say, "IOClassName" then there must be a group I/O
    class called `IOClassNameGroupIO` in either:
     * the internal alpenhorn submodule `alpenhorn.io.ioclassname`, or else
     * an I/O module named "ioclassname" provided by an "io-module" extension.
    """

    name = pw.CharField(max_length=64, unique=True)
    io_class = pw.CharField(max_length=255, null=True)
    notes = pw.TextField(null=True)
    io_config = pw.TextField(null=True)

    def state_on_node(self, file: ArchiveFile) -> tuple[str, StorageNode | None]:
        """Return the state of a copy of `file` in the group.

        Returns the value of `has_file` for an ArchiveFileCopy for
        ArchiveFile `file` if found in this group, and the node it was
        found on, or "N" and None if no such copy exists.

        If multiple file copies exist in the group, then a copy with
        `has_file=="Y"` wins.  Next in priority is "M" then "X".
        "N" is only returned if all copies have `has_file=="N"`.

        Parameters
        ----------
        file : ArchiveFile
            The file to look for

        Returns
        -------
        Always returns a 2-tuple:

        filecopy_state : str
            One of:
            - 'Y' file copy exists
            - 'X' file copy is corrupt
            - 'M' file copy needs to be checked
            - 'N' file copy does not exist
        node : StorageNode or None
            The StorageNode correpsonding to filecopy_state, or None, if
            there was no node with a copy of the file.
        """
        from .archive import ArchiveFileCopy

        state = "N"
        node = None
        for copy in (
            ArchiveFileCopy.select()
            .join(StorageNode)
            .where(
                ArchiveFileCopy.node.group == self,
                ArchiveFileCopy.file == file,
            )
        ):
            if copy.has_file == "Y":
                # No need to check more
                return "Y", copy.node
            if copy.has_file == "M":
                state = "M"
                node = copy.node
            elif copy.has_file == "X" and state == "N":
                state = "X"
                node = copy.node

        return state, node


class StorageNode(base_model):
    """A place on a host where file copies are stored.

    Attributes
    ----------
    name : string
        The name of this node.
    root : string
        The root directory for data in this node.
    host : string
        The hostname that this node lives on.
    address : string
        The internet address for the host (e.g., mistaya.phas.ubc.ca)
    io_class : string
        The I/O class for this node.  See below.  If this is NULL,
        the value "Default" is used.
    group : foreign key
        The group to which this node belongs.
    active : bool
        Is the node active?
    auto_import : bool
        Should files that appear on this node be automatically added?
    auto_verify : integer
        If greater than zero, automatically re-verify file copies on this
        node during times of no other activity.  The value is the maximum
        number of files that will be re-verified per update loop.
    storage_type : enum
        What is the type of storage?
        - 'A': archival storage
        - 'T': transiting storage
        - 'F': all other storage (i.e acquisition machines)
    max_total_gb : float
        The maximum amout of storage we should use.
    min_avail_gb : float
        What is the minimum amount of free space we should leave on this
        node?
    avail_gb : float
        How much free space is there on this node?
    avail_gb_last_checked : datetime
        The UTC time when `avail_gb` was last updated.
    notes : string
        Any notes or comments about this node.
    io_config : string
        An optional JSON blob of configuration data interpreted by the
        I/O class.  If given, must be a JSON object literal.

    If `io_class` is, say, "IOClassName" then there must be a node I/O
    class called `IOClassNameNodeIO` in either:
     * the internal alpenhorn submodule `alpenhorn.io.ioclassname`, or else
     * an I/O module named "ioclassname" provided by an "io-module" extension.
    """

    name = pw.CharField(max_length=64, unique=True)
    root = pw.CharField(max_length=255, null=True)
    host = pw.CharField(max_length=64, null=True)
    username = pw.CharField(max_length=64, null=True)
    address = pw.CharField(max_length=255, null=True)
    io_class = pw.CharField(max_length=255, null=True)
    group = pw.ForeignKeyField(StorageGroup, backref="nodes")
    active = pw.BooleanField(default=False)
    auto_import = pw.BooleanField(default=False)
    auto_verify = pw.IntegerField(default=0)
    storage_type = EnumField(["A", "T", "F"], default="A")
    max_total_gb = pw.FloatField(null=True)
    min_avail_gb = pw.FloatField(default=0)
    avail_gb = pw.FloatField(null=True)
    avail_gb_last_checked = pw.DateTimeField(null=True)
    notes = pw.TextField(null=True)
    io_config = pw.TextField(null=True)

    @property
    def local(self) -> bool:
        """Is this node local to where we are running?"""
        return self.host == util.get_hostname()

    @property
    def archive(self) -> bool:
        """Is this node an archival node?"""
        return self.storage_type == "A"

    @property
    def under_min(self) -> bool:
        """Is the amount of free space below the minimum allowed?

        Will be False if `avail_gb` is None.
        """
        if self.avail_gb is None:
            return False
        return self.avail_gb < self.min_avail_gb

    def check_over_max(self) -> bool:
        """Is the total size of files on the node greater than allowed?

        Calls `self.get_total_gb()` to get the total size.

        Returns
        -------
        over_max : bool
            False if `self.max_total_gb` is `None` or less than zero.
            Also False if `self.get_total_gb()` is less than
            `self.max_total_gb`.  Otherwise True.
        """
        if self.max_total_gb is None or self.max_total_gb <= 0:
            return False
        return self.get_total_gb() >= self.max_total_gb

    def named_copy_tracked(self, acqname: str, filename: str) -> bool:
        """Is an ArchiveFileCopy named `acqname/filename` being tracked?

        "Tracked" here means an `ArchiveFileCopy` record exists for the
        file on this node with `has_file!='N'`.

        Parameters
        ----------
        acqname : str
            The name of the ArchiveAcq
        filename : str
            The name of the ArchiveFile

        Returns
        -------
        named_copy_tracked : bool
            True if there is an ArchiveFileCopy with `has_file!='N'`
            for the specified path.  False otherwise.
        """
        from .acquisition import ArchiveAcq, ArchiveFile
        from .archive import ArchiveFileCopy

        # Try to find the ArchiveFile record
        try:
            copy = (
                ArchiveFileCopy.select()
                .join(ArchiveFile)
                .join(ArchiveAcq)
                .where(
                    ArchiveAcq.name == acqname,
                    ArchiveFile.name == filename,
                    ArchiveFileCopy.node == self,
                )
                .get()
            )
        except pw.DoesNotExist:
            return False

        # Check has_file
        return copy.has_file != "N"

    def filecopy_state(self, file: ArchiveFile) -> str:
        """What is the state of `file` on this node?

        Parameters
        ----------
        file : ArchiveFile
            the file to look for

        Returns
        -------
        filecopy_state : str
            One of:
            - 'Y' file copy exists
            - 'X' file copy is corrupt
            - 'M' file copy needs to be checked
            - 'N' file copy does not exist
        """
        from .archive import ArchiveFileCopy

        try:
            copy = ArchiveFileCopy.get(
                ArchiveFileCopy.file == file,
                ArchiveFileCopy.node == self,
            )
            return copy.has_file
        except pw.DoesNotExist:
            pass

        return "N"

    def get_total_gb(self) -> float:
        """Sum the size in GiB of all files on this node.

        Returns
        -------
        total_gib : float
            The total (in GiB) of all apparent file sizes
            from the database.  That is, the sum of
            `ArchiveFileCopy.size_b` for all existing
            file copies.

        Notes
        -----
        The value returned may be quite different than the
        amount of actual space the file copies take up on
        the underlying storage system.
        """
        from .acquisition import ArchiveFile
        from .archive import ArchiveFileCopy

        size = (
            ArchiveFile.select(fn.Sum(ArchiveFile.size_b))
            .join(ArchiveFileCopy)
            .where(ArchiveFileCopy.node == self, ArchiveFileCopy.has_file == "Y")
        ).scalar(as_tuple=True)[0]

        return 0.0 if size is None else float(size) / 2**30

    def get_all_files(
        self,
        present: bool = True,
        corrupt: bool = False,
        unknown: bool = False,
        removed: bool = False,
    ) -> set[pathlib.PurePath]:
        """Return a set of paths to files in the specified state on the node.

        By default, only files present (`has_file=='Y'`) are returned.

        Parameters
        ----------
        present : bool, optional
            If True, file copies with `has_file=='Y'` are included in the list.
        corrupt : bool, optional
            If True, file copies with `has_file=='X'` are included in the list.
        unknown : bool, optional
            If True, file copies with `has_file=='M'` are included in the list.
        removed : bool, optional
            If True, file copies with `has_file=='N'` are included in the list.

        Returns
        -------
        all_files : set of pathlib.PurePath
            A set of paths relative to `root` for all files with the requested criteria.
            If all parameters are set to `False`, or no files match, an empty
            set is returned without raising an error.
        """
        from .acquisition import ArchiveAcq, ArchiveFile
        from .archive import ArchiveFileCopy

        # Which filecopy states are we looking for?
        states = []

        if present:
            states.append("Y")
        if corrupt:
            states.append("X")
        if unknown:
            states.append("M")
        if removed:
            states.append("N")

        # No need for a DB query if the caller has asked for nothing
        if not len(states):
            return set()

        # This is a set and not a list because the primary thing alpenhornd
        # does with the output of this function is test to see if some path is
        # in the returned set.
        return {
            pathlib.PurePath(copy[0], copy[1])
            for copy in (
                ArchiveFileCopy.select(ArchiveAcq.name, ArchiveFile.name)
                .join(ArchiveFile)
                .join(ArchiveAcq)
                .where(ArchiveFileCopy.node == self, ArchiveFileCopy.has_file << states)
                .tuples()
            )
        }

    def update_avail_gb(self, new_avail: int | None) -> None:
        """Update `avail_gb` and record the update time.

        Parameters
        ----------
        new_avail : integer or None
            The amount of available space in bytes
        """
        # The value in the database is in GiB (2**30 bytes)
        if new_avail is None:
            # Warn unless we never knew the free space
            if self.avail_gb is not None:
                log.warning(f'Unable to determine available space for "{self.name}".')
        else:
            self.avail_gb = new_avail / 2**30

        # Record check time, even if we failed to update
        self.avail_gb_last_checked = pw.utcnow()

        # Update the DB with the free space but don't clobber changes made
        # manually to the database.
        self.save(only=[StorageNode.avail_gb, StorageNode.avail_gb_last_checked])


class StorageTransferAction(base_model):
    """Storage transfer rules for the archive.

    This provides configuration for the edges in the storage node/group directed
    graph.

    Attributes
    ----------
    node_from : foreign key
        The source node for the transfer.
    group_to : foreign key
        The destination group for the transfer.
    autosync : boolean
        If True, automatically copy files from `node_from` to `group_to`.
    autoclean : boolean
        If True, automatically delete file copies from `node_from` after they
        end up in `group_to`.

    Notes
    -----
    If `NODE_A` is in `GROUP_A` and `NODE_B` in `GROUP_B`, then
    `StorageTransferAction(node_from=NODE_A, group_to=NODE_B)` and
    `StorageTransferAction(node_from=NODE_B, group_to=NODE_A)` are distinct.
    (i.e. all edges are directed).

    Alpenhorn ignores records where `group_to == node_from.group` (self-loops).

    Autosyncing:
        If `autosync` between a NODE and a GROUP is enabled, then whenever a
        new file copy is added to NODE (i.e. after a successful import or pull),
        then a new `ArchiveFileCopyRequest` will be created to transfer the file
        from NODE to GROUP, so long as the file doesn't already exist in GROUP.

    Autocleaning:
        If `autoclean` between a NODE and a GROUP is enabled, then whenever a
        new file copy is added to GROUP (i.e. after a successful import or pull,
        regardless of the origin of the file), then the file will be scheduled
        for deletion from NODE (by setting `ArchiveFileCopy.wants_file` to "N"
        for the file copy on NONE).
    """

    node_from = pw.ForeignKeyField(StorageNode, backref="transfers_from")
    group_to = pw.ForeignKeyField(StorageGroup, backref="transfers_to")
    autosync = pw.BooleanField(default=False)
    autoclean = pw.BooleanField(default=False)

    @property
    def self_loop(self) -> bool:
        """True if this is a self-loop (i.e. node_from.group == group_to)."""
        return self.node_from.group == self.group_to

    class Meta:
        indexes = (
            (("node_from", "group_to"), True),
        )  # (node_from, group_to) is unique
