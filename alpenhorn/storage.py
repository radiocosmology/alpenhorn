"""StorageNode and StorageGroup table models."""
# Type annotation shennanigans
from __future__ import annotations
from typing import TYPE_CHECKING

import peewee as pw
from peewee import fn

from .db import EnumField, base_model
from . import util

if TYPE_CHECKING:
    import pathlib
    from .acquisition import ArchiveFile


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

    If `io_class` includes at least one `.`, then it's assumed to be
    the full import path to a class.

    Otherwise, the I/O module is assumed to be part of the
    `alpenhorn.io` package.  The class itself, then, is the final
    component of io_class with `GroupIO` appended.

    So:
    * if `io_class` is "Nearline", alpenhorn will import the class
        "NearlineGroupIO" from the module `alpenhorn.io.Nearline`.
    * if `io_class` is `mypackage.custom_alpenhorn.MyIOClass`, alpenhorn
        will import the class `MyIOClass` from the module
        `mypackage.custom_alpenhorn`.
    """

    name = pw.CharField(max_length=64, unique=True)
    io_class = pw.CharField(max_length=255, null=True)
    notes = pw.TextField(null=True)
    io_config = pw.TextField(null=True)

    def filecopy_state(self, file: ArchiveFile) -> str:
        """Return the state of a copy of `file` in the group.

        Returns the value of `has_file` for an ArchiveFileCopy for
        ArchiveFile `file` if found in this group, or "N" if no such
        copy exists.

        If multiple file copies exist in the group, then a copy with
        `has_file=="Y"` wins.  Next in priority is "M" then "X".
        "N" is only returned if all copies have `has_file=="N"`.

        Parameters
        ----------
        file : ArchiveFile
            The file to look for

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

        state = "N"
        for copy in (
            ArchiveFileCopy.select(ArchiveFileCopy.has_file)
            .join(StorageNode)
            .where(
                ArchiveFileCopy.node.group == self,
                ArchiveFileCopy.file == file,
            )
        ):
            if copy.has_file == "Y":
                # No need to check more
                return "Y"
            elif copy.has_file == "M":
                state = "M"
            elif copy.has_file == "X" and state == "N":
                state = "X"

        return state


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
        When was the amount of free space last checked?
    notes : string
        Any notes or comments about this node.
    io_config : string
        An optional JSON blob of configuration data interpreted by the
        I/O class.  If given, must be a JSON object literal.

    If `io_class` includes at least one `.`, then it's assumed to be
    the full import path to a class.

    Otherwise, the I/O module is assumed to be part of the
    `alpenhorn.io` package.  The class itself, then, is the final
    component of io_class with `NodeIO` appended.

    So:
    * if `io_class` is "Nearline", alpenhorn will import the class
        "NearlineNodeIO" from the module `alpenhorn.io.Nearline`.
    * if `io_class` is `mypackage.custom_alpenhorn.MyIOClass`, alpenhorn
        will import the class `MyIOClass` from the module
        `mypackage.custom_alpenhorn`.
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

    def named_copy_present(self, acqname: str, filename: str) -> bool:
        """Is an ArchiveFileCopy named `acqname/filename` present?

        Both the file and the acquisition must exist in the database.

        Parameters
        ----------
        acqname : str
            The name of the ArchiveAcq
        filename : str
            The name of the ArchiveFile

        Returns
        -------
        named_copy_present : bool
            True if there is an ArchiveFileCopy with `has_file=='Y'`
            for the specified path.  False otherwise.
        """
        from .acquisition import ArchiveFile, ArchiveAcq

        # Try to find the ArchiveFile record
        try:
            file = (
                ArchiveFile.select()
                .join(ArchiveAcq)
                .where(
                    ArchiveAcq.name == acqname,
                    ArchiveFile.name == filename,
                )
                .get()
            )
        except pw.DoesNotExist:
            return False

        # And then pass it to filecopy_present
        return self.filecopy_present(file)

    def filecopy_present(self, file: ArchiveFile) -> bool:
        """Is a copy of ArchiveFile `file` present on this node?

        Parameters
        ----------
        file : ArchiveFile
            the file to look for

        Returns
        -------
        filecopy_present : bool
            True if there is an ArchiveFileCopy of `file`r
            with `has_file=='Y'` on this node.  False otherwise.
        """
        from .archive import ArchiveFileCopy

        try:
            ArchiveFileCopy.get(
                ArchiveFileCopy.file == file,
                ArchiveFileCopy.node == self,
                ArchiveFileCopy.has_file == "Y",
            )
        except pw.DoesNotExist:
            return False

        return True

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

        return 0.0 if size is None else float(size) / 2**20

    def get_all_files(self) -> set[pathlib.PurePath]:
        """Return a set of paths to all files existing on the node.

        Returns
        -------
        all_files : set of pathlib.PurePath
            a set of paths relative to `root` for all file copies
            exsiting on the node.
        """
        from .archive import ArchiveFileCopy

        return {
            copy.file.path
            for copy in (
                ArchiveFileCopy.select().where(
                    ArchiveFileCopy.node == self, ArchiveFileCopy.has_file == "Y"
                )
            )
        }
