from __future__ import annotations

import logging
import pathlib

import peewee as pw

from ._base import base_model

log = logging.getLogger(__name__)


class ArchiveAcq(base_model):
    """Describe the acquisition.

    Attributes
    ----------
    name : string
        Name of acquisition.
    comment : string
        User-specified comment.
    """

    name = pw.CharField(max_length=64, unique=True)
    comment = pw.TextField(null=True)


class ArchiveFile(base_model):
    """A file in an acquisition.

    Attributes
    ----------
    acq : foreign key to ArchiveAcq
        The acqusition containing this file.
    name : string
        Name of the file.
    size_b : integer
        Size of file in bytes.
    md5sum : string
        md5 checksum of file. Used for verifying integrity.
    registered : datetime
        The UTC time when the file was registered in the database.
    """

    acq = pw.ForeignKeyField(ArchiveAcq, backref="files")
    name = pw.CharField(max_length=255)
    size_b = pw.BigIntegerField(null=True)
    md5sum = pw.CharField(null=True, max_length=32)
    # Note: default here is the now method itself (i.e. "now", not "now()").
    #       Will be evaulated by peewee at row-creation time.
    registered = pw.DateTimeField(default=pw.utcnow)

    class Meta:
        # (acq,name) is unique
        indexes = ((("acq", "name"), True),)

    @property
    def path(self) -> pathlib.PurePath:
        """The relative path to the file copy.

        Simply the path concatenation of `acq.name` and `name`.
        """
        # This is a PurePath: it can't be concrete until a node.root
        # is prepended.
        return pathlib.PurePath(self.acq.name, self.name)

    @property
    def archive_count(self) -> int:
        """The total number of archived copies of this file"""
        from .archive import ArchiveFileCopy
        from .storage import StorageNode

        return (
            self.copies.join(StorageNode)
            .select()
            .where(
                StorageNode.storage_type == "A",
                ArchiveFileCopy.has_file == "Y",
            )
            .count()
        )
