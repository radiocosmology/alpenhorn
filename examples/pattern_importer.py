"""A pattern-based example import-detect extension.

This is an example of a third-party "import detect" extension.
Such extensions are required to permit alpenhorn to find
new data files which need import, both as part of the auto-import
functionality, but also when performing manual imports
with the alpenhorn client.

This extension re-implements the AcqType and FileType tables
formerly found in alpenhorn proper and then extends them with
two new fields ("glob" and "patterns") which provide configuration
data for matching the acq/file name against one ore more patterns.

The detection function is `detect`.  This function is provided to
alpenhorn via `register_extension`.  The `detect` function loops
through the AcqTypes and FileTypes to try to match the path
supplied by alpenhorn to the patterns listed in the tables.

On a successful match, the import callback function `register_file`
will be called by alpenhorn, which then stores the matched AcqType
in an extended ArchiveAcq table and the matched FileType in an
extended ArchiveFile table.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Tuple

import re
import os
import json
import peewee as pw
from functools import partial

from alpenhorn.db import ArchiveAcq, ArchiveFile, base_model

if TYPE_CHECKING:
    import pathlib

    from alpenhorn.common.extensions import ImportCallback
    from alpenhorn.db.archive import ArchiveFileCopy
    from alpenhorn.server.update import UpdateableNode
del TYPE_CHECKING


class TypeBase(base_model):
    """Base class for AcqType and FileType.

    AcqType and FileType are identical, and vary just in name.

    Attributes
    ----------
    name : string
        The name of this file type.
    patterns : string
        A JSON array literal of regular expressions.  Patterns are tried in order
        listed.
    notes : string or None
        Any notes or comments about this file type.
    """

    name = pw.CharField(max_length=64, unique=True)
    glob = pw.BooleanField(default=False)
    patterns = pw.TextField()
    notes = pw.TextField(null=True)

    def check_match(self, name: str) -> bool:
        """Check if `path` matches a pattern.

        Loops through `self.patterns` and tries each against
        the supplied `path`.

        Parameters:
        -----------
        name
            The name to try to match

        Returns
        -------
        match
            True if a successful match was made.

        Raises
        ------
        ValueError:
            The value of `self.patterns` was invalid.
        """
        # Parse the pattern list, if necessary
        if not hasattr(self, "_pattern_list"):
            try:
                self._pattern_list = json.loads(self.patterns)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid patterns: {self.patterns}") from e

            if not isinstance(self._pattern_list, list):
                del self._pattern_list
                raise ValueError(
                    "Value of patterns must be a JSON array literal."
                    f'(Got "{self.patterns}")'
                )

        # Loop over patterns and check for matches
        for pattern in self._pattern_list:
            if re.match(pattern, name):
                return True

        return False


# These are just differently named copies of TypeBase
class AcqType(TypeBase):
    pass


class FileType(TypeBase):
    pass


class ExtendedAcq(ArchiveAcq):
    """Extend ArchiveAcq record.

    Adds a "type" attribute to hold a refernce to the AcqType.
    """

    type = pw.ForeignKeyField(AcqType, backref="acqs", null=True)

    # Use the same table name
    class Meta:
        table_name = ArchiveAcq._meta.table_name


class ExtendedFile(ArchiveFile):
    """Extend ArchiveFile record.

    Adds a "type" attribute to hold a refernce to the FileType.
    Also re-implements the "acq" field to point it to the
    `ExtendedAcq` record.
    """

    acq = pw.ForeignKeyField(ExtendedAcq, backref="files")
    type = pw.ForeignKeyField(FileType, backref="files", null=True)

    # Use the same table name
    class Meta:
        table_name = ArchiveFile._meta.table_name


def register_file(
    filecopy: ArchiveFileCopy,
    new_file: ArchiveFile | None,
    new_acq: ArchiveAcq | None,
    node: UpdateableNode,
    /,
    *,
    acq_type: AcqType,
    file_type: FileType,
) -> None:
    """Post-import callback.

    This file is used in `detect` as the callback function.  After importing
    a file, alpenhorn will call this function to allow this extension to
    add extra data to the database, or do anything else we want with the newly
    imported file.

    Parameters
    ----------
    filecopy: ArchiveFileCopy
        The ArchiveFileCopy record of the newly-imported file.  This
        record provides access to the ArchiveFile (via `filecopy.file`),
        the StorageNode (via `filecopy.node`), the ArchiveAcq (via
        `filecopy.file.acq`), and the path to the file on disk (via
        `filecopy.path`) of the newly-imported file.
    new_file : ArchiveFile or None
        If this import created a new `ArchiveFile` record, this is
        it (equivalent to `filecopy.file`).  If a new `ArchiveFile` was
        not created, this is None.
    new_acq : ArchiveAcq or None
        If this import created a new `ArchiveAcq` record, this is
        it (equivalent to `filecopy.file.acq`).  If a new `ArchiveAcq` was
        not created, this is None.
    acq_type : AcqType
        The type of the acquisition.
    file_type : FileType
        The type of the file.

    Notes
    -----
    The first three, positional-only parameters are filled in by alpenhorn
    when calling this function as a callback (and are the only parameters
    known to alpenhorn).  The last two, keyword-only parameters are filled-in
    by `detect()` by creating a `functools.partial` of this function which
    is passed to alpenhorn to use as a callback.

    If `new_acq` is not None, then `new_file` is also guaranteed to be not
    None, since the `ArchiveFile` record could not have existed if the
    acqusition it references didn't either.  The converse is _not_ true:
    `new_acq` _can_ be None when `new_file` is not.
    """

    # Store the acqtype in the acq records.  This only needs to happen
    # if the record is new.
    if new_acq is not None:
        acq = ExtendedAcq.get(id=new_acq.id)
        acq.type = acq_type
        acq.save()

    # Ditto for file type
    if new_file is not None:
        file = ExtendedFile.get(id=new_file.id)
        file.type = file_type
        file.save()


def detect(
    path: pathlib.PurePath, node: UpdateableNode
) -> Tuple[pathlib.PurePath | None, ImportCallback | None]:
    """The primary detection routine for this extension.

    Parameters
    ----------
    path
        the path to the file being imported.  Relative to `node.db.root`
    node
        the node on which the import is happening.  Unused.

    Returns
    -------
    acq_name : pathlib.Path or None
        If detection succeeded, the name of the acquisition, a parent of
        `path`.  If detection fails, this is None
    callback : callable or None
        If detection succeeded, this is a `functools.partial`-wrapped
        version of the `register_file()` function.  If detection fails,
        this is also None.
    """
    # Iterate over all known acquisition types to try and find one that
    # can handle the acquisition path.  For a given type, each time
    # matching fails, try successive parent paths until we run out of
    # directory segments
    acq_type = None
    for name in path.parents:
        if str(name) == ".":
            break  # Out of path elements

        for type_ in AcqType.select():
            if type_.check_match(str(name)):
                acq_type = type_
                acq_name = name
                break

        # If the inner loop found a match, stop trying path elements
        if acq_type is not None:
            break

    # If acq type couldn't be found, indicate failure
    if acq_type is None:
        return None, None

    # File name
    file_name = os.path.relpath(path, acq_name)

    # Now figure out the file type
    file_type = None
    for type_ in FileType.select():
        if type_.check_match(file_name):
            file_type = type_

    # If file type couldn't be found, indicate failure
    if file_type is None:
        return None, None

    # Otherwise, success.  Prepare a partial to use as the callback
    callback = partial(register_file, acq_type=acq_type, file_type=file_type)

    # Return success
    return acq_name, callback


def register_extension() -> dict:
    """Extension registration function.

    Called by alpenhorn during start-up to determine this extension's
    capabilities.
    """
    return {"import-detect": detect}
