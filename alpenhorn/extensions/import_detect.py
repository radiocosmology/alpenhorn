"""The Import Detect Extension"""

import pathlib
from collections.abc import Callable

from ..daemon import UpdateableNode
from ..db import ArchiveAcq, ArchiveFile, ArchiveFileCopy
from .base import Extension

# This is the type alias for the callback
ImportCallback = Callable[
    [ArchiveFileCopy, ArchiveFile | None, ArchiveAcq | None, UpdateableNode], None
]

# This is the type alias for the detect routine
ImportDetect = Callable[
    [pathlib.Path, UpdateableNode],
    tuple[pathlib.Path | str | None, ImportCallback | None],
]


class ImportDetectExtension(Extension):
    """An Import Detect Extension.

    Import detect extensions provide functionality to alpenhorn to allow it
    to identify new files needing registration in the Data Index.  Without
    one or more Import Detect Extensions, alpenhorn has no knowledge of
    which files need to be registered.

    An Import Detect Function must provide a `detect` function.  This is
    a callable object providing a detection routine which will be called
    when importing new files to determine if the file being considered is
    a valid data file.  It will be passed a two positional parameters:

      * `path`: a `pathlib.PurePath` giving the path relative to the node
        root to the file being imported.
      * `node`: a `UpdateableNode` instance of the node on which we're
        importing the file.

    The function should return a two-tuple.  If the detection fails, this
    should be a pair of `None`s.  Otherwise, if detection succeeds:

      * `acq_name`: The name of the acquisition, which does not already need
        to exist.  This should be a string or `pathlib.Path` and be one of
        the parents of the passed-in path.
      * `callback`: Either a callable object, which can be used by the
        extension to perform post-import actions, or else `None`, if no
        callback is needed.

    If the function returns a callable object, that object will be called after
    creating the archive record(s) and passed three positional arguments:

      * `filecopy`: the `ArchiveFileCopy` record for the newly imported file
      * `new_file`: If this import created a new `ArchiveFile` record, this is
        it (equivalent to `filecopy.file`).  If a new `ArchiveFile` was not created,
        this is None.
      * `new_acq`: If this import created a new `ArchiveAcq` record, this is
        it (equivalent to `filecopy.file.acq`).  If a new `ArchiveAcq` was
        not created, this is None.

    The value returned from the call is ignored.

    If multiple `import-detect` extensions are provided, they will be called in the
    order given in the config file until one of them indicates a successful match.

    Attributes
    ----------
    name : str
        The name of this Extension.  The name of an Extension should
        be unique within the extension module defining it, but does
        not need to be globally unique.
    version : str
        The version of the Extension.  The verison must be parsable by
        `packaging.version`.
    detect : Callable
        The detection routine.  See above.
    min_version : str, optional
        If given, the minimum Alpenhorn version supported by
        this Extension.  Note: it may make more sense for an extension
        module to check `alpenhorn.__version__` directly instead of using
        this parameter.
    max_version : str, optional
        If given, the maximum Alpenhorn version supported by
        this Extension.  Note: it may make more sense for an extension
        module to check `alpenhorn.__version__` directly instead of using
        this parameter.
    require_schema : dict, optional
        If given, a dict of Data Index schema components and the required
        component schema version needed by this extension.  See
        `alpenhorn.db.schema_version` for the specification of the requirements.
    """

    # Initialised after everything else
    stage = 3

    def __init__(
        self,
        name: str,
        version: str,
        detect: ImportDetect,
        min_version: str | None = None,
        max_version: str | None = None,
        require_schema: dict[str, int | str] | None = None,
    ) -> None:
        super().__init__(
            name,
            version,
            min_version=min_version,
            max_version=max_version,
            require_schema=require_schema,
        )
        if not callable(detect):
            raise ValueError("detect must be callable.")
        self.detect = detect
