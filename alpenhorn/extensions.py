"""Extension loading and registation.

Extensions are simply python packages or modules providing extra functionality
to alpenhorn. They should be specified in the `'extension'` section of the
alpenhorn configuration as fully qualified python name. They must have a
`'register_extension'` function that returns a `dict` specifying the extra
functionality they provide. There are currently two supported keys:

`database`
    A dict providing capabilities of a database extension.  See the db module
    for details.  At most one database extension is permitted.
`import-detect`
    A callable object providing a detection routine which will be called
    when importing new files to determine if the file being considered is
    a valid data file.  It will be passed a two positional parameters:

      * `path`: a `pathlib.PurePath` giving the path relative to the node
        root to the file being imported.
      * `node`: a `UpdateableNode` instance of the node on which we're
        importing the file.

    The funciton should return a two-tuple.  If the detection fails, this
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

If other keys are present in the dictionary returned by `register_extension`, they
are ignored.
"""
from __future__ import annotations
from typing import TYPE_CHECKING, Tuple

import logging
import importlib

from . import config

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable

    from .alpenhorn import ArchiveAcq, ArchiveFile
    from .archive import ArchiveFileCopy
    from .update import UpdateableNode

    ImportCallback = Callable[
        [ArchiveFileCopy, ArchiveFile | None, ArchiveAcq | None, UpdateableNode], None
    ]
    ImportDetect = Callable[
        [pathlib.Path, UpdateableNode],
        Tuple[pathlib.Path | str | None, ImportCallback | None],
    ]

log = logging.getLogger(__name__)

# Internal variables for holding the extension references
_db_ext = None
_id_ext = None


def load_extensions() -> None:
    """Load any extension modules specified in the configuration.

    Inspects the `'extensions'` section in the configuration for full resolved
    Python module names, and then registers any extension types and database
    connections.

    Raises
    ------
    KeyError
        Missing required key in model extension
    ModuleNotFoundError
        A extension module could not be found
    RuntimeError
        An extension module was missing the register_extension function.
    TypeError
        `register_extension` provided data of the wrong type.
    ValueError
        More than one database extension was specified in the alpenhorn
        config or the object provided by an import-detect extension was
        not callable.
    """

    # Initialise globals
    global _db_ext, _id_ext

    _id_ext = list()

    if "extensions" not in config.config:
        log.debug("No extensions to load.")
        return

    for name in config.config["extensions"]:
        log.info(f"Loading extension {name}")

        try:
            ext_module = importlib.import_module(name)
        except ModuleNotFoundError:
            raise ModuleNotFoundError(f"extension module {name} not found")

        try:
            extension_dict = ext_module.register_extension()
        except AttributeError:
            raise RuntimeError(
                f"extension {name} is not a valid alpenhorn extension "
                "(no register_extension hook).",
            )

        extension_dict["name"] = name
        extension_dict["module"] = ext_module

        # Does this extension provide something useful?
        useful_extension = False

        if "database" in extension_dict:
            useful_extension = True

            # Check for database capability dict
            if not isinstance(extension_dict["database"], dict):
                raise TypeError(
                    '"database" key returned by extension module '
                    f"{name} must provide a dict."
                )

            # Don't allow more than one database extension
            if _db_ext is None:
                _db_ext = extension_dict
            else:
                raise ValueError(
                    "more than one database extension in config "
                    f"({_db_ext['name']} and {name})"
                )

        if "import-detect" in extension_dict:
            useful_extension = True

            if not callable(extension_dict["import-detect"]):
                raise ValueError(
                    f"Import detect routine from extension {name} not callable"
                )

            _id_ext.append(extension_dict["import-detect"])

        if not useful_extension:
            log.warning(f"Ignoring extension {name} with no useable functionality!")


def database_extension() -> dict:
    """Find and return a database extension capability dict.

    Returns
    -------
    capabilities : dict or None
        A dict providing the capabilites of the database module, or None,
        if there was no database extension specified in the config.
    """
    if _db_ext is None:
        return None

    log.info(f"Using database extension {_db_ext['name']}")
    return _db_ext["database"]


def import_detection() -> list[ImportDetect]:
    """Returns the list of registered import detect callables.

    Returns
    -------
    import_detectors
        The list of import detection functions.  May be empty, if no
        import detect extensions have been loaded.
    """

    global _id_ext

    # Warn about no extensions
    if len(_id_ext) == 0:
        log.warning("Attempt to import file with no import detect extensions.")

    return _id_ext
