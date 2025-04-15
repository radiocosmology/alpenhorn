"""Extension loading and registation.

Extensions are simply python packages or modules providing extra functionality
to alpenhorn. They should be specified in the `'extension'` section of the
alpenhorn configuration as fully qualified python name. They must have a
`'register_extension'` function that returns a `dict` specifying the extra
functionality they provide. There are currently three supported keys:

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
`io-modules`
    A dict providing I/O modules to augment the default modules provided in
    `alpenhorn.io`.  Each I/O module should have a node I/O or group I/O class
    (or both).  I/O class names and dict keys must adhere to the following naming
    conventions:
      * For a StorageNode with `io_class` equal to "IOClassName", the node I/O class
        implementing this I/O class must be called `IOClassNameNodeIO`.
      * Similarly, a StorageGroup with `io_class == "IOClassName" must be named
        `IOClassNameGroupIO`.
      * In the `io-modules` dict, the key whose value is a module containing either
        (or both) of the above classes must be "ioclassname" (i.e. equivalent to the
        `io_class` of the group and/or node after conversion to lower case).  So,
        the above example classes would both be in a module associated with the dict
        key `ioclassname`.

    Multiple `io-modules` extensions may be provided; no two extensions may provide
    the same dict keys, nor may any extension provide a key which is the name of an
    existing `alpenhorn.io` submodule.

If other keys are present in the dictionary returned by `register_extension`, they
are ignored.
"""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING

from . import config

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable
    from types import ModuleType

    from .alpenhorn import ArchiveAcq, ArchiveFile
    from .archive import ArchiveFileCopy
    from .update import UpdateableNode

    ImportCallback = Callable[
        [ArchiveFileCopy, ArchiveFile | None, ArchiveAcq | None, UpdateableNode], None
    ]
    ImportDetect = Callable[
        [pathlib.Path, UpdateableNode],
        tuple[pathlib.Path | str | None, ImportCallback | None],
    ]
del TYPE_CHECKING

log = logging.getLogger(__name__)

# Internal variables for holding the extension references
_db_ext = None
_id_ext = None
_io_ext = {}


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
        The data returned by register_extension was not usable.
    """

    # Initialise globals
    global _db_ext, _id_ext

    _id_ext = []

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

        if "io-modules" in extension_dict:
            useful_extension = True

            for modname, extmod in extension_dict["io-modules"].items():
                # Check if this module is present in a previously imported
                # extension.  This test is easier to do then the alpenhorn.io
                # check, so we do it first
                if modname in _io_ext:
                    raise ValueError(
                        f'I/O module "{modname}" in extension {name} already '
                        "imported from an earlier extension module."
                    )

                # Second, check if this module already exists in alpenhorn.io
                try:
                    importlib.import_module("alpenhorn.io." + modname)
                    raise ValueError(
                        f'I/O module "{modname}" in extension {name} duplicates '
                        f'existing module "alpenhorn.io.{modname}"'
                    )
                except ImportError:
                    pass

                # Otherwise, add it to the list
                _io_ext[modname] = extmod

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
    """Returns the list of registered import-detect callables.

    Returns
    -------
    import_detectors
        The list of import detection functions.  May be empty, if no
        import-detect extensions have been loaded.
    """

    global _id_ext

    # Warn about no extensions
    if not len(_id_ext):
        log.error("Attempt to import file with no import-detect extensions.")

    return _id_ext


def io_module(name: str) -> ModuleType | None:
    """Returns the module supporting I/O class named `name`.

    Parameters
    ----------
    name : str
        The I/O class to find the module for.  Usually the value of
        `StorageNode.io_class` or `StorageGroup.io_class`.  This may
        not be "base", ignoring case.

    Returns
    -------
    iomod : module or None
        The Python module providing the implementation of the named I/O class.
        It is either a submodule of `alpenhorn.io` or else a module
        provided by one of the io-module extensions.

        This will be None if no I/O module could be found.
    """

    # Module names are always lower case
    name = name.lower()

    # Loading the I/O base is not allowed
    if name == "base":
        return None

    # Try to load the module from alpenhorn.io
    try:
        return importlib.import_module("alpenhorn.io." + name)
    except ImportError:
        # No alpenhorn.io module, maybe it's an extension module
        return _io_ext.get(name)
