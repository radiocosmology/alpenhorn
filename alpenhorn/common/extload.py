"""Extension loading and registation.

This module is responsible for loading third-party extensions listed in the
alpenhorn config.  See the `alpenhorn.extensions` module for the Extension API,
including a description of the various types of extensions.
"""

from __future__ import annotations

import importlib
import logging

from click import ClickException

from .. import db
from .. import extensions as extapi
from ..extensions.base import Extension
from ..io.base import InternalIO
from . import config

log = logging.getLogger(__name__)

# All initialised ImportDetectExtensions, if any
_id_ext = None

# All IOClassExtensions, including internal ones
_io_ext = None


def find_extensions() -> list[Extension]:
    """Collect all Extensions specified in the configuration.

    Inspects the `'extensions'` section in the configuration for full resolved
    Python module names, attempts to import all of them, and creates a list of
    extensions provided by these modules.

    Returns
    -------
    list
        The collected extensions.

    Raises
    ------
    click.ClickException:
        An extension module listed in the configuration could not be imported,
        or an extension module did not provide the `register_extensions` function.
    """
    from ..io import internal_io

    # Initialise globals
    global _id_ext, _io_ext

    _id_ext = []
    _io_ext = internal_io.copy()

    ext = {}

    for name in config.get("extensions", default=[], as_type=list):
        log.info(f"Loading extension module: {name}")

        ext_count = 0

        try:
            ext_module = importlib.import_module(name)
        except ImportError as e:
            raise ClickException(
                f"unable to import extension module {name}: {e}"
            ) from e

        try:
            extensions = ext_module.register_extensions()
        except AttributeError as e:
            raise ClickException(
                f'module "{name}" is not a valid Alpenhorn extension module: '
                'no "register_extensions" hook',
            ) from e

        for extension in extensions:
            # Make full name
            ext_name = name + "." + extension.name
            if ext_name in ext:
                log.warning(f'Ignoring duplicate extension "{ext_name}"')
                continue
            ext[ext_name] = extension
            extension.full_name = ext_name
            ext_count += 1

        if not ext_count:
            log.debug("No usable extensions in module")

    # This list is in the order we found them because dicts preserve insertion
    # order (since Python 3.7)
    return list(ext.values())


def init_extensions(extensions: list[Extension], stage: int) -> None:
    """Initialise extensions of a givens stage.

    Parameters
    ----------
    extensions : list
        The list of Extensions returned by `find_extensions`.
    stage : int
        The current initialisation stage of the extensions.

    Raises
    ------
    click.ClickException
        An Extension was not usable.
    """

    for extension in extensions:
        # Skip extensions from other stages
        if stage != extension.stage:
            continue

        # Try init
        if not extension.init_extension():
            # Init failed
            continue

        # What kind of extension is this?
        if isinstance(extension, extapi.DatabaseExtension):
            # Pass this to the database module.  If we already did
            # this, this returns the name of the previous extension
            last_ext = db.set_extension(extension)
            if last_ext:
                raise ClickException(
                    "more than one database extension in config "
                    f"({last_ext} and {extension.full_name})"
                )
        elif isinstance(extension, extapi.ImportDetectExtension):
            _id_ext.append(extension)
        elif isinstance(extension, extapi.IOClassExtension):
            # Check if this I/O Class is already known
            if extension.io_class_name in _io_ext:
                raise ClickException(
                    f'I/O class "{extension.io_class_name}" from '
                    f'Extension "{extension.full_name}" already provided by '
                    + _io_ext[extension.io_class_name].full_name
                    + "."
                )

            _io_ext[extension.io_class_name] = extension
        else:
            log.warning("Ignoring Extension with unknown type: " + extension.full_name)


def import_detection() -> list[extapi.ImportDetectExtension]:
    """Returns the list of registered import-detect extensions.

    Returns
    -------
    list of ImportDetectExtension
        The list of import detection extensions.  May be empty, if no
        import-detect extensions have been loaded.
    """

    global _id_ext

    # Warn about no extensions
    if not len(_id_ext):
        log.error("Attempt to import file with no import-detect extensions.")

    return _id_ext


def io_extension(name: str) -> extapi.IOClassExtension | InternalIO | None:
    """Returns the Extension providing the I/O class named `name`.

    Parameters
    ----------
    name : str
        The I/O class to find the module for.  Usually the value of
        `StorageNode.io_class` or `StorageGroup.io_class`.

    Returns
    -------
    IOClassExtension or InternalIO or None
        The I/O extension providing the implementation of the named I/O class.
        Either an internal extension provided by `alpenhorn.io`, or else
        a third-party IOClassExtension.

        This will be None if no I/O module could be found.
    """

    return _io_ext.get(name, None)
