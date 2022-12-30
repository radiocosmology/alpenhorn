"""Extension loading and registation.

Extensions are simply python packages or modules providing extra functionality
to alpenhorn. They should be specified in the `'extension'` section of the
alpenhorn configuration as fully qualified python name. They must have a
`'register_extension'` function that returns a `dict` specifying the extra
functionality they provide. There are currently one supported key:

`database`
    A dict providing capabilities of a database extension.  See the db module
    for details.  At most one database extension is permitted.

If multiple extensions provide acquisition or file types with the same name (as
seen by the `._acq_type` or `._file_type` properties), only the last one is
used. Similarly, only the last `database` specification matters.
"""


import importlib
import logging

from . import config

log = logging.getLogger(__name__)


# Internal variables for holding the extension references
_ext = None
_db_ext = None


def load_extensions():
    """Load any extension modules specified in the configuration.

    Inspects the `'extensions'` section in the configuration for full resolved
    Python module names, and then registers any extension types and database
    connections.
    """

    global _ext

    _ext = []

    if "extensions" not in config.config:
        log.info("No extensions to load.")
        return

    extension_list = list(config.config["extensions"])

    for name in extension_list:

        log.info("Loading extension %s", name)

        try:
            ext_module = importlib.import_module(name)
        except ImportError:
            raise ImportError(f"extension module {name} not found")

        try:
            extension_dict = ext_module.register_extension()
        except AttributeError:
            raise RuntimeError(
                f"extension {name} is not a valid alpenhorn extension "
                "(no register_extension hook).",
            )

        extension_dict["name"] = name
        extension_dict["module"] = ext_module

        # Don't allow more than one database extension
        if "database" in extension_dict:
            global _db_ext
            if _db_ext is None:
                _db_ext = extension_dict
            else:
                raise RuntimeError(
                    "more than one database extension in config "
                    f"({_db_ext['name']} and {name})"
                )

        _ext.append(extension_dict)


def database_extension():
    """Find and return a database extension capability dict.

    Returns
    -------
    db : `dict`
        A dict providing the capabilites of the database module, or None,
        if there was no database extension specified in the config.
    """
    if _db_ext is None:
        return None

    log.info(f"Using database extension {_db_ext['name']}")
    return _db_ext["database"]
