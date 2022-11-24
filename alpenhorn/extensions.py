"""Extension loading and registation.

Extensions are simply python packages or modules providing extra functionality
to alpenhorn. They should be specified in the `'extension'` section of the
alpenhorn configuration as fully qualified python name. They must have a
`'register_extension'` function that returns a `dict` specifying the extra
functionality they provide. There are currently three supported keys:

`database`
    A dict providing capabilities of a database extension.  See the db module
    for details.  At most one database extension is permitted.
`acq_types`
    The acquisition type extensions, given as a list of `AcqInfoBase` subclasses.
`file_types`
    The file type extensions, given as a list of `FileInfoBase` subclasses.

If multiple extensions provide acquisition or file types with the same name (as
seen by the `._acq_type` or `._file_type` properties), only the last one is
used. Similarly, only the last `database` specification matters.
"""


import importlib
import logging

from . import acquisition, config

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


def register_type_extensions():
    """Register any types found in extension modules.

    Later entries will override earlier ones. This *must* be called after the
    database has been connected.
    """

    for ext_dict in _ext:

        if "acq_types" in ext_dict:
            _register_acq_extensions(ext_dict["acq_types"])

        if "file_types" in ext_dict:
            _register_file_extensions(ext_dict["file_types"])


def _register_acq_extensions(acq_types):

    for acq_type in acq_types:
        name = acq_type._acq_type
        log.info("Registering new acquisition type %s", name)
        acquisition.AcqType.register_type(acq_type)

        try:
            conf = config.config["acq_types"][name]
        except KeyError:
            conf = {}

        acq_type.set_config(conf)


def _register_file_extensions(file_types):

    for file_type in file_types:
        name = file_type._file_type
        log.info("Registering new file type %s", name)
        acquisition.FileType.register_type(file_type)

        try:
            conf = config.config["file_types"][name]
        except KeyError:
            conf = {}

        file_type.set_config(conf)
