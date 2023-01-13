r"""For configuring alpenhorn from the config file.

Configuration file search order:

- `/etc/alpenhorn/alpenhorn.conf`
- `/etc/xdg/alpenhorn/alpenhorn.conf`
- `~/.config/alpenhorn/alpenhorn.conf`
- `ALPENHORN_CONFIG_FILE` environment variable

This is in order of increasing precedence, with options in later files
overriding those in earlier entries. Configuration is merged recursively by
`merge_dict_tree`.

Example config:

.. codeblock:: yaml

    # Base configuration
    base:
        hostname: alpenhost

    # Configure the database connection with a peewee db_url.  If using a database
    # extension, that may require different data in this section.
    database:
        url: peewee_url

    # Specify extensions as a list of fully qualified references to python packages or
    # modules
    extensions:
        - alpenhorn.generic
        - alpenhorn_chime
        - chimedb.core.alpenhorn

    # Logging configuration
    logging:

        # Set the overall logging level
        level: debug

        # Allow overriding the level on a module by module basis
        module_levels:
            alpenhorn.db: info

    # Table model configuration
    model:
        # acq_info_errors and file_info_errors indicate what should happen
        # when an Info class module can't be imported.  It should be a
        # mapping between acq_type or file_type names and actions, which should
        # be one of:
        #
        # - abort:  abort importing and raise ImportError.  This is the default
        #             action.
        # - ignore: ignore the failed import, as if the type had not
        #             specified any Info class.  In this case, acquisitions
        #             or files will be imported without adding the associated
        #             Info record
        # - skip:   completely ignore the type whose Info import failed.  In
        #             this case, alpenhorn behaves as if the type does not exist
        #             at all.  Importing acquisitions or files of this type will
        #             fail.
        #
        # In the mapping, the default action can be specified using the key "_".
        # If the only key in the mapping is the default "_", key, the whole value
        # may be replaced by the action itself.  So, the config:
        #
        #    acq_info_errors: ignore
        #
        # is equivalent to:
        #
        #    acq_info_errors:
        #         _: ignore
        #
        acq_info_errors: ignore
        file_info_errors:
            some_type: ignore
            other_type: skip
            _: abort

    # Configure the operation of the local service
    service:
        # Minimum time length (in seconds) between updates
        update_interval: 60

        # Timescale on which to poll the filesystem for new data to import
        auto_import_interval: 30
"""

import logging
import os

import yaml

log = logging.getLogger(__name__)

config = None

_default_config = {
    "logging": {"level": "warning", "module_levels": {"alpenhorn": "info"}},
    "model": {"acq_info_errors": "abort", "file_info_errors": "abort"},
    "service": {"update_interval": 60, "auto_import_interval": 30},
}


def load_config():
    """Find and load the configuration from a file."""

    global config

    # Initialise with the default configuration
    config = _default_config.copy()

    # Construct the configuration file path
    config_files = [
        "/etc/alpenhorn/alpenhorn.conf",
        "/etc/xdg/alpenhorn/alpenhorn.conf",
        "~/.config/alpenhorn/alpenhorn.conf",
    ]

    if "ALPENHORN_CONFIG_FILE" in os.environ:
        config_files.append(os.environ["ALPENHORN_CONFIG_FILE"])

    any_exist = False

    for cfile in config_files:

        # Expand the configuration file path
        absfile = os.path.abspath(os.path.expanduser(os.path.expandvars(cfile)))

        if not os.path.exists(absfile):
            continue

        any_exist = True

        log.info("Loading config file %s", cfile)

        with open(absfile, "r") as fh:
            conf = yaml.safe_load(fh)

        config = merge_dict_tree(config, conf)

    if not any_exist:
        raise RuntimeError("No configuration files available.")


class ConfigClass(object):
    """A base for classes that can be configured from a dictionary.

    Note that this configures the class itself, not instances of the class.
    """

    @classmethod
    def set_config(cls, configdict):
        """Configure the class from the supplied `configdict`."""


def merge_dict_tree(a, b):
    """Merge two dictionaries recursively.

    The following rules applied:

      - Dictionaries at each level are merged, with `b` updating `a`.
      - Lists at the same level are combined, with that in `b` appended to `a`.
      - For all other cases, scalars, mixed types etc, `b` replaces `a`.

    Parameters
    ----------
    a, b : dict
        Two dictionaries to merge recursively. Where there are conflicts `b`
        takes preference over `a`.

    Returns
    -------
    c : dict
        Merged dictionary.
    """

    # Different types should return b
    if type(a) != type(b):
        return b

    # From this point on both have the same type, so we only need to check
    # either a or b.
    if isinstance(a, list):
        return a + b

    # Dict's should be merged recursively
    if isinstance(a, dict):
        keys_a = set(a.keys())
        keys_b = set(b.keys())

        c = {}

        # Add the keys only in a...
        for k in keys_a - keys_b:
            c[k] = a[k]

        # ... now the ones only in b
        for k in keys_b - keys_a:
            c[k] = b[k]

        # Recursively merge any common keys
        for k in keys_a & keys_b:
            c[k] = merge_dict_tree(a[k], b[k])

        return c

    # All other cases (scalars etc) we should favour b
    return b


def info_import_errors(type_, is_acq):
    global config

    try:
        # Get the correct dict
        d = (
            config["model"]["acq_info_errors"]
            if is_acq
            else config["model"]["file_info_errors"]
        )

        # String? just return it
        if isinstance(d, str):
            return d

        # Return the action for this type, or the default action, or the default default.
        return d.get(type_, d.get("_", "abort"))
    except (KeyError, TypeError):
        return "abort"
