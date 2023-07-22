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

    # Logging configuration.  By default, alpenhorn sends all log message to
    # standard error.
    logging:
        # Set the overall logging level
        level: debug

        # Allow overriding the level on a module by module basis
        module_levels:
            alpenhorn.db: info

        # Alpenhorn can be configured to send log output to syslog and/or
        # a file.  This is _in addition_ to the log sent to standard error,
        # which is always enabled.

        # Syslog logging.
        syslog:
            # If true, enable logging to syslog.  Note: if the "syslog" section
            # is present in the logging config, then the default value
            # of this key is true.  As a result, this key need only be specified if
            # none of the other syslog configuration parameters are provided
            # in the config.
            enable: true

            # The network address to send syslog message to.
            #
            # May also be a Unix domain socket (like "/dev/log").  In that case,
            # set `port`, below, to zero to indicate this.
            #
            # Defaults to "localhost".
            address: localhost

            # The network port to send syslog messages to.  If this is zero,
            # then the port is ignored and `address` is taken to be a Unix domain
            # socket.  Defaults to 514, the standard syslog port.
            port: 514

            # The syslog facility to use.  If given, should be the name of one of
            # the syslog facilities, disregarding case ("user", "local0", ...)
            # Defaults to "user".
            facility: user

            # Set to True to use TCP, instead of UDP, to send messages to the syslog server.
            # Ignored if using a Unix domain socket (i.e. `port` is zero).  Default is False.
            use_tcp: false

        # File logging.
        file:
            name: /path/to/file.log

            # If a third-party (like logrotate) is rotating the alpenhorn log
            # file, set this to "true" to tell alpenhorn to watch for log-file
            # rotation.
            watch: false

            # Alternately, alpenhorn can manage log file rotation itself.  Set
            # "rotate" to true to enable.  At most one of "watch" and "rotate"
            # may be true.
            rotate: true

            # The following two settings affect alpenhorn-managed log rotation
            # and are ignored if "rotate" is not true.

            # Maximum number of rotated files to keep.  Must be at least one
            # Rotated files append an integer to the name specified (e.g.
            # "file.log.1", "file.log.2" etc.).
            backup_count: 100

            # Size, in bytes, at which log file rotation occurs.  May include a suffix:
            # k, M, or G.
            max_bytes: 4G


    # Configure the operation of the local service
    service:
        # Default number of worker threads
        num_workers: 4

        # Minimum time length (in seconds) between updates
        update_interval: 60

        # Timescale on which to poll the filesystem for new data to import
        auto_import_interval: 30

        # Minimum number of days to wait from the last update of a file copy
        # record before auto-verifying the file
        auto_verify_min_days: 7

        # Maximum time (in seconds) to run serial I/O per update loop (these
        # are I/O run tasks in the main thread, in cases when there are no
        # worker threads
        serial_io_timeout: 900

        # These two optional parameters control how long a pull job is
        # allowed to run before being forceably killed.  The timeout (in
        # seconds) for a pull of a file of size "size_b" bytes is:
        #
        #   pull_timeout_base + size_b / pull_bytes_per_second
        #
        # If pull_bytes_per_second is zero, the timeout is disabled (the
        # job will rum forever if it doesn't exit; not recommended).
        pull_timeout_base: 300
        pull_bytes_per_second: 20000000
"""

import logging
import os

import yaml

log = logging.getLogger(__name__)

config = None

_default_config = {
    "logging": {"level": "warning", "module_levels": {"alpenhorn": "info"}},
    "service": {
        "auto_import_interval": 30,
        "auto_verify_min_days": 7,
        "num_workers": 0,
        "serial_io_timeout": 900,
        "update_interval": 60,
    },
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

        if conf is not None:
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
