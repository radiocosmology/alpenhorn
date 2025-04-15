r"""For configuring alpenhorn from the config file.

Configuration file search order:

- `/etc/alpenhorn/alpenhorn.conf`
- `/etc/xdg/alpenhorn/alpenhorn.conf`
- `~/.config/alpenhorn/alpenhorn.conf`
- `ALPENHORN_CONFIG_FILE` environment variable
- the path passed via `-c` or `--conf` on the command line

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

            # Set to True to use TCP, instead of UDP, to send messages to the
            # syslog server.  Ignored if using a Unix domain socket (i.e. `port`
            # is zero).  Default is False.
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


    # Configure the operation of the local daemon
    daemon:
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

        # Prometheus client port.  Setting this to a positive value will
        # cause the daemon to start the prometheus client HTTP server to
        # serve GET requests on that port (but only when not running in
        # --exit-after-update mode).  If not set, or set to a non-positive
        # value, then the prometheus client is not started.
        prom_client_port: 8080
"""

from __future__ import annotations

import logging
import os
from typing import Any

import yaml
from click import ClickException

log = logging.getLogger(__name__)

config = None

_default_config = {
    "logging": {"level": "info"},
    "daemon": {
        "auto_import_interval": 30,
        "auto_verify_min_days": 7,
        "num_workers": 0,
        "prom_client_port": 0,
        "serial_io_timeout": 900,
        "update_interval": 60,
    },
}

_test_isolation = False


def test_isolation(enable: bool = True) -> None:
    """Enable or disable test isolation.

    Test isolation disables the reading of config files installed
    in the standard paths, but still allows specifying a config
    file via command line or environmental variable.

    For this function to have an effect, it must be called before
    the first `load_config` call.  Ideally, put it in an early
    test fixture in your test suite.

    Parameters:
    -----------
    enable : bool
        Whether to enable (the default) or disable test
        isolation.
    """
    global _test_isolation
    _test_isolation = enable


def load_config(cli_conf: str | os.PathLike | None, cli: bool) -> None:
    """Find and load the configuration from a file."""

    global config, _test_isolation

    # Initialise with the default configuration
    config = _default_config.copy()

    # Construct the configuration file path
    if _test_isolation:
        config_files = []
    else:
        config_files = [
            "/etc/alpenhorn/alpenhorn.conf",
            "/etc/xdg/alpenhorn/alpenhorn.conf",
            "~/.config/alpenhorn/alpenhorn.conf",
        ]

    enviro_conf = os.environ.get("ALPENHORN_CONFIG_FILE", None)
    if enviro_conf:
        config_files.append(enviro_conf)

    if cli_conf:
        config_files.append(str(cli_conf))

    no_config = True

    for cfile in config_files:
        # Expand the configuration file path
        absfile = os.path.abspath(os.path.expanduser(os.path.expandvars(cfile)))

        if not os.path.exists(absfile):
            # Warn if a user-supplied config file is missing
            if cfile == cli_conf:
                log.warning(f"Config file {absfile} defined on command line not found.")
            elif cfile == enviro_conf:
                log.warning(
                    f"Config file {absfile} defined by ALPENHORN_CONFIG_FILE not found."
                )
            continue

        no_config = False

        log.info("Loading config file %s", cfile)

        with open(absfile) as fh:
            conf = yaml.safe_load(fh)

        if conf is not None:
            config = merge_dict_tree(config, conf)

    if no_config:
        raise ClickException(
            "No configuration files available.  See --help-config for more details."
        )


def merge_dict_tree(a: Any, b: Any) -> Any:
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
    if type(a) is not type(b):
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
