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
            backup_count: 10

            # Size, in bytes, at which log file rotation occurs.  May include a suffix:
            # k, M, or G.
            max_bytes: 4M


    # Configure the operation of the local daemon
    daemon:
        # The daemon host name.  This value is matched against the "host" entries
        # of StorageNodes to determine which nodes are local to this daemon.
        host: alpenhost

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

        # Node update-skew threshold.  Normally, the daemon will exit with an
        # error if it detects some other process regularly updating one of the
        # nodes it is managing.  This is designed to catch instances where
        # multiple copies of the daemon have been spawned for some reason.  The
        # error will be triggered if the number of _consecutive_ main loops
        # where such a third-party update is detected equals or exceeds this
        # value.  Setting this to a zero disables the check.
       update_skew_threshold: 4
"""

from __future__ import annotations

import logging
import os
from typing import Any

import yaml
from click import ClickException

log = logging.getLogger(__name__)

# This is where the config is stored
_config = None

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

    global _config, _test_isolation

    # Initialise an empty config
    _config = {}

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
            _config = merge_dict_tree(_config, conf)

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


# This is used to mark no default (since the caller may want to use `None`
# as the default).
_SENTINAL = object()


def get(path: str, default: Any = _SENTINAL, as_type: type | None = None) -> Any:
    """Return the config value specified by `path`.

    Parameters:
    -----------
    path: str
        The dotted path to the config parameter, e.g. "logging.file.watch"
    default: Any, optional
        If given, the value to return if the config parameter is not found.
        If this is not given, a missing config parameter results in an
        exception.
    as_type: type, optional
        If this is specified and the config parameter exists, an attempt
        will be made to co-erce the value to this type.  The only supported
        types are: bool, dict, float, int, list, str.

    Notes
    -----
    If `as_type` is given, co-ercion is only attempted if the parameter exists.
    Co-ercion will never be attempted on the value specified by `default`: if
    it's used, it will be returned verbatim, even if it doesn't conform to the
    type given by `as_type`.

    Raises
    ------
    click.ClickException:
        The parameter didn't exist, and no default was given, or the value
        couldn't be co-erced to the requested type.
    ValueError:
        The value of `as_type` was not one of the allowed types listed above.
    """
    # Split the path
    paths = path.split(".")

    # Drill down to the requested parameter
    value = _config
    for elem in paths:
        if isinstance(value, dict) and elem in value:
            value = value[elem]
        else:
            if default is not _SENTINAL:
                return default
            raise ClickException(f"missing config value: {path}")

    # Now check type, if requested
    if as_type is dict:
        if not isinstance(value, dict):
            raise ClickException(f"invalid config value for {path}: mapping expected")
    elif as_type is list:
        if not isinstance(value, list):
            raise ClickException(f"invalid config value for {path}: sequence expected")
    elif as_type is not None:
        # Scalar co-ercion requested.  Reject non-scalars
        if isinstance(value, dict | list):
            raise ClickException(f"invalid config value for {path}: scalar expected")
        if as_type is bool:
            if value is not True and value is not False:
                raise ClickException(
                    f"invalid config value for {path}: boolean expected"
                )
        elif as_type in {float, int, str}:
            try:
                return as_type(value)
            except (ValueError, TypeError):
                raise ClickException(
                    f"invalid config value for {path}: {as_type} expected"
                )
        else:
            raise ValueError(f"unsupported as_type: {as_type}")

    # No coercion:
    return value


def _get_bounded(
    path: str,
    default: Any,
    as_type: type,
    min: int | float | None,
    max: int | float | None,
) -> int | float:
    """Return a config value with bounds checking.

    This is used to implement `get_int` and `get_float` (q.v.).
    """

    # Fetch
    value = get(path, default=default, as_type=as_type)
    if value is default:
        return default

    if min is not None and value < min:
        raise ClickException(f'config parameter "{path}" too small: {value} < {min}')
    if max is not None and value > max:
        raise ClickException(f'config parameter "{path}" too large: {value} > {max}')
    return value


def get_int(
    path: str, default: Any = _SENTINAL, min: int | None = None, max: int | None = None
) -> int:
    """Return a config value as an int.

    Includes optional bounds checking.

    Parameters:
    -----------
    path: str
        The dotted path to the config parameter, e.g. "logging.file.watch"
    default: Any, optional
        If given, the value to return if the config parameter is not found.
        If this is not given, a missing config parameter results in an
        exception.
    min, max: int, optional
        Bounds (inclusive).  If given, a config value below `min` or above `max`
        results in an exception.

    Notes
    -----
    Bounds checking is not done on the `default` value, if used.

    Raises
    ------
    click.ClickException:
        The parameter didn't exist, and no default was given, or the value
        couldn't be co-erced to an int, or the value was out of bounds
    """
    return _get_bounded(path, default, int, min, max)


def get_float(
    path: str,
    default: Any = _SENTINAL,
    min: float | None = None,
    max: float | None = None,
) -> float:
    """Return a config value as a float.

    Includes optional bounds checking.

    Parameters:
    -----------
    path: str
        The dotted path to the config parameter, e.g. "logging.file.watch"
    default: Any, optional
        If given, the value to return if the config parameter is not found.
        If this is not given, a missing config parameter results in an
        exception.
    min, max: float, optional
        Bounds (inclusive).  If given, a config value below `min` or above `max`
        results in an exception.

    Notes
    -----
    Bounds checking is not done on the `default` value, if used.

    Raises
    ------
    click.ClickException:
        The parameter didn't exist, and no default was given, or the value
        couldn't be co-erced to a float, or the value was out of bounds
    """
    return _get_bounded(path, default, float, min, max)


def get_bytes(path, default: str | None = None) -> int:
    """Return a config value as a byte size.

    Really, this is just an alternate int format which allows a
    "k", "M", "G" suffix for large integers.  Only positive
    values are allowed.

    Parameters
    ----------
    path: str
        The dotted path to the config parameter, e.g. "logging.file.max_bytes"
    default: str, optional
        The default value if the config parameter is not found.

    Notes
    -----
    Unlike most of the `config.get...` routines, this one _does_ interpret
    the default value, meaning defaults like "4M" can be used.  Also, although
    this function returns an int, float values are permitted in the config (so,
    e.g., "1.5k" is permitted)

    Returns
    -------
    bytes
        The config value or default, converted to an int

    Raises
    ------
    click.ClickException
        The requested parameter was not found or not valid, or non-positive.
    """
    # Get the config value
    value = get(path, default=_SENTINAL if default is None else default, as_type=str)

    exponent = 0

    # Look for a suffix
    if value.endswith("k"):
        value = value[:-1]
        exponent = 1
    elif value.endswith("M"):
        value = value[:-1]
        exponent = 2
    elif value.endswith("G"):
        value = value[:-1]
        exponent = 3

    try:
        result = int(float(value) * (1024**exponent))
    except ValueError:
        raise ClickException(f'invalid value for "{path}"')

    if result <= 0:
        raise ClickException(f'invalid value for "{path}"')

    return result
