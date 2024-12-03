"""Set up logging for alpenhorn.

Basic Configruation
-------------------

Both the CLI and daemon should call the `init_logging()` function as soon as
possible after program start to turn on logging to standard error.  Any log
messages produced before this call are discarded.


Daemon Logging
--------------

The daemon should immediately follow the loading of the alpenhorn config
with a call to `configure_logging()` which will re-configure the alpenhorn
logger based on the alpenhorn configuration, including starting file or syslog-
based logging, if requested.  The CLI should not call this function.

The alpenhorn daemon buffers log messages emitted between the `init_logging`
and `configure_logging` calls and will flush them to any additonal log
destinations started by `configure_logging` so that these messages are not lost.
(This is in addiiton to the messages being sent immediately to standard error,
which always happens.)

Note also that between the two calls, the log level of the root logger is
set to DEBUG.


CLI Logging
-----------

The CLI does not support file or syslog logging, so should _not_ call
`configure_logging`.  Instead, the CLI supports five verbosity levels:

    1.  No output on standard out.  Error messages on standard error.
    2.  No output on standard out.  Warning and error on standard error.
    3.  CLI output on standard out.  Warning and error messages on standard error.
    4.  CLI output on standard out.  Info, warning, errors on standard error.
    5.  CLI output on standard out.  Debug, info, warning, errors on standard error.

The initial verbosity can be specified in the `init_logging` call.  The
default verbosity is 3.   May be changed at runtime by calling `set_verbosity`.
"""

import logging
import logging.handlers
import pathlib
import socket

import click

from . import config

try:
    from concurrent_log_handler import (
        ConcurrentRotatingFileHandler as RotatingFileHandler,
    )
except ImportError:
    RotatingFileHandler = logging.handlers.RotatingFileHandler

# The log formats.  Used by the stderr log and any other log destinations
cli_fmt = logging.Formatter(
    "%(levelname)s >> %(message)s",
    "%b %d %H:%M:%S",
)
daemon_fmt = logging.Formatter(
    "%(asctime)s %(levelname)s >> [%(threadName)s] %(message)s",
    "%b %d %H:%M:%S",
)

# initialised by init_logging; daemon-only
log_buffer = None

# CLI output suppression.
_cli_echo = True


class StartupHandler(logging.handlers.BufferingHandler):
    """Start-up logging handler for alpenhorn.

    A logging hander similar to logging.handlers.MemoryHandler, except:
    * it can flush to potentially multiple target handlers
    * it never automatically flushes.
    * once the buffer is full, further messages are silently discarded

    Parameters
    ----------
    capacity
        The maximum number of log messages to buffer.
    """

    def __init__(self, capacity: int) -> None:
        super().__init__(capacity)
        self.targets = []

    def addTarget(self, handler: logging.Handler) -> None:
        """Add `handler` to the list of targets."""
        self.targets.append(handler)

    def shouldFlush(self, record) -> bool:
        """Returns false to disable autoflushing."""
        return False

    def emit(self, record) -> None:
        """Buffer `record` if not full."""
        self.acquire()
        try:
            if len(self.buffer) < self.capacity:
                self.buffer.append(record)
        finally:
            self.release()

    def flush(self) -> None:
        """Flush to all targets.

        After flushing, the buffer is cleared.
        """
        self.acquire()
        try:
            for target in self.targets:
                for record in self.buffer:
                    target.handle(record)
            self.buffer.clear()
        finally:
            self.release()

    def close(self) -> None:
        """Discard all targets and drop the buffer."""
        self.acquire()
        try:
            self.targets = []
            super().close()
        finally:
            self.release()


def echo(*args, **kwargs) -> None:
    """CLI wrapper for click.echo.

    Suppresses output when verbosity is less than three.
    """
    if _cli_echo:
        click.echo(*args, **kwargs)


def set_verbosity(verbosity: int) -> None:
    """Set cli verbosity.

    Sets the log level of the root logger based on the
    requested verbosity level.
    """

    # Levels 2 and 3 are the same.
    verbosity_to_level = {
        1: logging.ERROR,
        2: logging.WARNING,
        3: logging.WARNING,
        4: logging.INFO,
        5: logging.DEBUG,
    }

    if verbosity not in verbosity_to_level:
        raise ValueError(f"Bad verbosity: {verbosity}")

    root_logger = logging.getLogger()
    root_logger.setLevel(verbosity_to_level[verbosity])

    # Suppress normal cli output at low verbosity
    global _cli_echo
    _cli_echo = verbosity >= 3


def init_logging(cli: bool, verbosity: int | None = None) -> None:
    """Initialise the logger.

    This function is called before the config is read.  It sets up logging to
    standard error and also starts a log buffer where messages accumulate
    before the logging facilities defined by the configuration are started.

    Parameters
    ----------
    cli : bool
        Is the alpenhorn CLI being initialised?
    verbosity : int
        For the CLI, the verbosity level to use.  Ignored for daemons.
    """

    # This is the stderr logger.  It is always present, regardless of logging config
    log_stream = logging.StreamHandler()
    log_stream.setFormatter(cli_fmt if cli else daemon_fmt)

    # Set up initial logging
    root_logger = logging.getLogger()
    root_logger.addHandler(log_stream)

    if cli:
        if verbosity is None:
            verbosity = 3
        set_verbosity(verbosity)
    else:
        root_logger.setLevel(logging.DEBUG)

        # This is the start-up logger for the daemon.  It buffers messages in memory
        # until configure_logging() is called, at which point the buffered messages
        # are flushed to a file, if one was opened, so that messages logged before
        # the start of file logging are recorded, and then this handler is shut down.
        global log_buffer
        log_buffer = StartupHandler(10000)

        root_logger.addHandler(log_buffer)

        # Record daemon start
        root_logger.info("Alpenhorn start.")


def _max_bytes_from_config(max_bytes: str | float | int) -> int:
    """Convert logging.file.max_bytes to bytes.

    Parameters
    ----------
    max_bytes
        The value of logging.file.max_bytes.

    Returns
    -------
    max_bytes
        The max size converted to bytes

    Raises
    ------
    ValueError
        max_bytes was invalid
    """

    exponent = 0

    # Look for a suffix
    if isinstance(max_bytes, str):
        if max_bytes.endswith("k"):
            max_bytes = max_bytes[:-1]
            exponent = 1
        elif max_bytes.endswith("M"):
            max_bytes = max_bytes[:-1]
            exponent = 2
        elif max_bytes.endswith("G"):
            max_bytes = max_bytes[:-1]
            exponent = 3

    try:
        result = int(float(max_bytes) * (1024**exponent))
    except ValueError:
        raise ValueError("bad size for logging.file.max_bytes")

    if result <= 0:
        raise ValueError("bad size for logging.file.max_bytes")

    return result


def configure_sys_logging(syslog_config: dict) -> logging.handlers.SysLogHandler | None:
    """Configure a syslog logging handler based on the config.

    Parameters
    ----------
    syslog_config
        The contents of the `logging.syslog` section of the
        alpenhorn config.

    Returns
    -------
    syslog_handler
        The configured syslog handler
    """

    enable = syslog_config.get("enable", True)
    if enable is not True and enable is not False:
        raise ValueError("logging.syslog.enable in config must be boolean")

    if not enable:
        return None  # Explicitly disabled

    # Configuration parameters
    address = syslog_config.get("address", "localhost")
    port = int(syslog_config.get("port", 514))

    # If port is zero, then address is a local socket.
    if port:
        address = (address, port)

    use_tcp = syslog_config.get("use_tcp", False)
    if use_tcp is not True and use_tcp is not False:
        raise ValueError("logging.syslog.use_tcp in config must be boolean")

    # Get facility
    facname = syslog_config.get("facility", "user").lower()
    try:
        facility = logging.handlers.SysLogHandler.facility_names[facname]
    except KeyError:
        raise ValueError("unknown facility in logging.syslog.facility")

    handler = logging.handlers.SysLogHandler(
        address=address,
        facility=facility,
        socktype=socket.SOCK_STREAM if use_tcp else socket.SOCK_DGRAM,
    )

    # Format handler
    global daemon_fmt
    handler.setFormatter(daemon_fmt)

    # Log the start of syslogging to the alpenhorn logger.
    # We do this _before_ adding the file handler to prevent
    # duplicating this message (via both the syslog handler and
    # the start-up handler).
    alpen_logger = logging.getLogger("alpenhorn")
    if port:
        alpen_logger.info(
            f"Logging to syslog at {address}:{port} via "
            + ("TCP" if use_tcp else "UDP")
            + f" as facility {facname}"
        )
    else:
        alpen_logger.info(f"Logging to syslog socket {address} as facility {facname}")

    return handler


def configure_file_logging(file_config: dict) -> logging.Handler:
    """Configure a file logging handler based on the config.

    Paramters
    ---------
    file_config
        The contents of the `logging.file` section of the
        alpenhorn config.

    Returns
    -------
    file_handler
        The configured file handler

    Raises
    ------
    KeyError
        no `logging.file.name` was specified in the config
    ValueError
        a bad value was encountered in the logging config
    """

    if "name" not in file_config:
        raise KeyError("No logging.file.name in config")

    name = pathlib.Path(file_config["name"]).expanduser()

    watch = file_config.get("watch", False)
    if watch is not True and watch is not False:
        raise ValueError("logging.file.watch in config must be boolean")

    rotate = file_config.get("rotate", False)
    if rotate is not True and rotate is not False:
        raise ValueError("logging.file.rotate in config must be boolean")

    # Choose handler
    if rotate and watch:
        raise ValueError(
            "logging.file.rotate and logging.file.watch both true in config"
        )

    if rotate:
        # Alpenhorn is rotating the log
        try:
            backup_count = int(file_config.get("backup_count", 100))
        except ValueError:
            raise ValueError("Bad value for logging.file.backup_count")

        if backup_count <= 0:
            raise ValueError("Bad value for logging.file.backup_count")

        max_bytes = _max_bytes_from_config(file_config.get("max_bytes", "4G"))
        handler = RotatingFileHandler(
            name, maxBytes=max_bytes, backupCount=backup_count
        )
        how = " [rotating]"
    elif watch:
        # Someone else is rotating the log
        handler = logging.handlers.WatchedFileHandler(name)
        how = " [watching]"
    else:
        # No one is rotating the log
        handler = logging.FileHandler(name)
        how = ""

    # Format handler
    global daemon_fmt
    handler.setFormatter(daemon_fmt)

    # Log the start of file logging to the alpenhorn logger.
    # We do this _before_ adding the file handler to prevent
    # duplicating this message (via both the file handler and
    # the start-up handler).
    alpen_logger = logging.getLogger("alpenhorn")
    alpen_logger.info(f"Logging to{how} {name}")

    return handler


def configure_logging() -> None:
    """Configure the logger from from the config, and start logging.

    This will flush any log messages accumulated from program start until now
    to the log after it has been started.

    Raises
    ------
    KeyError
        A required key was missing from the logging config.
    ValueError
        An invalid value was found in the logging config.
    """

    def _check_level(level, source):
        if level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(f"Log level {level} defined by {source} is not valid")

    logger_config = config.config.get("logging", {})

    # Set the overall level
    level = logger_config["level"].upper()
    _check_level(level, "logging.level")

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Apply any module specific logging levels
    if "module_levels" in logger_config:
        for name, level in logger_config["module_levels"].items():
            logger = logging.getLogger(name)
            level = level.upper()

            _check_level(level, f"logging.module_levels.{name}")
            logger.setLevel(level)

    # Configure syslog logging, maybe
    if "syslog" in logger_config:
        syslog_handler = configure_sys_logging(logger_config["syslog"])
    else:
        syslog_handler = None

    # Configure file logging, maybe
    if "file" in logger_config:
        file_handler = configure_file_logging(logger_config["file"])
    else:
        file_handler = None

    # Start logging to the configured loggers
    global log_buffer
    for handler in [syslog_handler, file_handler]:
        if handler:
            root_logger.addHandler(handler)
            log_buffer.addTarget(handler)

    # Flush the start-up buffer to all targets
    log_buffer.flush()

    # Shut down and delete the start-up handler
    root_logger.removeHandler(log_buffer)
    log_buffer.close()
    log_buffer = None
