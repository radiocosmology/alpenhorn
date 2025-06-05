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


def configure_sys_logging() -> logging.handlers.SysLogHandler | None:
    """Configure a syslog logging handler based on the config.

    Returns
    -------
    syslog_handler
        The configured syslog handler

    Raises
    ------
    click.ClickException
        a bad value was encountered in the logging config
    """

    if not config.get("logging.syslog.enable", default=True, as_type=bool):
        return None  # Explicitly disabled

    # Configuration parameters
    address = config.get("logging.syslog.address", default="localhost", as_type=str)
    port = config.get_int("logging.syslog.port", default=514, min=0, max=65535)

    # If port is zero, then address is a local socket.
    if port:
        address = (address, port)

    use_tcp = config.get("logging.syslog.use_tcp", default=False, as_type=bool)

    # Get facility
    facname = config.get("logging.syslog.facility", default="user", as_type=str).lower()
    try:
        facility = logging.handlers.SysLogHandler.facility_names[facname]
    except KeyError:
        raise click.ClickException(
            f"unknown facility {facname} in logging.syslog.facility"
        )

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


def configure_file_logging() -> logging.Handler:
    """Configure a file logging handler based on the config.

    Returns
    -------
    file_handler
        The configured file handler

    Raises
    ------
    click.ClickException
        a bad value was encountered in the logging config
    """

    name = pathlib.Path(config.get("logging.file.name", as_type=str)).expanduser()

    watch = config.get("logging.file.watch", default=False, as_type=bool)
    rotate = config.get("logging.file.rotate", default=False, as_type=bool)

    # Choose handler
    if rotate and watch:
        raise click.ClickException(
            "logging.file.rotate and logging.file.watch both true in config"
        )

    if rotate:
        # Alpenhorn is rotating the log
        backup_count = config.get_int("logging.file.backup_count", default=10, min=1)
        max_bytes = config.get_bytes("logging.file.max_bytes", default="4M")
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

    def _get_level(path: str, default: str = "INFO") -> str:
        level = config.get(path, default=default, as_type=str).upper()
        if level and level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(f"Log level {level} defined by {path} is not valid")
        return level

    # Set the overall level
    root_logger = logging.getLogger()
    root_logger.setLevel(_get_level("logging.level"))

    # Apply any module specific logging levels
    module_levels = config.get("logging.module_levels", default={}, as_type=dict)
    for name in module_levels:
        logger = logging.getLogger(name)
        logger.setLevel(_get_level(f"logging.module_levels.{name}"))

    # Configure syslog logging, maybe
    if config.get("logging.syslog", default=None, as_type=dict):
        syslog_handler = configure_sys_logging()
    else:
        syslog_handler = None

    # Configure file logging, maybe
    if config.get("logging.file", default=None, as_type=dict):
        file_handler = configure_file_logging()
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
