"""Set up logging for alpenhorn.

This module provides two important functions:

* `init_logging()` should be called as soon as possible after program start
    to turn on logging to standard error.  Any log messages produced before
    this call are discarded.

* `configure_logging()` should be called immediately after the alpenhorn
    config has been loaded.  It will re-configure the alpenhorn logger
    based on the alpenhorn configuration, including starting file or syslog-
    based logging, if requested.

alpenhorn buffers log messages emitted between these two calls and will flush
them to any additonal log destinations started by `configure_logging` so that
these messages are not lost.  (This is in addiiton to the messages being sent
immediately to standard error, which always happens.)

Note also that between the two calls, the log level of the root logger is
set to DEBUG.
"""

import socket
import logging
import pathlib
import logging.handlers

from . import config

try:
    from concurrent_log_handler import (
        ConcurrentRotatingFileHandler as RotatingFileHandler,
    )
except ImportError:
    RotatingFileHandler = logging.handlers.RotatingFileHandler

# The log format.  Used by the stderr log and any other log destinations
log_fmt = logging.Formatter(
    "%(asctime)s %(levelname)s >> [%(threadName)s] %(message)s",
    "%b %d %H:%M:%S",
)

# initialised by init_logging
log_buffer = None


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
        self.targets = list()

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
            self.targets = list()
            super().close()
        finally:
            self.release()


def init_logging() -> None:
    """Initialise the logger.

    This function is called before the config is read.  It sets up logging to
    standard error and also starts a log buffer where messages accumulate
    before the logging facilities defined by the configuration are started.
    """

    # This is the stderr logger.  It is always present, regardless of logging config
    log_stream = logging.StreamHandler()
    log_stream.setFormatter(log_fmt)

    # This is the start-up logger.  It buffers messages in memory until configure_logging()
    # is called, at which point the buffered messages are flushed to a file, if one was
    # opened, so that messages logged before the start of file logging are recorded,
    # and then this handler is shut down.
    global log_buffer
    log_buffer = StartupHandler(10000)

    # Set up initial logging
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(log_buffer)
    root_logger.addHandler(log_stream)

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
    global log_fmt
    handler.setFormatter(log_fmt)

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
    elif rotate:
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
    global log_fmt
    handler.setFormatter(log_fmt)

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

    # TODO: apply different settings for the client

    def _check_level(level, source):
        if level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(f"Log level {level} defined by {source} is not valid")

    logger_config = config.config.get("logging", dict())

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
