"""Setup logging for alpenhorn.
"""

import logging

from . import config

root_logger = logging.getLogger()

log_stream = logging.StreamHandler()

log_fmt = logging.Formatter(
    "%(asctime)s %(levelname)s >> [%(threadName)s] %(message)s", "%b %d %H:%M:%S"
)

log_stream.setFormatter(log_fmt)

root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(log_stream)


def start_logging():
    """From the config, setup logging."""

    # TODO: apply different settings for the client

    # TODO: add in a way of taking completely custom logging parameters from the
    # config using logging.dictConfig

    # TODO: while I'm mostly buying into the philosophy that logs should
    # generally just be sent to stdout and then be left to the system to deal
    # with, on SciNet it's probably useful to specify file locations, so we
    # should bring that back

    def _check_level(level):

        if level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise RuntimeError("Log level %s is not valid" % level)

    # Set the overall level
    level = config.config["logging"]["level"].upper()
    _check_level(level)

    root_logger.setLevel(level)

    # Apply any module specific logging levels
    for name, level in config.config["logging"]["module_levels"].items():

        logger = logging.getLogger(name)
        level = level.upper()

        _check_level(level)
        logger.setLevel(level)
