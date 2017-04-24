"""Setup logging for alpenhorn.
"""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
import logging
import logging.handlers
import sys
import os

# Use the concurrent logging file handler if we can
try:
    from cloghandler import ConcurrentRotatingFileHandler as RFHandler
except ImportError:
    # Next 2 lines are optional:  issue a warning to the user
    from warnings import warn
    warn("ConcurrentLogHandler package not installed.  Using builtin log handler")
    from logging.handlers import RotatingFileHandler as RFHandler

# Set up logger.
_log = logging.getLogger("alpenhornd")
_log.setLevel(logging.DEBUG)
log_stream = logging.StreamHandler(stream=sys.stdout)

log_fmt = logging.Formatter("%(asctime)s %(levelname)s >> %(message)s",
                            "%b %d %H:%M:%S")

log_stream.setLevel(logging.INFO)
log_stream.setFormatter(log_fmt)
_log.addHandler(log_stream)

# Find path to use for logging output (get from environment if possible)
log_path = "/var/log/alpenhorn/alpenhornd.log"  # default path

if 'ALPENHORN_LOG_FILE' in os.environ:
    log_path = os.environ['ALPENHORN_LOG_FILE']

# If log_path is set, set up as log handler
if log_path != "":
    log_file = RFHandler(log_path,
                         maxBytes=(2**22), backupCount=100)
    log_file.setLevel(logging.INFO)
    log_file.setFormatter(log_fmt)
    _log.addHandler(log_file)


def get_log():
    """Get a logging instance.
    """
    return _log
