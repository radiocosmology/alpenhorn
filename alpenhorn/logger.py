"""Setup logging for alpenhorn.
"""
import logging
import logging.handlers
import os

# Find path to use for logging output (get from environment if possible)
log_path = "/var/log/alpenhorn/alpenhornd.log"  # default path

if 'ALPENHORN_LOG_PATH' in os.environ:
    log_path = os.environ['ALPENHORN_LOG_PATH']


# Use the concurrent logging file handler if we can
try:
    from cloghandler import ConcurrentRotatingFileHandler as RFHandler
except ImportError:
    # Next 2 lines are optional:  issue a warning to the user
    from warnings import warn
    warn("ConcurrentLogHandler package not installed.  Using builtin log handler")
    from logging.handlers import RotatingFileHandler as RFHandler

# Set up logger.
logging.basicConfig(level=logging.INFO)
log_fmt = logging.Formatter("%(asctime)s %(levelname)s >> %(message)s",
                            "%b %d %H:%M:%S")
log = logging.getLogger("")
log.setLevel(logging.INFO)

log_path = None

# If log_path is set, set up as log handler
if log_path != "":
    log_file = RFHandler(log_path,
                         maxBytes=(2**22), backupCount=100)
    log_file.setLevel(logging.DEBUG)
    log_file.setFormatter(log_fmt)
    log.addHandler(log_file)


def get_log():
    """Get a logging instance.
    """
    return log
