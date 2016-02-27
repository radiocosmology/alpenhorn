"""Setup logging for alpenhorn.
"""
import logging
import logging.handlers
import sys
import os
import socket


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

# If run as system service
if sys.argv[0] == "/usr/sbin/alpenhornd":
    log_path = "/var/log/alpenhorn/alpenhornd.log"
elif socket.gethostname()[:3] == 'gpc':
    # On Scinet
    log_path = os.path.expanduser('/project/k/krs/alpenhorn/alpenhornd.log')

# If file exists, set up as log handler
if log_path is not None:
    log_file = RFHandler("/var/log/alpenhorn/alpenhornd.log",
                         maxBytes=(2**22), backupCount=100)
    log_file.setLevel(logging.DEBUG)
    log_file.setFormatter(log_fmt)
    log.addHandler(log_file)


def get_log():
    """Get a logging instance.
    """
    return log
