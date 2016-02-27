"""Setup logging for alpenhorn.
"""
import logging
import logging.handlers
import sys
import os
import socket


# Set up logger.
logging.basicConfig(level=logging.INFO)
log_fmt = logging.Formatter("%(asctime)s %(levelname)s >> %(message)s",
                            "%b %d %H:%M:%S")
log = logging.getLogger("")
log.setLevel(logging.INFO)
if sys.argv[0] == "/usr/sbin/alpenhornd":
    log_file = logging.handlers.TimedRotatingFileHandler(
        "/var/log/alpenhorn/alpenhornd.log",
        when="W0",
        backupCount=100)
    log_file.setLevel(logging.DEBUG)
    log_file.setFormatter(log_fmt)
    log.addHandler(log_file)
elif socket.gethostname()[:3] == 'gpc':
    # On Scinet
    log_path = os.path.expanduser('/project/k/krs/alpenhorn/alpenhornd.log')
    log_file = logging.handlers.TimedRotatingFileHandler(log_path, when='W0', backupCount=100)
    log_file.setLevel(logging.INFO)
    log_file.setFormatter(log_fmt)
    log.addHandler(log_file)


def get_log():
    return log
