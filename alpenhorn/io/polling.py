"""Polling Node I/O class.

The same as DefaultNodeIO but uses the PollingObserver for auto-import.

Use in situations where inotify won't work (like NFS mounts).
"""

from watchdog.observers.polling import PollingObserver

from .base import InternalIO
from .default import DefaultNodeIO


class PollingNodeIO(DefaultNodeIO):
    observer = PollingObserver


PollingIO = InternalIO(__name__, PollingNodeIO, None)
