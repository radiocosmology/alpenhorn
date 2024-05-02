"""Polling Node I/O class.

The same as DefaultNodeIO but uses the PollingObserver for auto-import.

Use in situations where inotify won't work (like NFS mounts).
"""

from watchdog.observers.polling import PollingObserver

from .default import DefaultNodeIO


class PollingNodeIO(DefaultNodeIO):
    observer = PollingObserver
