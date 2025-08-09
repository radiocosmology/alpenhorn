"""``alpenhorn.io.polling``: Polling I/O class.

This module defines the `PollingNodeIO` class, which works the same as
`DefaultNodeIO` but uses the `PollingObserver` for auto-import.

Use in situations where normal filesystem activity detection won't work
(like NFS mounts).
"""

from watchdog.observers.polling import PollingObserver

from .default import DefaultNodeIO


class PollingNodeIO(DefaultNodeIO):
    """Poling Node I/O.

    This Node I/O class is the same as the DefaultNodeIO,
    except that it uses a polling observer to detect changes
    to the filesystem.

    Use this I/O class for `StorageNode` instances which can't
    be monitored by the normal observer (e.g. use this for NFS
    or other network mounts).
    """

    observer = PollingObserver
