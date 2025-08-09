"""``alpenhorn.daemon``: The Alpenhorn daemon implementation.

This module implements the update loop core of the alpenhorn daemon.
The daemon's task scheduler is implemented in `alpenhorn.scheduler`.

The entry-point for the daemon is in `alpenhorn.daemon.entry`.
"""

from .. import __version__
from .entry import entry

# These classes are used by extensions, so let's import them into the daemon
# base
from .update import UpdateableNode, UpdateableGroup, RemoteNode
