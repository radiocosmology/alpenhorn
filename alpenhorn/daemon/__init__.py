"""Alpenhorn daemon."""

from .. import __version__
from .entry import entry
from .update import host

# These classes are used by extensions, so let's import them into the daemon
# base
from .update import UpdateableNode, UpdateableGroup, RemoteNode
