"""Alpenhorn Default Remote I/O class.

This is the I/O class used to interface with StorageNodes using the
"Default" I/O class (perhaps implcitly due to not explicitly specifying
an I/O class) which are the source-side of a pull requets (meaning they're
not expected to be locally available).
"""

from __future__ import annotations

from ...db import ArchiveFile
from ..base import BaseNodeRemote


class DefaultNodeRemote(BaseNodeRemote):
    """I/O class for a remote DefaultIO StorageNode."""

    def pull_ready(self, file: ArchiveFile) -> bool:
        """Is `file` ready for pulling from this remote node?

        Parameters
        ----------
        file : ArchiveFile
            the file being checked

        Returns
        -------
        True
            Files on Default nodes are always ready.
        """
        return True
