"""Alpenhorn LFSQuota Node I/O class.

This I/O class extends the DefaultNodeIO.  The primary change is that it uses the
"lfs quota" command to determine free space.
"""
import logging

from alpenhorn.io.Default import DefaultNodeIO
from .lfs import LFS

log = logging.getLogger(__name__)


class LFSQuotaNodeIO(DefaultNodeIO):
    """An extension to DefaultNodeIO which uses the "lfs quota" to determine
    free space, rather than stat.

    Required io_config keys:
        * quota_group: the user group to query quota for

    Optional io_config keys:
        * fixed_quota: a fixed number of kiB to use to override the max quota
            reported by the "lfs quota" command.
        * lfs: the lfs(1) executable.  Defaults to "lfs"; may be a full path.
    """

    def __init__(self, node):
        super().__init__(node)

        # Make alpenhornd crash if someone's been screwing up the node config in the database
        if "quota_group" not in self.config:
            raise KeyError(
                f'"quota_group" missing from StorageNode {node.name} io_config'
            )

        self._lfs = LFS(
            quota_group=self.config["quota_group"],
            fixed_quota=self.config.get("fixed_quota", None),
            lfs=self.config.get("lfs", "lfs"),
        )

    def bytes_avail(self, fast=False):
        """Use "lfs quota" get the amount of free quota.

        Fast calls are skipped.
        """
        if fast:
            return None

        return self._lfs.quota_remaining(self.node.root)
