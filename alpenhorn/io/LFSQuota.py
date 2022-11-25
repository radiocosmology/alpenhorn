"""Alpenhorn LFSQuota Node I/O classes.

These classes extend the Default I/O.  The primary change is that they use the
"lfs quota" command to determine free space.


Required io_config keys:
    * quota_group: the user group to query quota for

Optional io_config keys:
    * quota_root: the mountpoint to use for the quota query.  If not given the value
        of "node.root" is used.
"""

from alpenhorn.io.Default import DefaultNodeIO
from alpenhorn.util import run_command

import alpenhorn.logging as logging

log = logging.getLogger(__name__)


def LFSQuotaNodeIO(DefaultNodeIO):
    """An extension to DefaultNodeIO which uses the "lfs quota" to determine free space, rather than stat."""
    pass

    def bytes_avail(self, fast=False):
        import re

        # Fast calls are skipped
        if fast:
            return None

        # If the quota group is not defined, we can't determine free space
        if self.confg.get("quota_group") is None:
            log.error("No quota group defined for node f{self.node.name}")
            return None

        # Strip non-numeric things
        regexp = re.compile(b"[^\d ]+")

        ret, stdout, stderr = run_command(
            [
                "/usr/bin/lfs",
                "quota",
                "-q",
                "-g",
                self.config["quota_group"],
                self.config.get("quota_root", self.node.root),
            ]
        )
        lfs_quota = regexp.sub("", stdout).split()

        # lfs quota reports values in kiByte blocks
        node.avail_gb = (int(lfs_quota[1]) - int(lfs_quota[0])) * 2**10.0
