"""Cedar nearline I/O.

These I/O classes are specific to the nearline tape archive at cedar.

Required io_config keys:
    * quota_group: the user group to query quota for
    * quota_blocks: the quota, in kiB, on the /nearline filesystem
"""

from alpenhorn.io.base import BaseNodeIO, BaseGroupIO
from alpenhorn.util import run_command

import alpenhorn.logging as logging

log = logging.getLogger(__name__)


class NearlineNodeIO(BaseNodeIO):
    def bytes_avail(self):
        import re

        # Fast calls are skipped
        if fast:
            return None

        # If the quota group or quota blocks are not defined, we can't determine free space
        if self.confg.get("quota_group") is None:
            log.error('No "quota_group" defined for node f{self.node.name}')
            return None
        if self.confg.get("quota_blocks") is None:
            log.error('No "quota_blocks" defined for node f{self.node.name}')
            return None

        # Strip non-numeric things
        regexp = re.compile(b"[^\d ]+")

        ret, stdout, stderr = run_command(
            [
                "/usr/bin/lfs",
                "quota",
                "-q",
                "-g",
                self.confg["quota_group"],
                "/nearline",
            ]
        )
        lfs_quota = regexp.sub("", stdout).split()

        # lfs quota reports values in kiByte blocks
        node.avail_gb = (self.config["quota_blocks"] - int(lfs_quota[0])) * 2**10


class NearlineGroupIO(BaseGroupIO):
    pass
