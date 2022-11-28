"""Cedar nearline I/O.

These I/O classes are specific to the nearline tape archive at cedar.

Required StorageNode io_config keys:
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
    """Nearline Group I/O

    The Nearline Group contains two nodes:
     - a primary node pointing to the nearline storage itself.
     - a secondary node on a regular disk used for storing files too small for nearline.

    Use the "threshold" io_config key to specify the smallfile threshold.
    Default is 1000000000 bytes (1 GB).

    To identify the nodes, the primary node must have the same name as the group.
    """

    def __init__(self, group):
        super().__init__(group)
        self._threshold = self.config.get("threshold", 1000000000)  # bytes

    def before_update(self, nodes, queue_empty):
        """Identify primary and secondary nodes.

        If both nodes aren't online, we cancel the update.
        """
        if len(nodes) != 2:
            log.error(f"need exactly two nodes in Nearline group {self.group.name}")
            return True

        if nodes[0].name == self.group.name:
            self._nearline = nodes[0]
            self._smallfile = nodes[1]
        elif nodes[1].name == self.group.name:
            self._nearline = nodes[1]
            self._smallfile = nodes[0]
        else:
            log.error(f"no node in Nearline group named {self.group.name}")
            return True

        return False

    def pull(self, req):
        """Pass ArchiveFileCopyRequest to the correct node."""
        if req.file.size_b <= self._threshold:
            self._smallfile.io.pull(req)
        else:
            self._nearline.io.pull(req)
