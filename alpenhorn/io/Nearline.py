"""Cedar nearline I/O.

These I/O classes are specific to the nearline tape archive at cedar.
"""

import os
import logging

import alpenhorn.archive as ar
from alpenhorn.io.base import BaseGroupIO
from alpenhorn.io.LFSQuota import LFSQuotaNodeIO
from . import lfs

log = logging.getLogger(__name__)


def NearlineNodeRemote(BaseNodeRemote):
    """NearlineNodeRemote: information about a Nearline remote StorageNode."""

    def pull_ready(self, file):
        """Returns True if the file copy is ready (not released)."""
        try:
            copy = ar.ArchiveFileCopy.get(file=file, node=self.node)
        except pw.DoesNotExist:
            return False

        return copy.ready


class NearlineNodeIO(LFSQuotaNodeIO):
    """Nearline node I/O.

    Required io_config keys:
        * quota_group : string
            the user group to query quota for
        * fixed_quota : integer
            the quota, in kiB, on the Nearline disk

    Optional io_config keys:
        * lfs : string
            the lfs(1) executable.  Defaults to "lfs"; may be a full path.
        * headroom: integer
            the amount of space in bytes to keep empty on the disk.  Defaults
            to 25% of fixed_quota.
    """

    remote_class = NearlineNodeRemote

    def __init__(self, node):
        super().__init__(node)

        # 25% of 1 kiB == 2**8 bytes
        self._headroom = self.config.get(
            "headroom", self.config["fixed_quota"] * 2**8
        )

    def check_active(self):
        """Returns True.

        There's no ALPENHORN_NODE file on nearline.
        """
        return True

    def fits(self, size_b):
        """Returns True.

        Everything fits on Nearline."""
        return True

    def filesize(self, path, actual=False):
        """Returns the size of the file given by path.

        This _always_ returns the apprent size, since the size on tape is
        not known (or important).
        """
        path = pathlib.PurePath(path)
        if not path.is_absolute():
            path = pathlib.PurePath(self.node.root, path)
        return os.path.getsize(path)

    def reserve_bytes(self, size, check_only=False):
        """Returns True."""
        return True

    def release_bytes(self, size):
        """Does nothing."""
        pass

    def check(self, copy):
        """Make sure the file is recalled before trying to check it."""

        # If the file is released, recall it
        if self._lfs.hsm_released(copy.path):
            self._lfs.hsm_recall(copy.path)
            return

        # Otherwise, use the DefaultIO method to do the check
        super().check(copy)

    def ready(self, req):
        """Recall a file, if necessary, to prepare for a remote pull."""
        # If the file is released, recall it
        state = self._lfs.hsm_state(copy.path)

        # Update DB based on HSM state
        ar.ArchiveFileCopy.update(
            ready=(state == self._lfs.HSM_RECALLED or state == self._lfs.HSM_UNARCHIVED)
        ).where(ar.ArchiveFileCopy.id == copy.id).execute()

        # If it's recallable, recall it
        if state == self._lfs.HSM_RELEASED:
            self._lfs.hsm_recall(copy.path)


class NearlineGroupIO(BaseGroupIO):
    """Nearline Group I/O

    The Nearline Group contains two nodes:
     - a primary node pointing to the nearline storage itself.
     - a secondary node on a regular disk used for storing files too small for nearline.

    Use the "threshold" io_config key to change the smallfile threshold.
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
            log.error(
                f"need exactly two nodes in Nearline group {self.group.name} (have {len(node)})"
            )
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
