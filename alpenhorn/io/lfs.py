"""LFS helper class.

Provides the LFS class which wraps calls to lfs(1) for use on Lustre filesystems
"""
import re
import shutil
import logging
from enum import Enum

from alpenhorn.util import run_command

log = logging.getLogger(__name__)


class HSMState(Enum):
    """HSM States.

    Indicates the state of a file in HSM (the nearline tape archive).

    Four states are possible:
    HSMState.MISSING:
        The file is not on nearline at all
    HSMState.UNARCHIVED:
        This file is on the /nearline disk but not on tape.  This is
        the state of newly created files until they are archived.
    HSMState.RELEASED:
        The file is on tape but not on disk.
    HSMState.RECALLED:
        The file is both on tape and on disk.

    A new file starts off in state UNARCHIVED.  Initial archiving is beyond
    our control, but once the file has been archived, it moves from state
    UNARCHIVED to state RECALLED.

    A lfs.hsm_recall() changes a file's state from RELEASED to RECALLED.
    A lfs.hsm_release() changes a file's state from RECALLED to RELEASED.
    """

    MISSING = 0
    UNARCHIVED = 1
    RECALLED = 2
    RELEASED = 3


class LFS:
    """A class that wraps invocations of the lfs(1) command for use on Lustre
    filesystems.

    If the lfs(1) command can't be found in the PATH, attempting to instantiate
    this class will fail with RuntimeError.

    Parameters:
    -----------
        - quota_group : string
            The name of the group to use when running quota queries
        - fixed_quota : integer or None
            Set to something other than None to override the max
            quota reported by "lfs quota".
        - lfs : string
            The name of the lfs command, may be a path.  Defaults to "lfs".
        - path : string or None
            The path to search for the lfs executable.  By default PATH
            is searched.
    """

    def __init__(self, quota_group, fixed_quota=None, lfs="lfs", path=None):
        self._quota_group = quota_group
        self._fixed_quota = fixed_quota

        self._lfs = shutil.which(lfs, path=path)
        if self._lfs is None:
            raise RuntimeError("lfs command not found.")

    def run_lfs(self, *args):
        """Run the lfs command with the args provided.

        Returns stdout if the command was successful or
        None if it failed.
        """
        ret, stdout, stderr = run_command([self._lfs] + args)

        if ret != 0:
            log.warning(f"LFS command failed (ret={ret}): " + " ".join(args))
            if stderr:
                log.debug(f"LFS stderr: {stderr}")
            if stdout:
                log.debug(f"LFS stdout: {stdout}")
            return None

        return stdout

    def quota_remaining(self, path):
        """Return the remaining quota for "path".

        Returns None if running "lfs quota" fails.
        """

        # Strip non-numeric things
        regexp = re.compile(b"[^\d ]+")

        stdout = self.run_lfs("quota", "-q", "-g", self._quota_group, path)
        if stdout is None:
            return None

        lfs_quota = regexp.sub("", stdout).split()

        quota_limit = (
            self._fixed_quota if self._fixed_quota is not None else int(lfs_quota[1])
        )

        # lfs quota reports values in kiByte blocks
        node.avail_gb = (quota_limit - int(lfs_quota[0])) * 2**10.0

    def hsm_state(self, path):
        """Returns the HSM state of path.

        Returns a HSMState enum value, or None if there was an
        error running "lfs hsm_state".
        """

        # No need to check with HSM if the path isn't present
        if not pathlib.Path(path).exists():
            return HSM_MISSING

        stdout = self.run_lfs("hsm_state", path)
        if stdout is None:
            return None

        # The output of hsm_state looks like this:
        #
        # <path>: (<hus-states-bits>) [hus-states-words][, archive_id:<archive-id>]
        #
        # where:
        #  - "path" is the path verbatim from the command line
        #  - "hus-states-bits" is a "0x%08x"-formatted hex representation of the
        #                      hus_states bitfield
        #  - "hus-states-words" are specific words, separated by spaces
        #                      one per hus_states bit set.  There may be none
        #                      of these if none of the bits are set
        #  - "archive-id" is the archive ID (i.e. HSM backend index) for this
        #                      file.  If the file is unarchived, this whole
        #                      part, starting with the comma, is omitted.

        # Strip path from the output to handle the corner case where, say,
        # "archived" is part of the filename.
        stdout = stdout[len(path) :]

        # Check some hus-states-words to figure out the state.  There are more
        # bits providing information, but I don't know if we care about them.
        #
        # See llapi_hsm_state_get(3) for full details about these.
        if not "archived" in stdout:
            return HSM_UNARCHIVED
        if "released" in stdout:
            return HSM_RELEASED
        return HSM_RECALLED

    def hsm_released(self, path):
        """Is this file released?"""
        return self.hsm_state(path) == HSM_RELEASED

    def hsm_recall(self, path):
        """Trigger recall of path from tape.

        If path is already recalled, returns True.

        Otherwise, returns a boolean indicating whether the recall request was
        successful.
        """

        state = self.hsm_state(path)

        # If the file doesn't exist, fail
        if state == HSM_MISSING:
            return False

        # If there's nothing to do, do nothing
        if state != HSM_RELEASED:
            return True

        return self.run_lfs("hsm_recall", path) is not None

    def hsm_release(self, path):
        """Trigger release of path from disk.

        If path is already released, returns True.

        Otherwise, returns a boolean indicating whether the release request was
        successful.
        """

        state = self.hsm_state(path)

        # If there's nothing to do, do nothing
        if state == HSM_RELEASED:
            return True

        # If the file can't be released, fail
        if state != HSM_RECALLED:
            return False

        # Otherwise send the request
        return self.run_lfs("hsm_release", path) is not None