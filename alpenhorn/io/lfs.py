"""LFS helper class.

Provides the LFS class which wraps calls to lfs(1) for use on Lustre filesystems
"""
import shutil
import logging
import pathlib
from enum import Enum

from alpenhorn import util

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
    HSMState.RESTORED:
        The file is both on tape and on disk.

    A new file starts off in state UNARCHIVED.  Initial archiving is beyond
    our control, but once the file has been archived, it moves from state
    UNARCHIVED to state RESTORED.

    A lfs.hsm_restore() changes a file's state from RELEASED to RESTORED.
    A lfs.hsm_release() changes a file's state from RESTORED to RELEASED.
    """

    MISSING = 0
    UNARCHIVED = 1
    RESTORED = 2
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

    # Conveniences for clients
    HSM_MISSING = HSMState.MISSING
    HSM_UNARCHIVED = HSMState.UNARCHIVED
    HSM_RESTORED = HSMState.RESTORED
    HSM_RELEASED = HSMState.RELEASED

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
        ret, stdout, stderr = util.run_command([self._lfs] + list(args))

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

        # There are two lines output by "lfs quota -q -g <group> <path>"
        #
        # The first line is just the path.
        #
        # The second line has eight fields:
        #  - blocks used
        #  - block quota
        #  - block limit
        #  - block grace
        #  - files used
        #  - file quota
        #  - file limit
        #  - file grace

        stdout = self.run_lfs("quota", "-q", "-g", self._quota_group, path)
        if stdout is None:
            return None

        # Split lines
        lines = stdout.splitlines()

        # Split the second line into the eight values
        lfs_quota = lines[1].split()

        quota_limit = (
            self._fixed_quota if self._fixed_quota is not None else int(lfs_quota[1])
        )

        # lfs quota reports values in kiByte blocks
        return (quota_limit - int(lfs_quota[0])) * 2**10.0

    def hsm_state(self, path):
        """Returns the HSM state of path.

        Returns a HSMState enum value, or None if there was an
        error running "lfs hsm_state".
        """

        # No need to check with HSM if the path isn't present
        if not pathlib.Path(path).exists():
            return HSMState.MISSING

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
        if "archived" not in stdout:
            return HSMState.UNARCHIVED
        if "released" in stdout:
            return HSMState.RELEASED
        return HSMState.RESTORED

    def hsm_archived(self, path):
        """Is this file archived?"""
        state = self.hsm_state(path)
        return state == HSMState.RESTORED or state == HSMState.RELEASED

    def hsm_released(self, path):
        """Is this file released?"""
        return self.hsm_state(path) == HSMState.RELEASED

    def hsm_restore(self, path):
        """Trigger restore of path from tape.

        If path is already restored, returns True.

        Otherwise, returns a boolean indicating whether the restore request was
        successful.
        """

        state = self.hsm_state(path)

        # If the file doesn't exist, fail
        if state == HSMState.MISSING:
            return False

        # If there's nothing to do, do nothing
        if state != HSMState.RELEASED:
            return True

        return self.run_lfs("hsm_restore", path) is not None

    def hsm_release(self, path):
        """Trigger release of path from disk.

        If path is already released, returns True.

        Otherwise, returns a boolean indicating whether the release request was
        successful.
        """

        state = self.hsm_state(path)

        # If there's nothing to do, do nothing
        if state == HSMState.RELEASED:
            return True

        # If the file can't be released, fail
        if state != HSMState.RESTORED:
            return False

        # Otherwise send the request
        return self.run_lfs("hsm_release", path) is not None
