"""lfs(1) wrapper.

This module provides the `LFS` class which wraps invocations of
`lfs(1)` to interact with a Lustre filesystem.

It is not a complete wrapper for that command and is only able to
run these commands:

* `lfs quota`
    to retrieve the group quota for a path.  The user group used
    must be provided to the constructor.  Can also handle a
    fixed max quota, for cases where "lfs quota" doesn't correctly
    report the max.
* `lfs hsm_state`
    to retrieve the HSM state for a file.  The state itself is
    represented with the `HSMState` enum and is one of:
        * `MISSING`:    file is not present on disk or external storage
        * `UNARCHIVED`: file exists on disk, but not on external storage
        * `RESTORED`:   file exists on both disk and external storage
        * `RELEASED`:   file exists in external storage only
* `lfs hsm_restore`
    requests the state change `RELEASED -> RESTORED`
* `lfs hsm_release`
    requests the state change `RESTORED -> RELEASED`
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import shutil
import logging
import pathlib
from enum import Enum

from alpenhorn.common import util

if TYPE_CHECKING:
    import os
del TYPE_CHECKING


log = logging.getLogger(__name__)


class HSMState(Enum):
    """HSM States.

    Indicates the state of a file in HSM (the external storage).

    Four states are possible:
    HSMState.MISSING:
        The file is not on the system (disk or external storage) at all
    HSMState.UNARCHIVED:
        This file is on the Lustre disk but not in external storage.  This
        is the state of newly created files until they are archived.
    HSMState.RELEASED:
        The file is in external storage but not on disk.
    HSMState.RESTORED:
        The file is both in external storage and on disk.

    A new file starts off in state UNARCHIVED.  Initial archiving is beyond
    our control, but once the file has been archived, it moves from state
    UNARCHIVED to state RESTORED.

    A `lfs.hsm_restore()` changes a file's state from RELEASED to RESTORED.
    A `lfs.hsm_release()` changes a file's state from RESTORED to RELEASED.
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
    quota_group : str
        The name of the group to use when running quota queries
    fixed_quota : integer, optional
        If not None, a quota size in kiB which will override the
        maximum quota reported by "lfs quota".
    lfs : string, optional
        The name of the lfs(1) command; may include a path.
        Defaults to "lfs".
    path : string, optional
        If not None, the search path to use to look for the lfs(1)
        commnad.  If None, the "PATH" environmental variable is used.
    """

    # Conveniences for clients
    HSM_MISSING = HSMState.MISSING
    HSM_UNARCHIVED = HSMState.UNARCHIVED
    HSM_RESTORED = HSMState.RESTORED
    HSM_RELEASED = HSMState.RELEASED

    def __init__(
        self,
        quota_group: str,
        fixed_quota: int | None = None,
        lfs: str = "lfs",
        path: str | None = None,
    ) -> None:
        self._quota_group = quota_group
        self._fixed_quota = fixed_quota

        self._lfs = shutil.which(lfs, path=path)
        if self._lfs is None:
            raise RuntimeError("lfs command not found.")

    def run_lfs(self, *args: str, timeout: float | None = None) -> str | False | None:
        """Run the lfs command with the `args` provided.

        Parameters
        ----------
        *args : strings
            The list of command-line arguments to pass to the
            lfs command.
        timeout : float, optional
            If not None, stop waiting for the command after `timeout` seconds

        Retunrs
        -------
        output : str or False or None
            If the command succeeded, returns standard output of
            the command.  If the command failed or timed out,
            returns False (failed) or None (timed out) and logs
            the failure.
        """
        # Stringify args
        args = [str(arg) for arg in args]

        ret, stdout, stderr = util.run_command([self._lfs] + args, timeout=timeout)

        # Failure or timeout
        if ret is None or ret != 0:
            if ret is None:
                result = "timed out"
            else:
                result = f"failed (ret={ret})"
                ret = False
            log.warning(f"LFS command {result}: " + " ".join(args))
            if stderr:
                log.debug(f"LFS stderr: {stderr}")
            if stdout:
                log.debug(f"LFS stdout: {stdout}")
            return ret

        return stdout

    def quota_remaining(self, path: str | os.PathLike) -> int | None:
        """Retrieve the remaining quota for `path`.

        Parameters
        ----------
        path : str
            The path to get the quota for

        Returns
        -------
        quota_remaining : int or None
            The remaining quota for `path`, or None if running
            "lfs quota" failed.

        Raises
        ------
        ValueError
            The quota group is using the default block quota setting,
            but no fixed_quota was specified.
        """

        # If possible, "lfs quota -q -g <group> <path>" will output quota
        # information on the first line.  But, if the path is too long
        # (more than 15 characters), the path will be printed by itelf
        # on the first line and then the quota information will end up on
        # the second line.
        #
        # After the path, there are eight fields:
        #  - blocks used
        #  - block quota
        #  - block limit
        #  - block grace
        #  - files used
        #  - file quota
        #  - file limit
        #  - file grace
        #
        # Sometimes there are trailing lines.  If one of these contains
        # "using default block quota setting", then we know that the
        # default quota is in use.  We (non-root) don't have permission
        # to fetch the default quota, so we need to fall back on the fixed
        # quota in that case.

        # Stringify path
        path = str(path)

        stdout = self.run_lfs("quota", "-q", "-g", self._quota_group, path)
        if stdout is None:
            return None  # Command returned error

        # Split lines
        lines = stdout.splitlines()
        if len(lines) < 1:
            log.warning(f'Error parsing "lfs quota" output: {stdout}')
            return None

        # Remove the path, wherever it ended up
        if lines[0] == path:  # Did the line wrap because path was too long?
            lines.pop(0)  # Remove by discarding the whole line
            if len(lines) < 1:
                # Ran out of output
                log.warning(f'Error parsing "lfs quota" output: {stdout}')
                return None
        elif stdout.startswith(path):
            lines[0] = lines[0][len(path) :]  # Trim path
        else:
            log.warning(f'Error parsing "lfs quota" output: {stdout}')
            return None

        # Split the (potentially newly promoted) first line into the eight values
        lfs_quota = lines[0].split()
        if len(lfs_quota) != 8:
            log.warning(f'Error parsing "lfs quota" output: {stdout}')
            return None

        # Check the rest of the lines to see if we're using default block quota
        for line in lines[1:]:
            if "using default block quota setting" in line:
                if self._fixed_quota is None:
                    log.error(f"ERROR: Unable to fetch default block quota for {path}")
                    return None

        quota_limit = (
            self._fixed_quota if self._fixed_quota is not None else int(lfs_quota[1])
        )

        # If we're over quota, the quota value has a '*' appended to it
        quota = int(lfs_quota[0].rstrip("*"))

        # lfs quota reports values in kiByte blocks
        return (quota_limit - quota) * 2**10

    def hsm_state(self, path: os.PathLike | str) -> HSMState:
        """Returns the HSM state of path.

        Parameters
        ----------
        path : path-like
            The path to determine the state for.

        Returns
        -------
        state : HSMState or None
            The state of the file, or None, if running "lfs hsm_state"
            failed.  If `path` doesn't exist, this will be `HSMState.MISSING`.
        """

        # No need to check with HSM if the path isn't present
        if not pathlib.Path(path).exists():
            return HSMState.MISSING

        # Stringify path
        path = str(path)

        stdout = self.run_lfs("hsm_state", path)
        if stdout is False:
            return None  # Command returned error

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
        if stdout.startswith(path + ":"):
            stdout = stdout[len(path) :]
        else:
            log.warning(f"Error parsing hsm_state output: {stdout}")
            return None  # Parsing failed

        # Check some hus-states-words to figure out the state.  There are more
        # bits providing information, but I don't know if we care about them.
        #
        # See llapi_hsm_state_get(3) for full details about these.
        if "archived" not in stdout:
            return HSMState.UNARCHIVED
        if "released" in stdout:
            return HSMState.RELEASED
        return HSMState.RESTORED

    def hsm_archived(self, path: os.PathLike) -> bool:
        """Is `path` archived by HSM?"""
        state = self.hsm_state(path)
        return state == HSMState.RESTORED or state == HSMState.RELEASED

    def hsm_released(self, path: os.PathLike) -> bool:
        """Is `path` released to external storage?"""
        return self.hsm_state(path) == HSMState.RELEASED

    def hsm_restore(self, path: os.PathLike) -> bool | None:
        """Trigger restore of `path` from external storage.

        If `path` is already restored or is missing, this does nothing.

        Parameters
        ----------
        path : path-like
            The path to restore.

        Returns
        -------
        restored : bool or None
            None if the request timed out.
            False if the request failed.
            True if a successful restore request was made, or if
            `path` was already restored.
        """
        state = self.hsm_state(path)

        # If the file doesn't exist, fail
        if state == HSMState.MISSING:
            log.debug(f"Attempt to restore non-existent file: {path}")
            return False

        # If there's nothing to do, do nothing
        if state != HSMState.RELEASED:
            return True

        result = self.run_lfs("hsm_restore", path, timeout=60)
        if result is None or result is False:
            return result
        return True

    def hsm_release(self, path: os.PathLike) -> bool:
        """Trigger release of `path` from disk.

        If `path` is already released, or can't be released
        because it's either unarchived or missing, does nothing.

        Parameters
        ----------
        path : path-like
            The path to release.

        Returns
        -------
        released : bool
            True if a successful release request was made, or if
            `path` was already released.  False otherwise.
        """

        state = self.hsm_state(path)

        # If there's nothing to do, do nothing
        if state == HSMState.RELEASED:
            return True

        # If the file can't be released, fail
        if state != HSMState.RESTORED:
            log.debug(f'Unable to release "{path}" [state={state}]')
            return False

        # Otherwise send the request
        return self.run_lfs("hsm_release", path) is not False
