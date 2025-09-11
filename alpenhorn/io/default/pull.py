"""File copying functions.

These are the low-level functions used by Default I/O
to copy files around.
"""

from __future__ import annotations

import logging
import os
import pathlib
import re
import shutil
import time
from tempfile import TemporaryDirectory

from ...common import config, util
from ...common.metrics import Metric
from ...daemon import RemoteNode
from ...daemon.pullutil import copy_request_done
from ...daemon.scheduler import Task, threadlocal
from ...db import (
    ArchiveFileCopyRequest,
)
from ..base import BaseNodeIO
from .check import force_check_filecopy
from .updownlock import UpDownLock

__all__ = ["bbcp", "hardlink", "local_copy", "rsync"]

log = logging.getLogger(__name__)


def _pull_timeout(size_b: int) -> float | None:
    """Given a file of `size_b` bytes, return the timeout in seconds.

    The timeout used for a bbcp or rsync call is

        pull_timeout_base + file.size_b / pull_bytes_per_second

    where `pull_timeout_base` and `pull_bytes_per_second` may
    be given in the "daemon" section of the config.

    If `pull_bytes_per_second` is zero, None is returned (disabling
    the timeout).

    If not given, the defaults are:

        pull_timeout_base = 300          (five minutes)
        pull_bytes_per_second = 20000000 (20MB/s)

    which are conservative CHIME values for cedar.

    Sample values with the defaults here:

    File              Time @ 50MB/s     Timeout

    100GB chime_hfb       33m           1h 28m
     30GB chimestack      10m              30m
      2GB chimegain       40s               6m
    230MB chimetiming      5s               5m
    """

    # These may be overridden at runtime via the config
    PULL_TIMEOUT_BASE = 300  # 5 minutes base
    PULL_BYTES_PER_SECOND = 20000000  # 20MB/s

    base = config.get_float("daemon.pull_timeout_base", default=PULL_TIMEOUT_BASE)
    bps = config.get_float(
        "daemon.pull_bytes_per_second", default=PULL_BYTES_PER_SECOND
    )

    if bps == 0:
        return None

    return base + size_b / bps


def bbcp(source: str | os.PathLike, target: str | os.PathLike, size_b: int) -> dict:
    """Transfer a file with BBCP.

    Command times out after `_pull_timeout(size_b)` seconds have elapsed.

    Parameters
    ----------
    source : path-like
        Source location
    target : path-like
        Target location
    size_b : int
        Size of the file to transfer.  Only used to
        set the timeout.

    Returns
    -------
    ioresult : dict
        Result of the transfer, with keys:
        "ret": int
            exit code of bbcp (0 == success)
        "md5sum": str
            MD5 hash of the transferred file.  Only present
            if ret == 0
        "stderr": str
            Standard error output from bbcp
        "check_src": bool
            True unless it's clear that a failure wasn't
            due to a problem with the source file.
    """

    # Set port number, which is different for each worker
    # We use 4200 for the main thread and increase by ten
    # for each worker.
    port = 4200 + getattr(threadlocal, "worker_id", 0) * 10

    ret, stdout, stderr = util.run_command(
        [  # See: https://www.slac.stanford.edu/~abh/bbcp/
            "bbcp",
            #
            #
            # -V (AKA --vverbose, with two v's) is here because
            # bbcp is weirdly broken.
            #
            # There's a bug in bbcp (which DVW doesn't want
            # to explain in this comment), that affects the
            # CHIME site-to-cedar transfers, but (essentially)
            # turning on two-v-verbose mode changes the code
            # path in bbcp to avoid the bug. We do not care
            # _at all_ about the extra verbose output: it all
            # just goes in the bin.  XXX Probably we should
            # patch the bbcp on cedar to fix the bug.
            "-V",
            #
            #
            # force: delete an existing destination file before
            # transfer
            "-f",
            #
            #
            # Use a reverse connection to get through a firewall
            # (This may not be appropriate everywhere -- more reason
            # we need an edge table in the database.)
            "-z",
            #
            #
            # Port to use
            "--port",
            str(port),
            #
            #
            # TCP window size.  4M is what Linux typically limits
            # you to (cf. /proc/sys/net/ipv4/tcp_wmem)
            "-W",
            "4M",
            #
            #
            # Number of streams
            "-s",
            "16",
            #
            #
            # Do block-level checksumming to detect transmission
            # errors
            "-e",
            #
            #
            # Calculate _and print_ a MD5 checksum of the whole file
            # on the source.  MD5ing is done on the source to avoid
            # the need for the file transfer to occur in order
            # (which can cause bbcp to lock up).
            #
            # See https://www.slac.stanford.edu/~abh/bbcp/#_Toc392015140
            # and https://github.com/chime-experiment/alpenhorn/pull/15
            "-E",
            "%md5=",
            str(source),
            str(target),
        ],
        timeout=_pull_timeout(size_b),
    )

    # Attempt to parse STDERR for the md5 hash
    ioresult = {"ret": ret, "stderr": stderr, "check_src": True}
    if ret == 0:
        mo = re.search("md5 ([a-f0-9]{32})", stderr)
        if mo is None:
            log.error(
                f"BBCP transfer has gone awry. STDOUT: {stdout}\n STDERR: {stderr}"
            )
            return {
                "ret": -1,
                "stderr": "Unable to read m5sum from bbcp output",
                "check_src": False,
            }

        ioresult["md5sum"] = mo.group(1)

    return ioresult


def rsync(
    source: str | os.PathLike, target: str | os.PathLike, size_b: int, local: bool
) -> dict:
    """Rsync a file (either local or remote).

    Command times out after `_pull_timeout(size_b)` seconds have elapsed.

    Parameters
    ----------
    source : path-like
        Source location
    target : path-like
        Target location
    size_b : int
        Size of the file to transfer.  Only used to
        set the timeout.
    local : bool
        False if this is a network transfer.  In that
        case, compression is turned on in rsync.

    Returns
    -------
    ioresult : dict
        Result of the transfer, with keys:
        "ret": int
            exit code of rsync (0 == success)
        "md5sum": bool
            True if ret == 0, absent otherwise
        "stderr": str
            Standard error output from rsync
        "check_src": bool
            True unless it's clear that a failure wasn't
            due to a problem with the source file.
    """

    if local:
        remote_args = []
    else:
        remote_args = [
            "--compress",
            "--rsync-path=ionice -c2 -n4 rsync",
            "--rsh=ssh -q",
        ]

    ret, _, stderr = util.run_command(
        [
            "rsync",
            *remote_args,
            "--quiet",
            "--times",
            "--protect-args",
            "--perms",
            "--group",
            "--owner",
            "--copy-links",
            "--sparse",
            str(source),
            str(target),
        ],
        timeout=_pull_timeout(size_b),
    )

    ioresult = {"ret": ret, "stderr": stderr}

    # If the rsync error occured during `mkstemp` or during a write, this is a
    # problem on the destination, not the source
    if ret:
        if "mkstemp" in stderr:
            log.warning("rsync file creation failed")
            ioresult["check_src"] = False
        elif "write failed on" in stderr:
            log.warning(
                "rsync failed to write to destination: "
                + stderr[stderr.rfind(":") + 2 :].strip()
            )
            ioresult["check_src"] = False
        else:
            # Other error, perhaps due to source
            ioresult["check_src"] = True
    else:
        # rsync guarantees the md5 sum of a file is not changed if a
        # transfer succeeds (ret == 0)
        ioresult["md5sum"] = True

    return ioresult


def hardlink(
    from_path: str | os.PathLike, to_dir: str | os.PathLike, filename: str
) -> dict | None:
    """Hard link `from_path` as `to_dir/filename`

    Atomically overwrites an existing `to_dir/filename`.

    If hardlinking fails, this funciton assumes it's because src and dest
    aren't on the same filesystem (i.e., failure is probably not for an
    interesting reason).

    Parameters
    ----------
    from_path : path-like
        Source location
    to_dir : path-like
        Destination directory
    filename : str
        Destination filename

    Returns
    -------
    ioresult : dict or None
        `{"ret": 1}` if the attempt timed out;
        None if hardlinking failed; otherwise, the dict
        `{"ret": 0, "md5sum": True}`
    """

    from_path = pathlib.Path(from_path)
    dest_path = pathlib.Path(to_dir, filename)

    # Neither POSIX nor libc have any facilities to atomically overwite
    # an existing file with a hardlink, so we have to do this ridiculousness:
    try:
        # Create a temporary directory as a subdirectory of the destination dir
        with TemporaryDirectory(dir=to_dir, prefix=".alpentemp") as tmpdir:
            # We can be certain that we can create anything we want in here
            tmp_path = pathlib.Path(tmpdir, filename)

            # Try to create the new hardlink in the tempdir
            util.timeout_call(tmp_path.hardlink_to, 600, from_path)

            # Hardlink succeeded!  Overwrite any existing file atomically
            tmp_path.rename(dest_path)
    except OSError as e:
        # Link creation failed for some reason
        log.debug(f"hardlink failed: {e}")
        return None
    except TimeoutError:
        log.warning(f'Timeout trying to hardlink "{dest_path}".')
        return {"ret": 1}

    # md5sum is obviously correct
    return {"ret": 0, "md5sum": True}


def local_copy(
    from_path: str | os.PathLike, to_dir: str | os.PathLike, filename: str, size_b: int
) -> dict:
    """Copy `from_path` to `to_dir/filename` using `shutil`

    Atomically overwrites an existing `to_dir/filename`.

    Copy attempt times out after `_pull_timeout(size_b)` seconds have elapsed.

    After a successful copy, `common.util.md5sum_file` will be called to verify
    the transfer was successful.

    Parameters
    ----------
    from_path : path-like
        Source location
    to_dir : path-like
        Destination directory
    filename : str
        Destination filename
    size_b : int
        Size in bytes of file

    Returns
    -------
    ioresult : dict
        Result of the transfer, with keys:
        "ret": int
            0 if copy succeeded; 1 if it failed
        "md5sum": str
            Only present if ret == 0: md5sum of the file computed via
            `util.md5sum_file`
        "stderr": str
            Only present if ret == 1: a message indicating what went wrong.
        "check_src": bool
            True unless it's clear that a failure wasn't
            due to a problem with the source file.
    """

    from_path = pathlib.Path(from_path)
    dest_path = pathlib.Path(to_dir, filename)

    # We create the copy in a temporary place so that the destination filename
    # never points to a partially transferred file.
    try:
        # Create a temporary directory as a subdirectory of the destination dir
        with TemporaryDirectory(dir=to_dir, prefix=".alpentemp") as tmpdir:
            # Timeout for the pull
            timeout = _pull_timeout(size_b)

            # If no timeout, just directly call shutil
            if timeout is None:
                tmp_path = shutil.copy2(from_path, tmpdir)
            else:
                # Otherwise copy the source into the dest, with timeout.
                # Raises OSError or TimeoutError on failure
                tmp_path = util.timeout_call(shutil.copy2, timeout, from_path, tmpdir)

            # Copy succeeded!  Overwrite any existing file atomically
            pathlib.Path(tmp_path).rename(dest_path)

        # Now MD5 the file to verify it.
        log.info(f'verifying "{dest_path}" after local copy')
        md5 = util.md5sum_file(dest_path)
    except (OSError, TimeoutError) as e:
        # Copy failed for some reason
        log.warning(f"local copy failed: {e}")
        return {"ret": 1, "stderr": str(e)}

    # Copy succeeded
    return {"ret": 0, "md5sum": md5}


def pull_async(
    task: Task,
    io: BaseNodeIO,
    tree_lock: UpDownLock,
    req: ArchiveFileCopyRequest,
    did_search: bool,
) -> None:
    """Fulfill `req` by pulling a file onto the local node.

    Things to try:
        - hard link (for nodes on the same filesystem)
        - bbcp (remote transfers only)
        - rsync
        - shutil.copy (local transfers only)

    Parameters
    ----------
    task : Task
        The task instance containing this async.
    io : Node I/O instance
        The I/O instance for the pull destination node.
    tree_lock : UpDownLock
        The directory tree modificiation lock.
    req : ArchiveFileCopyRequest
        The request we're fulfilling.
    did_search : bool
        True if a search for an existing unregistered copy was
        already performed.
    """

    # Before we were queued, NodeIO reserved space for this file.
    # Automatically release bytes on task completion
    task.on_cleanup(io.release_bytes, args=(req.file.size_b,))

    pullrun_metric = Metric(
        "pull_running_count",
        "Count of in-progress pulls",
        unbound={"method", "remote"},
        bound={"node": io.node.name},
    )

    # Rerun the database checks, because we don't know how long we've been in the queue
    if not req.check(node_to=io.node):
        return

    # We know dest is local, so if source is too, this is a local transfer
    local = req.node_from.local

    # The Remote Node
    remote = RemoteNode(req.node_from)

    # Source spec
    if local:
        from_path = remote.io.file_path(req.file)
    else:
        try:
            from_path = remote.io.file_addr(req.file)
        except ValueError:
            log.warning(
                f"Skipping request for {req.file.path} "
                f"due to unconfigured route to host for node {req.node_from.name}."
            )
            return

    to_file = pathlib.Path(io.node.root, req.file.path)
    to_dir = to_file.parent

    # Check for existing file, if not done already
    if not did_search:
        try:
            if io.exists(req.file.path):
                log.warning(
                    "Skipping pull request for "
                    f"{req.file.acq.name}/{req.file.name}: "
                    f"file already on disk on node {io.node.name}."
                )

                force_check_filecopy(req.file, io.node, io)
                # request not resolved.  Should be sorted out after
                # the file check happens.
                return
        except OSError:
            # On error, try to do the pull
            pass

    # Placeholder file
    placeholder = pathlib.Path(to_dir, f".{to_file.name}.placeholder")

    # Create directories.  This must be done while locking up the tree lock
    with tree_lock.up:
        if not to_dir.exists():
            log.info(f'Creating directory "{to_dir}".')
            to_dir.mkdir(parents=True, exist_ok=True)

        # If the file doesn't exist, create a placeholder so we can release
        # the tree lock without having to wait for the transfer to complete
        if not to_file.exists():
            placeholder.touch(mode=0o600, exist_ok=True)

    # Giddy up!
    start_time = time.time()

    # Attempt to transfer the file. Each of the methods below needs to return
    # a dict with required key:
    #  - ret : integer
    #        return code (0 == success)
    # optional keys:
    #  - md5sum : string or True
    #        If True, the sum is guaranteed to be right; otherwise, it's a
    #        md5sum to check against the source.  Must be present if ret == 0
    #  - stderr : string
    #        if given, printed to the log when ret != 0
    #  - check_src : bool
    #        if given and False, the source file will _not_ be marked suspect
    #        when ret != 0; otherwise, a failure results in a source check

    # First we need to check if we are copying over the network
    if not local:
        if shutil.which("bbcp") is not None:
            # First try bbcp which is a fast multistream transfer tool. bbcp can
            # calculate the md5 hash as it goes, so we'll do that to save doing
            # it at the end.
            log.info(f"Pulling remote file {req.file.path} using bbcp")
            pullrun_metric.inc(method="bbcp", remote="1")
            ioresult = bbcp(from_path, to_file, req.file.size_b)
            pullrun_metric.dec(method="bbcp", remote="1")
        elif shutil.which("rsync") is not None:
            # Next try rsync over ssh.
            log.info(f"Pulling remote file {req.file.path} using rsync")
            pullrun_metric.inc(method="rsync", remote="1")
            ioresult = rsync(from_path, to_file, req.file.size_b, local)
            pullrun_metric.dec(method="rsync", remote="1")
        else:
            # We have no idea how to transfer the file...
            log.error("No commands available to complete remote pull.")
            ioresult = {"ret": -1, "check_src": False}

    else:
        # Okay, great we're just doing a local transfer.

        # First try to just hard link the file. This will only work if we
        # are on the same filesystem.  If it didn't work, ioresult will be None
        #
        # But don't do this if it creates a hardlink between an archive node and
        # a non-archive node
        if req.node_from.archive == io.node.archive:
            pullrun_metric.inc(method="link", remote="0")
            ioresult = hardlink(from_path, to_dir, req.file.name)
            pullrun_metric.dec(method="link", remote="0")
            if ioresult is not None:
                log.info(f"Hardlinked local file {req.file.path}")
        else:
            ioresult = None

        # If we couldn't just link the file, try copying it with rsync.
        if ioresult is None:
            if shutil.which("rsync") is not None:
                log.info(f"Pulling local file {req.file.path} using rsync")
                pullrun_metric.inc(method="rsync", remote="0")
                ioresult = rsync(from_path, to_file, req.file.size_b, local)
                pullrun_metric.dec(method="rsync", remote="0")
            else:
                # No rsync?  Just use shutil.copy, I guess
                log.warning("Falling back on shutil.copy to complete local pull.")
                pullrun_metric.inc(method="internal", remote="0")
                ioresult = local_copy(from_path, to_dir, req.file.name, req.file.size_b)
                pullrun_metric.dec(method="internal", remote="0")

    # Delete the placeholder, if we created it
    placeholder.unlink(missing_ok=True)

    if not copy_request_done(
        req,
        io,
        check_src=ioresult.get("check_src", True),
        md5ok=ioresult.get("md5sum", None),
        start_time=start_time,
        stderr=ioresult.get("stderr", None),
        success=(ioresult["ret"] == 0),
    ):
        # Remove file, on error
        try:
            to_file.unlink(missing_ok=True)
        except OSError as e:
            log.error(f"Error removing corrupt file {to_file}: {e}")

    # Whatever has happened, update free space, if possible
    new_avail = io.bytes_avail(fast=True)

    # This was a fast update, so don't save "None" to the database
    if new_avail is not None:
        io.node.update_avail_gb(new_avail)
