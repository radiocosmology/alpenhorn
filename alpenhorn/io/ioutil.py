"""I/O utility functions."""

from __future__ import annotations

import errno
import logging
import pathlib
import re
import shutil
import time
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import peewee as pw

from .. import db
from ..common import config, metrics, util
from ..common.metrics import Metric
from ..db import (
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageNode,
    StorageTransferAction,
    utcfromtimestamp,
    utcnow,
)
from ..scheduler import threadlocal

if TYPE_CHECKING:
    import os

    from ..acquisition import ArchiveFile
    from .base import BaseNodeIO
    from .updownlock import UpDownLock
del TYPE_CHECKING


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

    base = config.config["daemon"].get("pull_timeout_base", PULL_TIMEOUT_BASE)
    bps = config.config["daemon"].get("pull_bytes_per_second", PULL_BYTES_PER_SECOND)

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

    ret, stdout, stderr = util.run_command(
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


def post_add(node: StorageNode, file_: ArchiveFile) -> None:
    """Run any actions after adding `file` to `node`.

    Possible actions are autosync or autoclean.

    Parameters
    ----------
    node : StorageNode
        The node the file copy was added to.
    file_ : ArchiveFile
        The file added.
    """

    # Autosync: find all StorageTransferActions where we're the source node
    for edge in StorageTransferAction.select().where(
        StorageTransferAction.node_from == node,
        StorageTransferAction.group_to != node.group,
        StorageTransferAction.autosync == True,  # noqa: E712
    ):
        if edge.group_to.state_on_node(file_)[0] != "Y":
            log.debug(
                f"Autosyncing {file_.path} from node {node.name} "
                f"to group {edge.group_to.name}"
            )

            ArchiveFileCopyRequest.create(
                node_from=node, group_to=edge.group_to, file=file_
            )

    # Autoclean: find all the StorageTransferActions where we're in the
    # destination group
    for edge in StorageTransferAction.select().where(
        StorageTransferAction.group_to == node.group,
        StorageTransferAction.node_from != node,
        StorageTransferAction.autoclean == True,  # noqa: E712
    ):
        count = (
            ArchiveFileCopy.update(wants_file="N", last_update=utcnow())
            .where(
                ArchiveFileCopy.file == file_,
                ArchiveFileCopy.node == edge.node_from,
                ArchiveFileCopy.has_file == "Y",
                ArchiveFileCopy.wants_file == "Y",
            )
            .execute()
        )

        if count > 0:
            log.debug(f"Autocleaning {file_.path} from node {edge.node_from.name}")


def copy_request_done(
    req: ArchiveFileCopyRequest,
    io: BaseNodeIO,
    success: bool,
    md5ok: bool | str,
    start_time: float,
    check_src: bool = True,
    stderr: str | None = None,
) -> bool:
    """Update the database after attempting a copy request.

    Parameters
    ----------
    req : ArchiveFileCopyRequest
        The copy request that was attempted
    io : Node I/O instance
        The I/O instance of the destination node
    success : bool
        True unless the file transfer failed.
    md5ok : boolean or str
        Either a boolean indicating if the MD5 sum was correct or
        else a string MD5 sum which we need to verify.  Ignored if
        success is not True.
    start_time : float
        time.time() when the transfer was started
    check_src : boolean
        if success is False, should the source file be marked suspect?
    stderr : str or None
        if success is False, this will be copied into the log

    Returns
    -------
    good_transfer : bool
        True if the parameters indicate the transfer was successful
        or False if the transfer failed.
    """

    # The only label left unbound here is "result"
    transf_metric = metrics.by_name("transfers").bind(
        node_from=req.node_from.name,
        group_to=req.group_to.name,
    )

    # Check the result
    if not success:
        if stderr is None:
            stderr = "Unspecified error."
            transf_metric.inc(result="failure")
        if check_src:
            # If the copy didn't work, then the remote file may be corrupted.
            log.error("Copy failed.  Marking source file suspect.")
            log.info(f"Output: {stderr}")
            ArchiveFileCopy.update(has_file="M", last_update=utcnow()).where(
                ArchiveFileCopy.file == req.file,
                ArchiveFileCopy.node == req.node_from,
            ).execute()
            transf_metric.inc(result="check_src")
        else:
            # An error occurred that can't be due to the source being corrupt
            log.error("Copy failed")
            log.info(f"Output: {stderr}")
            transf_metric.inc(result="failure")
        return False

    # Otherwise, transfer was completed, remember end time
    end_time = time.time()

    # Check integrity.
    if isinstance(md5ok, str):
        md5ok = md5ok == req.file.md5sum
    if not md5ok:
        log.error(
            f"MD5 mismatch on node {io.node.name}; "
            f"Marking source file {req.file.name} on node {req.node_from} suspect."
        )
        ArchiveFileCopy.update(has_file="M", last_update=utcnow()).where(
            ArchiveFileCopy.file == req.file,
            ArchiveFileCopy.node == req.node_from,
        ).execute()
        transf_metric.inc(result="integrity")
        return False

    # Transfer successful
    trans_time = end_time - start_time
    rate = req.file.size_b / trans_time
    log.info(
        f"Pull of {req.file.path} complete. "
        f"Transferred {util.pretty_bytes(req.file.size_b)} "
        f"in {util.pretty_deltat(trans_time)} [{util.pretty_bytes(rate)}/s]"
    )

    with db.database_proxy.atomic():
        # Upsert the FileCopy
        size = io.filesize(req.file.path, actual=True)
        try:
            ArchiveFileCopy.insert(
                file=req.file,
                node=io.node,
                has_file="Y",
                wants_file="Y",
                ready=True,
                size_b=size,
                last_update=utcnow(),
            ).execute()
        except pw.IntegrityError:
            ArchiveFileCopy.update(
                has_file="Y",
                wants_file="Y",
                ready=True,
                size_b=size,
                last_update=utcnow(),
            ).where(
                ArchiveFileCopy.file == req.file, ArchiveFileCopy.node == io.node
            ).execute()

        # Mark AFCR as completed
        ArchiveFileCopyRequest.update(
            completed=True,
            transfer_started=utcfromtimestamp(start_time),
            transfer_completed=utcfromtimestamp(end_time),
        ).where(ArchiveFileCopyRequest.id == req.id).execute()

    # Update metrics
    metrics.by_name("requests_completed").inc(
        type="copy",
        node=req.node_from.name,
        group=req.group_to.name,
        result="success",
    )
    transf_metric.inc(result="success")

    # This can be used to measure throughput
    Metric(
        "pulled_bytes",
        "Count of bytes pulled",
        counter=True,
        bound={"node_from": req.node_from.name, "group_to": req.group_to.name},
    ).add(req.file.size_b)

    # Run post-add actions, if any
    post_add(io.node, req.file)

    return True


def remove_filedir(
    node: StorageNode, dirname: pathlib.Path, tree_lock: UpDownLock
) -> None:
    """Try to delete a file's parent directory(s) from a node

    Will attempt to remove the enitre tree given as `dirname`
    while holding the `tree_lock` down until reaching `node.root`.
    Blocks until the lock can be acquired.

    The attempt to delete starts at `acq.name` and walks upwards until
    it runs out of path elements in `acq.name`.

    As soon as a non-empty directory is encountered, the attempt stops
    without raising an error.

    If `acq.name` is missing, or partially missing, that is not an error
    either, but an attempt to delete the part remaining will still be
    attempted.

    Parameters
    ----------
    node: StorageNode
        The node to delete the acq directory from.
    dirname: pathlib.Path
        The path to delete.  Must be absolute and rooted at `node.root`.
    tree_lock: UpDownLock
        This function will block until it can acquire the down lock and
        all I/O will happen while holding the lock down.

    Raises
    ------
    ValueError
        `dirname` was not a subdirectory of `node.root`
    """
    # Sanity check
    if not dirname.is_relative_to(node.root):
        raise ValueError(f"dirname {dirname} not rooted under {node.root}")

    # try to delete the directories.  This must be done while locking down the tree lock
    with tree_lock.down:
        while str(dirname) != node.root:
            try:
                dirname.rmdir()
                log.info(f"Removed directory {dirname} on {node.name}")
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    # This is fine, but stop trying to rmdir.
                    break
                if e.errno == errno.ENOENT:
                    # Already deleted, which is fine.
                    pass
                else:
                    log.warning(
                        f"Error deleting directory {dirname} on {node.name}: {e}"
                    )
                    # Otherwise, let's try to soldier on

            dirname = dirname.parent
