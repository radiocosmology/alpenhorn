"""I/O utilities functions (mostly inner loops)"""

import os
import re
import time
import peewee as pw
from datetime import datetime
from tempfile import TemporaryDirectory

from ..archive import ArchiveFileCopy, ArchiveFileCopyRequest
from .. import util
from .. import db

import logging

log = logging.getLogger(__name__)


# TODO put these constants in the ioconfig or something

# The timeout used for a bbcp or rsync call is
#
#  PULL_TIMEOUT_BASE + file.size_b / PULL_BYTES_PER_SECOND
#
# For large files, transfers rates from site to cedar via
# BBCP are usually >=50MB/s
#
# Sample values with the numbers here:
#
#  File              Time @ 50MB/s     Timeout
#
#  100GB chime_hfb       33m           1h 28m
#   30GB chimestack      10m              30m
#    2GB chimegain       40s               6m
#  230MB chimetiming      5s               5m
#
PULL_TIMEOUT_BASE = 300  # 5 minutes base
PULL_BYTES_PER_SECOND = 20000000  # 20MB/s


def pretty_bytes(num):
    """Return a nicely formatted string describing a size in bytes."""

    # Reject weird stuff
    try:
        if num < 0:
            raise ValueError("negative size")
    except TypeError:
        raise TypeError("non-numeric size")

    if num < 2**10:
        return f"{num} B"

    num = float(num)
    for x, p in enumerate("kMGTPE"):
        if num < 2 ** ((2 + x) * 10):
            num /= 2 ** ((1 + x) * 10)
            return f"{num:.1f} {p}iB"

    # overflow
    raise OverflowError("a suffusion of yellow")


def pretty_deltat(seconds):
    """Return a nicely formatted time delta."""

    # Reject weird stuff
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        raise TypeError("non-numeric time delta")

    if seconds < 0:
        raise ValueError("negative time delta")

    hours, minutes = divmod(seconds, 3600)
    minutes, seconds = divmod(minutes, 60)

    if hours > 0:
        return f"{int(hours)}h {int(minutes):02}m {int(seconds):02}s"
    if minutes > 0:
        return f"{int(minutes)}m {int(seconds):02}s"
    return f"{seconds:.1f}s"


def bbcp(from_path, to_dir, size_b):
    """Transfer a file with BBCP.

    Command times out after:

        PULL_TIMEOUT_BASE + size_b / PULL_BYTES_PER_SECOND

    seconds have elapsed.
    """
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
            "4200",
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
            from_path,
            to_dir,
        ],
        timeout=PULL_TIMEOUT_BASE + size_b / PULL_BYTES_PER_SECOND,
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
        else:
            ioresult["md5sum"] = mo.group(1)

    return ioresult


def rsync(from_path, to_dir, size_b, local):
    """Rsync a file (either local or remote).

    Command times out after:

        PULL_TIMEOUT_BASE + size_b / PULL_BYTES_PER_SECOND

    seconds have elapsed.
    """

    if local:
        remote_args = list()
    else:
        remote_args = [
            "--compress",
            "--rsync-path=ionice -c2 -n4 rsync",
            "--rsh=ssh -q",
        ]

    ret, stdout, stderr = util.run_command(
        ["rsync"]
        + remote_args
        + [
            "--quiet",
            "--times",
            "--protect-args",
            "--perms",
            "--group",
            "--owner",
            "--copy-links",
            "--sparse",
            from_path,
            to_dir,
        ],
        timeout=PULL_TIMEOUT_BASE + size_b / PULL_BYTES_PER_SECOND,
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


def hardlink(from_path, to_dir, filename):
    """hard link from_path as to_dir/filename

    Atomically overwrites an existing to_dir/filename.

    If hardlinking fails, this funciton assumes it's because src and dest
    aren't on the same filesystem and return None to indicate that.
    """

    dest_path = os.path.join(to_dir, filename)

    # Neither POSIX nor libc have any facilities to atomically overwite
    # an existing file with a hardlink, so we have to do this ridiculousness:
    try:
        # Create a temporary directory as a subdirectory of the destination dir
        with TemporaryDirectory(dir=to_dir) as tmpdir:
            # We can be certain that we can create anything we want in here
            tmp_path = os.path.join(tmpdir, filename)
            # Try to create the new hardlink in the tempdir
            os.link(from_path, tmp_path)
            # Hardlink succeeded!  Overwrite any existing file atomically
            os.rename(tmp_path, dest_path)
    except OSError as e:
        # Link creation failed for some reason
        log.debug(f"hardlink failed: {e}")
        return None

    # md5sum is obviously correct
    return {"ret": 0, "md5sum": True}


def copy_request_done(
    req,
    node,
    success,
    md5ok,
    start_time,
    check_src=True,
    stderr=None,
):
    """Update the database after attempting a copy request.

    Parameters
    ----------
    req : ArchiveFileCopyRequest
        The copy request that was attempted
    node : StorageNode
        The destination node
    success : boolean
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

    Returns True if the caller should keep the file or False if the
    file should be deleted."""

    # Check the result
    if not success:
        if stderr is None:
            stderr = "Unspecified error."
        if check_src:
            # If the copy didn't work, then the remote file may be corrupted.
            log.error(f"Copy failed: {stderr};  Marking source file suspect.")
            ArchiveFileCopy.update(has_file="M").where(
                ArchiveFileCopy.file == req.file,
                ArchiveFileCopy.node == req.node_from,
            ).execute()
        else:
            # An error occurred that can't be due to the source
            # being corrupt
            log.error(f"Copy failed: {stderr}")
        return False

    # Otherwise, transfer was completed, remember end time
    end_time = time.time()

    # Check integrity.
    if isinstance(md5ok, str):
        md5ok = md5ok == req.file.md5sum
    if not md5ok:
        log.error(
            f"MD5 mismatch on node {node.name}; "
            f"Marking source file {req.file.name} on node {req.node_from} suspect."
        )
        ArchiveFileCopy.update(has_file="M").where(
            ArchiveFileCopy.file == req.file,
            ArchiveFileCopy.node == req.node_from,
        ).execute()
        return False

    # Transfer successful
    trans_time = end_time - start_time
    rate = req.file.size_b / trans_time
    log.info(
        f"Pull complete (md5sum correct). Transferred {pretty_bytes(req.file.size_b)} "
        f"in {pretty_deltat(trans_time)} [{pretty_bytes(rate)}/s]"
    )

    with db.database_proxy.atomic():
        # Upsert the FileCopy
        size = node.io.filesize(req.file.path, actual=True)
        try:
            ArchiveFileCopy.insert(
                file=req.file,
                node=node,
                has_file="Y",
                wants_file="Y",
                ready=True,
                size_b=size,
            ).execute()
        except pw.IntegrityError:
            ArchiveFileCopy.update(
                has_file="Y", wants_file="Y", ready=True, size_b=size
            ).where(
                ArchiveFileCopy.file == req.file, ArchiveFileCopy.node == node
            ).execute()

        # Mark AFCR as completed
        ArchiveFileCopyRequest.update(
            completed=True,
            transfer_started=datetime.fromtimestamp(start_time),
            transfer_completed=datetime.fromtimestamp(end_time),
        ).where(ArchiveFileCopyRequest.id == req.id).execute()

    return True
