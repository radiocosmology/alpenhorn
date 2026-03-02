"""Daemon subproccess/subprocedure tools."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import subprocess
from collections.abc import Callable
from typing import Any

from ..common import util
from .metrics import Metric

log = logging.getLogger(__name__)


def run_command(
    cmd: list[str], timeout: float | None = None, **kwargs
) -> tuple[int | None, str, str]:
    """Run a command.

    Parameters
    ----------
    cmd : list of strings
        A command as a list of strings including all arguments.
    timeout : float or None
        Number of seconds to wait before forceably killing the process,
        or None to wait forever.

    Other keyword args are passed directly on to subprocess.Popen

    Returns
    -------
    retval : int or None
        Return code, or None if the process was killed after timing out.
        Integer zero indicates success.
    stdout : string
        Value of stdout.
    stderr : string
        Value of stderr.
    """

    log.debug(f"Running command [timeout={timeout}]: " + " ".join(cmd))

    # run using Popen
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
    )
    try:
        stdout_val, stderr_val = proc.communicate(timeout=timeout)
        retval = proc.returncode
    except subprocess.TimeoutExpired:
        log.warning(f"Process overrun [timeout={timeout}]: " + " ".join(cmd))
        proc.kill()
        return (None, "", "")

    return (
        retval,
        stdout_val.decode(errors="replace"),
        stderr_val.decode(errors="replace"),
    )


def timeout_call(func: Callable, timeout: float, /, *args: Any, **kwargs: Any) -> Any:
    """Call a (non-awaitable) function with a timeout.

    Uses asyncio.to_thread to call a function in a thread that
    will be killed if it runs over time.

    Parameters
    ----------
    func : Callable
        the function to call
    timeout : float
        timeout, in seconds
    args, kwargs:
        passed to `func`

    Returns
    -------
    result:
        The return value of func

    Raises
    ------
    TimeoutError:
        The call exceeded the timeout
    """

    # await-able wrapper
    async def _async_wrapper(
        func: Callable, timeout: float, args: tuple, kwargs: dict
    ) -> Any:
        try:
            async with asyncio.timeout(timeout):
                return await asyncio.to_thread(func, *args, **kwargs)
        except TimeoutError:
            log.error(f"Timeout after {util.pretty_deltat(timeout)} calling {func}.")
            raise

    # If timeout is not positive, don't even try
    if timeout <= 0:
        raise TimeoutError(f'Negative timeout for "{func}" in timeout_call.')

    # Otherwise call via asyncio
    return asyncio.run(_async_wrapper(func, timeout, args, kwargs))


async def _md5sum_file(filename: str | os.PathLike) -> str | None:
    """asyncio implementation of md5sum_file().

    Aborts and returns None if computation is too slow.

    (Specifically: if it takes more than ten minutes to
    read and compute the hash of 32MiB of the file.)
    """

    block_size = 256 * 128  # 32,768 bytes

    # This is here just to reduce the number of times
    # we have to spin up the async.  Has not been tuned.
    blocks_per_chunk = 1024  # ie. chunks are 32MiB

    md5 = hashlib.md5()

    def _md5_chunk(f, md5, block_size, blocks_per_chunk):
        """MD5 a "chunk" of a file.

        This function is run in a asyncio thread."""

        block_count = 0
        for block in iter(lambda: f.read(block_size), b""):
            md5.update(block)
            block_count += 1
            if block_count >= blocks_per_chunk:
                return False

        return True

    with open(filename, "rb") as f:
        eof = False
        while not eof:
            try:
                # Here we're going to timeout if it takes more than 10 minutes to
                # MD5 a "chunk" (i.e. 32 MiB), which should be extremely conservative
                async with asyncio.timeout(600):
                    eof = await asyncio.to_thread(
                        _md5_chunk, f, md5, block_size, blocks_per_chunk
                    )
            except TimeoutError:
                log.warning(f"Timeout trying to MD5 {filename}.")
                return None

    return md5.hexdigest()


def md5sum_file(filename: str | os.PathLike) -> str | None:
    """Find the md5sum of a given file.

    This implementation runs in an asyncio wrapper and will time out
    if a 32MiB portion of the file can't be processed in less than ten
    minutes.

    Parameters
    ----------
    filename: string
        Name of file to checksum.

    Returns
    -------
    md5hash: str
        The hexadecimal MD5 hash of the file, or None if the operation timed out.

    See Also
    --------
    http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-python
    """
    metric = Metric("hash_running_count", "Count of in-progress MD5 hashing")

    metric.inc()
    result = asyncio.run(_md5sum_file(filename))
    metric.dec()

    return result
