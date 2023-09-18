"""Utility functions."""

from __future__ import annotations

import socket
import hashlib
import logging

from . import config

log = logging.getLogger(__name__)


def run_command(
    cmd: list[str], timeout: float | None = None, **kwargs
) -> tuple(int | None, str, str):
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
    stdout : string
        Value of stdout.
    stderr : string
        Value of stderr.
    """

    import subprocess

    log.debug(f"Running command [timeout={timeout}]: " + " ".join(cmd))

    # run using Popen
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
    )
    try:
        stdout_val, stderr_val = proc.communicate(timeout=timeout)
        retval = proc.returncode
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout_val, stderr_val = proc.communicate()
        retval = None

    return (
        retval,
        stdout_val.decode(errors="replace"),
        stderr_val.decode(errors="replace"),
    )


def md5sum_file(filename: str, hr: bool = True) -> str:
    """Find the md5sum of a given file.

    Output should reproduce that of UNIX md5sum command.

    Parameters
    ----------
    filename: string
        Name of file to checksum.
    hr: boolean, optional
        Should output be a human readable hexstring (default is True).

    See Also
    --------
    http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-python
    """
    block_size = 256 * 128

    md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            md5.update(chunk)
    if hr:
        return md5.hexdigest()
    return md5.digest()


def get_hostname() -> str:
    """Returns the hostname for the machine we're running on.

    If there is a host name specified in the config, that is returned
    otherwise the local hostname up to the first '.' is returned"""
    if config.config is not None and "hostname" in config.config.get("base", dict()):
        return config.config["base"]["hostname"]

    return socket.gethostname().split(".")[0]


def pretty_bytes(num: int) -> str:
    """Return a nicely formatted string describing a size in bytes.

    Parameters
    ----------
    num : int
        Number of bytes

    Returns
    -------
    pretty_bytes : str
        A formatted string using power-of-two prefixes,
        e.g. "103.4 GiB"

    Raises
    ------
    TypeError
        `num` was non-numeric
    ValueError
        `num` was less than zero
    """

    # Reject weird stuff
    try:
        if num < 0:
            raise ValueError("negative size")
    except TypeError:
        raise TypeError("non-numeric size")

    if num < 2**10:
        return f"{num} B"

    for x, p in enumerate("kMGTPE"):
        if num < 2 ** ((2 + x) * 10):
            num /= 2 ** ((1 + x) * 10)
            if num >= 100:
                return f"{num:.1f} {p}iB"
            elif num >= 10:
                return f"{num:.2f} {p}iB"
            else:
                return f"{num:.3f} {p}iB"

    # overflow or something: in this case lets just go
    # with what we were given and get on with our day.
    return f"{num} B"


def pretty_deltat(seconds: float) -> str:
    """Return a nicely formatted time delta.

    Parameters
    ----------
    seconds : float
        The time delta, in seconds

    Returns
    -------
    pretty_deltat : str
        A human-readable indication of the time delta.

    Raises
    ------
    TypeError
        `seconds` was non-numeric
    ValueError
        `seconds` was less than zero
    """

    # Reject weird stuff
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        raise TypeError("non-numeric time delta")

    if seconds < 0:
        # If the delta is negative, just print it
        return f"{seconds:.1f}s"

    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    if hours > 0:
        return f"{int(hours)}h{int(minutes):02}m{int(seconds):02}s"
    if minutes > 0:
        return f"{int(minutes)}m{int(seconds):02}s"

    # For short durations, include tenths of a second
    return f"{seconds:.1f}s"
