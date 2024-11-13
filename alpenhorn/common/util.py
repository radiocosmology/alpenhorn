"""Utility functions."""

from __future__ import annotations

import click
import socket
import asyncio
import hashlib
import logging
import subprocess

from . import config, extensions, logger

log = logging.getLogger(__name__)


def version_option(func):
    """Click --version option"""
    return click.option(
        "--version",
        is_flag=True,
        callback=print_version,
        expose_value=False,
        is_eager=True,
        help="Show version information and exit.",
    )(func)


def print_version(ctx, param, value):
    """Click callback for the --version eager option."""

    from .. import __version__
    import sys

    if not value or ctx.resilient_parsing:
        return

    click.echo(f"alpenhorn {__version__} (Python {sys.version})")
    click.echo(
        """
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
    )
    ctx.exit(0)


def start_alpenhorn(
    cli_conf: str | None, cli: bool, verbosity: int | None = None
) -> None:
    """Initialise alpenhorn

    Parameters
    ----------
    cli_conf : str or None
        The config file given on the command line, if any.
    cli : bool
        Is the alpenhorn cli being initialised?
    verbosity : int, optional
        For the cli, the initial verbosity level.  Ignored for daemons.
    """
    # Initialise logging
    logger.init_logging(cli=cli, verbosity=verbosity)

    # Load the configuration for alpenhorn
    config.load_config(cli_conf, cli=cli)

    # Set up daemon logging based on config
    if not cli:
        logger.configure_logging()

    # Load alpenhorn extensions
    extensions.load_extensions()


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


async def _md5sum_file(filename: str, hr: bool = True) -> str | None:
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
                log.warning("Timeout trying to MD5 {filename}.")
                return None

    if hr:
        return md5.hexdigest()
    return md5.digest()


def md5sum_file(filename: str, hr: bool = True) -> str | None:
    """Find the md5sum of a given file.

    Output should reproduce that of UNIX md5sum command.

    Parameters
    ----------
    filename: string
        Name of file to checksum.
    hr: boolean, optional
        Should output be a human readable hexstring (default is True).

    Returns
    -------
    md5hash:
        The MD5 sum of the file, or None if the operation timed out.

    See Also
    --------
    http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-python
    """
    return asyncio.run(_md5sum_file(filename, hr))


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
