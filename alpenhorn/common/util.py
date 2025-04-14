"""Utility functions."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import socket
import subprocess
from typing import TYPE_CHECKING, Any

import click

from . import config, extensions, logger
from .metrics import Metric

if TYPE_CHECKING:
    from collections.abc import Callable
    from os import PathLike
del TYPE_CHECKING

log = logging.getLogger(__name__)


def help_config_option(func):
    """Click --help-config option"""

    # This is the callback
    def _help_config(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return

        click.echo(
            """
In order to operate, alpenhorn needs to be configured to point it to the
SQL database containing its Data Index.  This is done through one or more
alpenhorn config files.

Alpenhorn searches for config files in the following order:
\b
  * /etc/alpenhorn/alpenhorn.conf
  * /etc/xdg/alpenhorn/alpenhorn.conf
  * ~/.config/alpenhorn/alpenhorn.conf
  * the value of the "ALPENHORN_CONFIG_FILE" environment variable
  * the path passed via "-c" or "--conf" on the command line

If multiple config files from this list are found, all will be read,
with config from later files overriding earlier ones.  The config files
are YAML.

Typically, to configure the database connection, the config should define
a database URL, with a YAML config like this:
\b
database:
    url: mysql://user:passwd@hostname:port/my_database

However, an alternate way to provide database configuration to alpenhorn
is through a database extension module.  If you use such an extension, you
should declare it in the config, instead, so alpenhorn can load it:
\b
extensions:
    - my_extensions.alpenhorn_db_extension

The database connection is the only thing that can be configured for the
alpenhorn CLI.  But further configuration of the daemon is possible.
Consult the alpenhorn documentation for more information.
"""
        )
        ctx.exit(0)

    return click.option(
        "--help-config",
        is_flag=True,
        callback=_help_config,
        expose_value=False,
        is_eager=True,
        help="Help on configuring alpenhorn.",
    )(func)


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

    import sys

    from .. import __version__

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
            log.error(f"Timeout after {pretty_deltat(timeout)} calling {func}.")
            raise

    # If timeout is not positive, don't even try
    if timeout <= 0:
        raise TimeoutError(f'Negative timeout for "{func}" in timeout_call.')

    # Otherwise call via asyncio
    return asyncio.run(_async_wrapper(func, timeout, args, kwargs))


async def _md5sum_file(filename: str | PathLike) -> str | None:
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


def md5sum_file(filename: str | PathLike) -> str | None:
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


def get_hostname() -> str:
    """Returns the hostname for the machine we're running on.

    If there is a host name specified in the config, that is returned
    otherwise the local hostname up to the first '.' is returned"""
    if config.config is not None and "hostname" in config.config.get("base", {}):
        return config.config["base"]["hostname"]

    return socket.gethostname().split(".")[0]


def pretty_bytes(num: int | None) -> str:
    """Return a nicely formatted string describing a size in bytes.

    Parameters
    ----------
    num : int or None
        Number of bytes

    Returns
    -------
    pretty_bytes : str
        If `num` was None, this will be "-".  Otherwise, it's a
        formatted string using power-of-two prefixes, e.g. "103.4 GiB".

    Raises
    ------
    TypeError
        `num` was non-numeric
    ValueError
        `num` was less than zero
    """

    # Return something unhelpful if given None
    if num is None:
        return "-"

    # Reject weird stuff
    try:
        num = int(num)
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
            if num >= 10:
                return f"{num:.2f} {p}iB"
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


def invalid_import_path(name: str) -> str | None:
    """Is `name` invalid as an import path?

    i.e., can `name` be used as an ArchiveAcq or
    ArchiveFile name (or both, combined).

    Returns
    -------
    rejection_reason: str or None
        A string describing why `name` was rejected,
        or None, if the name was valid.
    """

    # Can't be the null string
    if name == "":
        return "empty path"

    # Can't simply be "." or ".."
    if name == "." or name == "..":
        return "invalid path"

    # Can't start with "/" or "./" or "../"
    if name.startswith("/") or name.startswith("./") or name.startswith("../"):
        return "invalid start"

    # Can't end with "/" or "/." or "/.."
    if name.endswith("/") or name.endswith("/.") or name.endswith("/.."):
        return "invalid end"

    # Can't have multiple "/" in a row
    if "//" in name:
        return "repeated /"

    # Can't have internal "/./"
    if "/./" in name:
        return 'invalid path element "."'

    # Can't have internal "/../"
    if "/../" in name:
        return 'invalid path element ".."'

    return None
