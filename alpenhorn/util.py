"""Utility functions.
"""


import socket
import logging

from . import config

log = logging.getLogger(__name__)


def run_command(cmd, timeout=None, **kwargs):
    """Run a command.

    Parameters
    ----------
    cmd : array
        A command as a list of strings including all arguments.
    timeout : number or None
        Number of seconds to wait before forceably killing the process,
        or None to wait forever.

    Other keyword args are passed directly on to subprocess.Popen

    Returns
    -------
    Returns a three-element tuple containing:
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
    if timeout is None:
        stdout_val, stderr_val = proc.communicate()
        retval = proc.returncode
    else:
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


def md5sum_file(filename, hr=True, cmd_line=False):
    """Find the md5sum of a given file.

    Output should reproduce that of UNIX md5sum command.

    Parameters
    ----------
    filename: string
        Name of file to checksum.
    hr: boolean, optional
        Should output be a human readable hexstring (default is True).
    cmd_line: boolean, optional
        If True, then simply do an os call to md5sum (default is False).

    See Also
    --------
    http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-python
    """
    if cmd_line:
        ret, stdout, stderr = run_command(["md5sum", filename])
        md5 = stdout.split()[0]
        assert len(md5) == 32
        return md5
    else:
        import hashlib

        block_size = 256 * 128

        md5 = hashlib.md5()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(block_size), b""):
                md5.update(chunk)
        if hr:
            return md5.hexdigest()
        return md5.digest()


def get_hostname():
    """Returns the canonical hostname for the machine we're running on.

    If there is a host name specified in the config, that is returned
    otherwise the local hostname up to the first '.' is returned"""
    if config.config is not None and "hostname" in config.config.get("base", dict()):
        return config.config["base"]["hostname"]

    return socket.gethostname().split(".")[0]
