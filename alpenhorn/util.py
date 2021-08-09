"""Utility functions.
"""


import logging
import os.path
import re
import shlex
import socket

log = logging.getLogger(__name__)


def run_command(cmd, **kwargs):
    """Run a command.

    Parameters
    ----------
    cmd : string
        A command as a string including all arguments. These must be quoted as
        if called from a shell.
    kwargs : dict
        Passed directly onto `subprocess.Popen.`

    Returns
    -------
    retval : int
        Return code.
    stdout_val : string
        Value of stdout.
    stderr_val : string
        Value of stderr.
    """

    import subprocess

    log.debug('Running command "%s"', cmd)

    # Split the cmd string appropriately and then run using Popen
    proc = subprocess.Popen(
        shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
    )
    stdout_val, stderr_val = proc.communicate()
    retval = proc.returncode

    return retval, stdout_val.decode(errors="replace"), stderr_val.decode(errors="replace")


def is_md5_hash(h):
    """Is this the correct format to be an md5 hash."""
    return re.match("[a-f0-9]{32}", h) is not None


def command_available(cmd):
    """Is this command available on the system."""
    from distutils import spawn

    return spawn.find_executable(cmd) is not None


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
    http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-
    python
    """
    if cmd_line:
        ret, stdout, stderr = run_command("md5sum %s" % filename)
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


def get_short_hostname():
    """Returns the short hostname (up to the first '.')"""
    return socket.gethostname().split(".")[0]


def alpenhorn_node_check(node):
    """Check for valid ALPENHORN_NODE file contents

    Return
    ------

    True if ALPENHORN_NODE is present in `node.root` directory and contains the
    contains node name as its first line, False otherwise.

    .. Note:: The caller needs to ensure the StorageNode has the appropriate
    `active` status.
    """

    file_path = os.path.join(node.root, "ALPENHORN_NODE")
    try:
        with open(file_path, "r") as f:
            first_line = f.readline()
            # Check if the actual node name is in the textfile
            if node.name == first_line.rstrip():
                # Great! Everything is as expected.
                return True
            log.debug(
                f"Node name in file {file_path} does not match expected {node.name}."
            )
    except IOError:
        log.debug(f"Node file {file_path} could not be read.")

    return False
