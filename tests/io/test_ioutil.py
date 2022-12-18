"""Test alpenhorn.io.ioutil"""

import os
import pytest
from unittest.mock import patch

from alpenhorn.io import ioutil


def test_pretty_bytes():
    """Test ioutil.pretty_bytes."""
    with pytest.raises(ValueError):
        ioutil.pretty_bytes(-1)

    with pytest.raises(TypeError):
        ioutil.pretty_bytes(None)

    with pytest.raises(OverflowError):
        ioutil.pretty_bytes(1234567890123456789012)

    assert ioutil.pretty_bytes(123456789012345678901) == "107.1 EiB"
    assert ioutil.pretty_bytes(12345678901234567890) == "10.7 EiB"
    assert ioutil.pretty_bytes(1234567890123456789) == "1.1 EiB"
    assert ioutil.pretty_bytes(123456789012345678) == "109.7 PiB"
    assert ioutil.pretty_bytes(12345678901234567) == "11.0 PiB"
    assert ioutil.pretty_bytes(1234567890123456) == "1.1 PiB"
    assert ioutil.pretty_bytes(123456789012345) == "112.3 TiB"
    assert ioutil.pretty_bytes(12345678901234) == "11.2 TiB"
    assert ioutil.pretty_bytes(1234567890123) == "1.1 TiB"
    assert ioutil.pretty_bytes(123456789012) == "115.0 GiB"
    assert ioutil.pretty_bytes(12345678901) == "11.5 GiB"
    assert ioutil.pretty_bytes(1234567890) == "1.1 GiB"
    assert ioutil.pretty_bytes(123456789) == "117.7 MiB"
    assert ioutil.pretty_bytes(12345678) == "11.8 MiB"
    assert ioutil.pretty_bytes(1234567) == "1.2 MiB"
    assert ioutil.pretty_bytes(123456) == "120.6 kiB"
    assert ioutil.pretty_bytes(12345) == "12.1 kiB"
    assert ioutil.pretty_bytes(1234) == "1.2 kiB"
    assert ioutil.pretty_bytes(123) == "123 B"
    assert ioutil.pretty_bytes(12) == "12 B"
    assert ioutil.pretty_bytes(1) == "1 B"
    assert ioutil.pretty_bytes(0) == "0 B"


def test_pretty_deltat():
    """Test ioutil.pretty_deltat."""

    with pytest.raises(TypeError):
        ioutil.pretty_deltat(None)

    with pytest.raises(ValueError):
        ioutil.pretty_deltat(-1)

    assert ioutil.pretty_deltat(1234567) == "342h 56m 07s"
    assert ioutil.pretty_deltat(123456) == "34h 17m 36s"
    assert ioutil.pretty_deltat(12345) == "3h 25m 45s"
    assert ioutil.pretty_deltat(1234) == "20m 34s"
    assert ioutil.pretty_deltat(123) == "2m 03s"
    assert ioutil.pretty_deltat(12) == "12.0s"
    assert ioutil.pretty_deltat(1) == "1.0s"
    assert ioutil.pretty_deltat(0.1) == "0.1s"
    assert ioutil.pretty_deltat(0.01) == "0.0s"
    assert ioutil.pretty_deltat(0) == "0.0s"


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
def test_bbcp_good(mock_run_command):
    """Test a successful ioutil.bbcp() call."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert args["timeout"] == 305.0  # 300 seconds base + 5 seconds for 1e8 bytes


@pytest.mark.run_command_result(0, "", "")
def test_bbcp_nomd5(mock_run_command):
    """Test a ioutil.bbcp() call with md5 sum missing from commmand output."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": False,
        "ret": -1,
        "stderr": "Unable to read m5sum from bbcp output",
    }


@pytest.mark.run_command_result(1, "", "bbcp_stderr")
def test_bbcp_fail(mock_run_command):
    """Test a ioutil.bbcp() failed command."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "ret": 1,
        "stderr": "bbcp_stderr",
    }


@pytest.mark.run_command_result(0, "", "")
def test_rsync_good(mock_run_command):
    """Test successful ioutil.rsync() commands."""

    # Local rsync
    assert ioutil.rsync("from/path", "to/dir", 1e8, True) == {
        "md5sum": True,
        "ret": 0,
        "stderr": "",
    }

    # Local rsync doesn't compress
    args = mock_run_command()
    assert "--compress" not in args["cmd"]
    assert args["timeout"] == 305.0

    # Remote rsync
    assert ioutil.rsync("from/path", "to/dir", 1e8, False) == {
        "md5sum": True,
        "ret": 0,
        "stderr": "",
    }

    # Remote rsync does compress
    args = mock_run_command()
    assert "--compress" in args["cmd"]
    assert args["timeout"] == 305.0


@pytest.mark.run_command_result(1, "", "mkstemp")
def test_rsync_mkstemp(mock_run_command):
    """Test catching a mkstemp error in ioutil.rsync()."""

    assert ioutil.rsync("from/path", "to/dir", 1e8, True) == {
        "check_src": False,
        "ret": 1,
        "stderr": "mkstemp",
    }


@pytest.mark.run_command_result(1, "", "write failed on")
def test_rsync_write(mock_run_command):
    """Test catching a write error in ioutil.rsync()."""

    assert ioutil.rsync("from/path", "to/dir", 1e8, True) == {
        "check_src": False,
        "ret": 1,
        "stderr": "write failed on",
    }


@pytest.mark.run_command_result(1, "", "rsync_stderr")
def test_rsync_fail(mock_run_command):
    """Test general failure in ioutil.rsync()."""

    assert ioutil.rsync("from/path", "to/dir", 1e8, True) == {
        "check_src": True,
        "ret": 1,
        "stderr": "rsync_stderr",
    }


def test_hardlink(tmp_path):
    """Test successful ioutil.hardlink() call."""

    # Create src and dest in a temporary directory
    srcdir = tmp_path.joinpath("src")
    srcdir.mkdir()
    file = srcdir.joinpath("file")
    file.write_text("data")
    dstdir = tmp_path.joinpath("dst")
    dstdir.mkdir()
    destfile = dstdir.joinpath("file")

    assert ioutil.hardlink(file, dstdir, "file") == {"ret": 0, "md5sum": True}
    assert destfile.read_text() == "data"


def test_hardlink_clobber(tmp_path):
    """Test successful overwrite in ioutil.hardlink() call."""

    # Create src and dest in a temporary directory
    srcdir = tmp_path.joinpath("src")
    srcdir.mkdir()
    file = srcdir.joinpath("file")
    file.write_text("data")
    dstdir = tmp_path.joinpath("dst")
    dstdir.mkdir()
    destfile = dstdir.joinpath("file")
    destfile.write_text("other_data")

    assert destfile.read_text() == "other_data"
    assert ioutil.hardlink(file, dstdir, "file") == {"ret": 0, "md5sum": True}
    assert destfile.read_text() == "data"


def test_hardlink_fail(tmp_path):
    """Test failed ioutil.hardlink() call."""

    # Create src but not dest
    srcdir = tmp_path.joinpath("src")
    srcdir.mkdir()
    file = srcdir.joinpath("file")
    file.write_text("data")
    dstdir = tmp_path.joinpath("dst")
    destfile = dstdir.joinpath("file")

    assert ioutil.hardlink(file, dstdir, "file") is None
    with pytest.raises(FileNotFoundError):
        destfile.read_text()

    # Try with access error instead
    dstdir.mkdir(mode=0o400)

    assert ioutil.hardlink(file, dstdir, "file") is None
    with pytest.raises(PermissionError):
        destfile.read_text()
