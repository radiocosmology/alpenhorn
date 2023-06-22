"""Test alpenhorn.io.ioutil"""

import pytest

from alpenhorn.io import ioutil


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
@pytest.mark.alpenhorn_config({"service": {"pull_timeout_base": 1000}})
def test_bbcp_config_timeout(mock_run_command, set_config):
    """Test setting timeout via config."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert args["timeout"] == 1005.0  # 1000 seconds base + 5 seconds for 1e8 bytes


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
@pytest.mark.alpenhorn_config({"service": {"pull_bytes_per_second": 0}})
def test_bbcp_no_timeout(mock_run_command, set_config):
    """Test disabling timeout via config."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert args["timeout"] is None  # pull_bytes_per_second = 0 disables


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
