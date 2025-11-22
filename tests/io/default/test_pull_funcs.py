"""Test the low-level Default I/O pull functions."""

import pathlib

import pytest

from alpenhorn.io.default import pull


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
@pytest.mark.alpenhorn_config({"daemon": {"pull_timeout_base": 1000}})
def test_bbcp_config_timeout(mock_run_command, set_config):
    """Test setting timeout via config."""

    assert pull.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert args["timeout"] == 1005.0  # 1000 seconds base + 5 seconds for 1e8 bytes


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
@pytest.mark.alpenhorn_config({"daemon": {"pull_bytes_per_second": 0}})
def test_bbcp_no_timeout(mock_run_command, set_config):
    """Test disabling timeout via config."""

    assert pull.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert args["timeout"] is None  # pull_bytes_per_second = 0 disables


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
def test_bbcp_pathlib(mock_run_command):
    """Test passing pathlib.Path to pull.bbcp()."""

    assert pull.bbcp(pathlib.Path("from/path"), pathlib.Path("to/dir"), 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert "from/path" in args["cmd"]
    assert "to/dir" in args["cmd"]


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
def test_bbcp_good(mock_run_command):
    """Test a successful pull.bbcp() call."""

    assert pull.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert args["timeout"] == 305.0  # 300 seconds base + 5 seconds for 1e8 bytes


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
def test_bbcp_port(mock_run_command):
    """Test setting bbcp from worker threads."""

    from alpenhorn.daemon.scheduler import threadlocal

    # Ensure we have no worker id
    try:
        del threadlocal.worker_id
    except AttributeError:
        pass

    pull.bbcp("from/path", "to/dir", 1e8)
    args = mock_run_command()
    assert "4200" in args["cmd"]

    # Set worker id
    threadlocal.worker_id = 1

    pull.bbcp("from/path", "to/dir", 1e8)
    args = mock_run_command()
    assert "4200" in args["cmd"]

    # Set worker id
    threadlocal.worker_id = 2

    pull.bbcp("from/path", "to/dir", 1e8)
    args = mock_run_command()
    assert "4210" in args["cmd"]


@pytest.mark.run_command_result(0, "", "")
def test_bbcp_nomd5(mock_run_command):
    """Test a pull.bbcp() call with md5 sum missing from commmand output."""

    assert pull.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": False,
        "ret": -1,
        "stderr": "Unable to read m5sum from bbcp output",
    }


@pytest.mark.run_command_result(1, "", "bbcp_stderr")
def test_bbcp_fail(mock_run_command):
    """Test a pull.bbcp() failed command."""

    assert pull.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "ret": 1,
        "stderr": "bbcp_stderr",
    }


@pytest.mark.run_command_result(0, "", "")
def test_rsync_good(mock_run_command):
    """Test successful pull.rsync() commands."""

    # Local rsync
    assert pull.rsync("from/path", "to/dir", 1e8, True) == {
        "md5sum": True,
        "ret": 0,
        "stderr": "",
    }

    # Local rsync doesn't compress
    args = mock_run_command()
    assert "--compress" not in args["cmd"]
    assert args["timeout"] == 305.0

    # Remote rsync
    assert pull.rsync("from/path", "to/dir", 1e8, False) == {
        "md5sum": True,
        "ret": 0,
        "stderr": "",
    }

    # Remote rsync does compress
    args = mock_run_command()
    assert "--compress" in args["cmd"]
    assert args["timeout"] == 305.0


@pytest.mark.run_command_result(0, "", "")
def test_rsync_pathlib(mock_run_command):
    """Test passing pathlib.Path to pull.rsync()."""

    # Local rsync
    assert pull.rsync(pathlib.Path("from/path"), pathlib.Path("to/dir"), 1e8, True) == {
        "md5sum": True,
        "ret": 0,
        "stderr": "",
    }

    # Paths should be stringified
    args = mock_run_command()
    assert "from/path" in args["cmd"]
    assert "to/dir" in args["cmd"]


@pytest.mark.run_command_result(1, "", "mkstemp")
def test_rsync_mkstemp(mock_run_command):
    """Test catching a mkstemp error in pull.rsync()."""

    assert pull.rsync("from/path", "to/dir", 1e8, True) == {
        "check_src": False,
        "ret": 1,
        "stderr": "mkstemp",
    }


@pytest.mark.run_command_result(1, "", "write failed on")
def test_rsync_write(mock_run_command):
    """Test catching a write error in pull.rsync()."""

    assert pull.rsync("from/path", "to/dir", 1e8, True) == {
        "check_src": False,
        "ret": 1,
        "stderr": "write failed on",
    }


@pytest.mark.run_command_result(1, "", "rsync_stderr")
def test_rsync_fail(mock_run_command):
    """Test general failure in pull.rsync()."""

    assert pull.rsync("from/path", "to/dir", 1e8, True) == {
        "check_src": True,
        "ret": 1,
        "stderr": "rsync_stderr",
    }


def test_hardlink(xfs):
    """Test successful pull.hardlink() call."""

    # Create src and dest
    file = "/src/file"
    xfs.create_file(file, contents="data")
    destfile = pathlib.Path("/dest/file")
    xfs.create_dir(destfile.parent)

    assert pull.hardlink(file, destfile) == {"ret": 0, "md5sum": True}
    assert destfile.read_text() == "data"


def test_hardlink_clobber(xfs):
    """Test successful overwrite in pull.hardlink() call."""

    # Create src and dest
    file = "/src/file"
    xfs.create_file(file, contents="data")
    destfile = pathlib.Path("/dest/file")
    xfs.create_file(destfile, contents="other_data")

    assert destfile.read_text() == "other_data"
    assert pull.hardlink(file, destfile) == {"ret": 0, "md5sum": True}
    assert destfile.read_text() == "data"


def test_hardlink_fail(xfs):
    """Test failed pull.hardlink() call."""

    # Create src but not destdir
    file = "/src/file"
    xfs.create_file(file, contents="data")
    destfile = pathlib.Path("/dest/file")

    assert pull.hardlink(file, destfile) is None
    with pytest.raises(FileNotFoundError):
        destfile.read_text()

    # Try with access error instead.
    xfs.create_file(destfile, contents="other_data")
    xfs.chmod(destfile.parent, 0o400)

    assert pull.hardlink(file, destfile) is None
    with pytest.raises(PermissionError):
        destfile.read_text()


def test_local_copy(xfs, set_config):
    """Test successful pull.local_copy() call."""

    # Create src and dest
    file = "/src/file"
    xfs.create_file(file, contents="data")
    destfile = pathlib.Path("/dest/file")
    xfs.create_dir(destfile.parent)

    assert pull.local_copy(file, destfile, 4) == {
        "ret": 0,
        "md5sum": "8d777f385d3dfec8815d20f7496026dc",
    }
    assert destfile.read_text() == "data"


def test_local_copy_clobber(xfs, set_config):
    """Test successful overwrite in pull.local_copy() call."""

    # Create src and dest
    file = "/src/file"
    xfs.create_file(file, contents="data")
    destfile = pathlib.Path("/dest/file")
    xfs.create_file(destfile, contents="other_data")

    assert destfile.read_text() == "other_data"
    assert pull.local_copy(file, destfile, 4) == {
        "ret": 0,
        "md5sum": "8d777f385d3dfec8815d20f7496026dc",
    }
    assert destfile.read_text() == "data"


def test_local_copy_fail(xfs, set_config):
    """Test failed pull.local_copy() call."""

    # Create src but not destdir
    file = "/src/file"
    xfs.create_file(file, contents="data")
    destfile = pathlib.Path("/dest/file")

    result = pull.local_copy(file, destfile, 4)
    assert result["ret"] != 0
    assert "stderr" in result

    with pytest.raises(FileNotFoundError):
        destfile.read_text()

    # Try with access error instead.
    xfs.create_file(destfile, contents="other_data")
    xfs.chmod(destfile.parent, 0o400)

    result = pull.local_copy(file, destfile, 4)
    assert result["ret"] != 0
    assert "stderr" in result

    with pytest.raises(PermissionError):
        destfile.read_text()
