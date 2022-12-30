"""alpenhorn.util tests."""

import pytest
from alpenhorn import util, config


def test_run_retval0():
    """Test getting success from run_command."""
    retval, stdout, stderr = util.run_command(["true"])
    assert retval == 0


def test_run_retval0():
    """Test getting success from run_command."""
    retval, stdout, stderr = util.run_command(["false"])
    assert retval != 0


def test_run_stdout():
    """Test getting stdout from run_command."""
    retval, stdout, stderr = util.run_command(["echo", "stdout"])
    assert stderr == ""
    assert stdout == "stdout\n"
    assert retval == 0


def test_run_stderr():
    """Test getting stdout from run_command."""
    retval, stdout, stderr = util.run_command(
        ["python3", "-c", "import os; os.write(2, b'stderr')"]
    )
    assert stderr == "stderr"
    assert stdout == ""
    assert retval == 0


def test_md5sum_file(tmp_path):
    """Test util.md5sum_file"""

    file = tmp_path.joinpath("tmp")
    file.write_text("")
    assert util.md5sum_file(file) == "d41d8cd98f00b204e9800998ecf8427e"

    file.write_text("The quick brown fox jumps over the lazy dog")
    assert util.md5sum_file(file) == "9e107d9d372bb6826bd81d3542a419d6"


def test_gethostname_config(hostname):
    """Test util.get_hostname with config"""

    assert util.get_hostname() == hostname


def test_gethostname_default():
    """Test util.get_hostname with no config"""
    host = util.get_hostname()
    assert "." not in host
    assert len(host) > 0
