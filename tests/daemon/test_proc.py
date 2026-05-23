"""alpenhorn.daemon.proc tests."""

from unittest.mock import MagicMock, patch

import pytest

from alpenhorn.daemon import proc


def test_run_retval0():
    """Test getting success from run_command."""
    retval, _, _ = proc.run_command(["true"])
    assert retval == 0


def test_run_retval1():
    """Test getting failure from run_command."""
    retval, _, _ = proc.run_command(["false"])
    assert retval != 0


def test_run_stdout():
    """Test getting stdout from run_command."""
    retval, stdout, stderr = proc.run_command(["echo", "stdout"])
    assert stderr == ""
    assert stdout == "stdout\n"
    assert retval == 0


def test_run_stderr():
    """Test getting stderr from run_command."""
    retval, stdout, stderr = proc.run_command(
        ["python3", "-c", "import os; os.write(2, b'stderr')"]
    )
    assert stderr == "stderr"
    assert stdout == ""
    assert retval == 0


def test_run_timeout():
    """Test run_command timing out."""
    retval, _, _ = proc.run_command(["sleep", "10"], timeout=0.1)
    assert retval is None


def test_md5sum_file(xfs):
    """Test proc.md5sum_file"""

    xfs.create_file("/test/file", contents="")
    assert proc.md5sum_file("/test/file") == "d41d8cd98f00b204e9800998ecf8427e"

    xfs.create_file(
        "/another/file", contents="The quick brown fox jumps over the lazy dog"
    )
    assert proc.md5sum_file("/another/file") == "9e107d9d372bb6826bd81d3542a419d6"


def test_hashes_running_dec(xfs):
    """Test decrement of hashes_running metric.

    It should decrement even if the hash times out
    (or an I/O error occurs).
    """

    # Mock the Metic for monitoring
    metric_mock = MagicMock()

    def Metric(*args, **kwargs):
        return metric_mock

    # Replacement _md5sum_file function that just throws an error
    async def _md5sum_file(filename):
        raise TimeoutError("Testing")

    with pytest.raises(TimeoutError):
        with patch("alpenhorn.daemon.proc.Metric", Metric):
            with patch("alpenhorn.daemon.proc._md5sum_file", _md5sum_file):
                proc.md5sum_file("/unused/path")

    # Check that the decrement happened.
    metric_mock.dec.assert_called()
