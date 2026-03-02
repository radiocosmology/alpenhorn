"""alpenhorn.daemon.proc tests."""

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


def test_md5sum_file(tmp_path):
    """Test proc.md5sum_file"""

    file = tmp_path.joinpath("tmp")
    file.write_text("")
    assert proc.md5sum_file(file) == "d41d8cd98f00b204e9800998ecf8427e"

    file.write_text("The quick brown fox jumps over the lazy dog")
    assert proc.md5sum_file(file) == "9e107d9d372bb6826bd81d3542a419d6"
