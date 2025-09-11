"""Test alpenhorn.io._lfs LFS wrapper."""

import pathlib

import pytest

from alpenhorn.io._lfs import LFS


@pytest.fixture
def lfs(have_lfs):
    """Returns a minimally-configured LFS instance."""

    return LFS("quota_id", "group")


@pytest.mark.run_command_result(0, "lfs_out", "lfs_err")
def test_run_lfs_success(lfs, mock_run_command):
    """Test successful invocation of lfs.run_lfs."""

    assert lfs.run_lfs("arg1", "arg2") == {
        "failed": False,
        "missing": False,
        "output": "lfs_out",
        "timeout": False,
    }
    assert mock_run_command() == {
        "cmd": ["LFS", "arg1", "arg2"],
        "kwargs": {},
        "timeout": 60,
    }


@pytest.mark.run_command_result(0, "lfs_out", "lfs_err")
def test_run_lfs_stringify(lfs, mock_run_command):
    """run_lfs should stringify arguments."""

    assert lfs.run_lfs(pathlib.Path("path"), 2)["output"] == "lfs_out"
    assert mock_run_command() == {
        "cmd": ["LFS", "path", "2"],
        "kwargs": {},
        "timeout": 60,
    }


@pytest.mark.run_command_result(1, "lfs_out", "lfs_err")
def test_run_lfs_fail(lfs, mock_run_command):
    """Test failed invocation of lfs.run_lfs."""

    assert lfs.run_lfs("arg1", "arg2") == {
        "failed": True,
        "missing": False,
        "output": None,
        "timeout": False,
    }
    assert mock_run_command() == {
        "cmd": ["LFS", "arg1", "arg2"],
        "kwargs": {},
        "timeout": 60,
    }


@pytest.mark.run_command_result(None, "lfs_out", "lfs_err")
def test_run_lfs_timeout(lfs, mock_run_command):
    """Test timed out invocation of lfs.run_lfs."""

    assert lfs.run_lfs("arg1", "arg2") == {
        "failed": False,
        "missing": False,
        "output": None,
        "timeout": True,
    }
    assert mock_run_command() == {
        "cmd": ["LFS", "arg1", "arg2"],
        "kwargs": {},
        "timeout": 60,
    }


@pytest.mark.run_command_result(
    2, "lfs_out", "Something didn't work: No such file or directory"
)
def test_run_lfs_missing(lfs, mock_run_command):
    """Test ENOENT from lfs.run_fls."""

    assert lfs.run_lfs("arg1", "arg2") == {
        "failed": False,
        "missing": True,
        "output": None,
        "timeout": False,
    }
    assert mock_run_command() == {
        "cmd": ["LFS", "arg1", "arg2"],
        "kwargs": {},
        "timeout": 60,
    }


@pytest.mark.run_command_result(0, "/path", None)
def test_quota_auto_syntax_short(lfs, mock_run_command):
    """Test quota syntax error: too few lines."""

    assert lfs.quota_remaining("/path") is None


@pytest.mark.run_command_result(
    0,
    "/path           1234 0 0 - 100 0 0 -\n"
    "gid 345678 is using default block quota setting\n"
    "gid 345678 is using default file quota setting",
    None,
)
def test_quota_auto_with_default(have_lfs, mock_run_command):
    """Test getting quota with default settings."""

    lfs = LFS("quota_id", "group", fixed_quota=2500)

    assert lfs.quota_remaining("/path") == (2500 - 1234) * 2**10


@pytest.mark.run_command_result(
    0,
    "/path           1234 0 0 - 100 0 0 -\n"
    "gid 345678 is using default block quota setting\n"
    "gid 345678 is using default file quota setting",
    None,
)
def test_quota_auto_default_no_fixed(lfs, mock_run_command):
    """Test getting quota with default settings without a fixed quota."""

    assert lfs.quota_remaining("/path") is None


@pytest.mark.run_command_result(0, "/path           1234", None)
def test_quota_auto_syntax_quota(lfs, mock_run_command):
    """Test quota syntax error: bad second line"""

    assert lfs.quota_remaining("/path") is None


@pytest.mark.run_command_result(
    0, "/path           1234* 1000 3000 - 100 200 300 -", None
)
def test_quota_over(lfs, mock_run_command):
    """Test getting quota from lfs.quota_remaining() when over quota."""

    assert lfs.quota_remaining("/path") == (1000 - 1234) * 2**10
    assert "quota_id" in mock_run_command()["cmd"]


@pytest.mark.run_command_result(
    0, "/path           1234 2000 3000 - 100 200 300 -", None
)
def test_quota_auto(lfs, mock_run_command):
    """Test getting quota from lfs.quota_remaining() with no fixed_quota."""

    assert lfs.quota_remaining("/path") == (2000 - 1234) * 2**10
    assert "quota_id" in mock_run_command()["cmd"]


@pytest.mark.run_command_result(
    0, "/path-so-long-it-wraps\n                1234 2000 3000 - 100 200 300 -", None
)
def test_quota_longpath(lfs, mock_run_command):
    """Test getting quota from lfs.quota_remaining() with a long path."""

    assert lfs.quota_remaining("/path-so-long-it-wraps") == (2000 - 1234) * 2**10
    assert "quota_id" in mock_run_command()["cmd"]


@pytest.mark.run_command_result(
    0, "/path           1234 2000 3000 - 100 200 300 -", None
)
def test_quota_fixed(have_lfs, mock_run_command):
    """Test getting quota_remaining from lfs.quota() with fixed quota."""

    lfs = LFS("quota_id", "group", fixed_quota=2500)

    assert lfs.quota_remaining("/path") == (2500 - 1234) * 2**10


def test_user_quota_type(have_lfs, mock_run_command):
    """Test lfs with quota_type == "user"."""

    lfs = LFS("quota_id", "user")

    # None becuase of bad data from run_command
    assert lfs.quota_remaining("/path") is None

    assert mock_run_command()["cmd"] == [
        "LFS",
        "quota",
        "-q",
        "-u",
        "quota_id",
        "/path",
    ]


def test_group_quota_type(have_lfs, mock_run_command):
    """Test lfs with quota_type == "group"."""

    lfs = LFS("quota_id", "group")

    # None becuase of bad data from run_command
    assert lfs.quota_remaining("/path") is None

    assert mock_run_command()["cmd"] == [
        "LFS",
        "quota",
        "-q",
        "-g",
        "quota_id",
        "/path",
    ]


def test_project_quota_type(have_lfs, mock_run_command):
    """Test lfs with quota_type == "project"."""

    lfs = LFS("quota_id", "project")

    # None becuase of bad data from run_command
    assert lfs.quota_remaining("/path") is None

    assert mock_run_command()["cmd"] == [
        "LFS",
        "quota",
        "-q",
        "-p",
        "quota_id",
        "/path",
    ]


def test_bad_quota_type(have_lfs, mock_run_command):
    """Test lfs with invalid quota_type."""

    with pytest.raises(ValueError):
        LFS("quota_id", "pool")


@pytest.mark.run_command_result(
    2, "", "Something didn't work: No such file or directory"
)
def test_hsm_state_missing(xfs, lfs, mock_run_command):
    """Test hsm_state on a missing file."""

    assert lfs.hsm_state("/path") == lfs.HSM_MISSING


@pytest.mark.run_command_result(0, "/poth: (0x00000000)", None)
def test_hsm_state_syntax(xfs, lfs, mock_run_command):
    """Test syntax error in hsm_state output."""

    xfs.create_file("/path")

    assert lfs.hsm_state("/path") is None


@pytest.mark.run_command_result(0, "/path: (0x00000000)", None)
def test_hsm_state_unarchived(xfs, lfs, mock_run_command):
    """Test hsm_state on an unarchived file."""

    xfs.create_file("/path")

    assert lfs.hsm_state("/path") == lfs.HSM_UNARCHIVED


@pytest.mark.run_command_result(
    0, "/path: (0x00000009) exists archived, archive_id:2", None
)
def test_hsm_state_restored(xfs, lfs, mock_run_command):
    """Test hsm_state on a restored file."""

    xfs.create_file("/path")

    assert lfs.hsm_state("/path") == lfs.HSM_RESTORED


@pytest.mark.run_command_result(
    0, "/path: (0x0000000d) released exists archived, archive_id:2", None
)
def test_hsm_state_released(xfs, lfs, mock_run_command):
    """Test hsm_state on a released file."""

    xfs.create_file("/path")

    assert lfs.hsm_state("/path") == lfs.HSM_RELEASED


@pytest.mark.lfs_hsm_state(
    {
        "/missing": "missing",
        "/unarchived": "unarchived",
        "/restored": "restored",
        "/released": "released",
    }
)
def test_hsm_archived(mock_lfs):
    """Test hsm_archived()."""

    lfs = LFS("quota_id", "group")

    assert not lfs.hsm_archived("/missing")
    assert not lfs.hsm_archived("/unarchived")
    assert lfs.hsm_archived("/restored")
    assert lfs.hsm_archived("/released")
    assert not lfs.hsm_released("/other")


@pytest.mark.lfs_hsm_state(
    {
        "/missing": "missing",
        "/unarchived": "unarchived",
        "/restored": "restored",
        "/released": "released",
    }
)
def test_hsm_released(mock_lfs):
    """Test hsm_released()."""

    lfs = LFS("quota_id", "group")

    assert not lfs.hsm_released("/missing")
    assert not lfs.hsm_released("/unarchived")
    assert not lfs.hsm_released("/restored")
    assert lfs.hsm_released("/released")
    assert not lfs.hsm_released("/other")


@pytest.mark.lfs_hsm_state(
    {
        "/missing": "missing",
        "/unarchived": "unarchived",
        "/restored": "restored",
        "/released": "released",
    }
)
@pytest.mark.run_command_result(0, "", None)
@pytest.mark.lfs_dont_mock("hsm_restore")
def test_hsm_restore(mock_lfs, mock_run_command):
    """Test hsm_restore()."""

    lfs = LFS("quota_id", "group")

    assert lfs.hsm_restore("/missing") is False
    assert lfs.hsm_restore("/unarchived")
    assert lfs.hsm_restore("/restored")

    # None of these should have called run_lfs
    assert "cmd" not in mock_run_command()

    # Restore is run here
    assert lfs.hsm_restore("/released")
    assert "hsm_restore" in mock_run_command()["cmd"]
    assert "/released" in mock_run_command()["cmd"]


@pytest.mark.run_command_result(None, "", "")
@pytest.mark.lfs_hsm_state({"/released": "released"})
@pytest.mark.lfs_dont_mock("hsm_restore")
def test_hsm_restore_timeout(mock_lfs, mock_run_command):
    """Test hsm_restore() timeout."""

    lfs = LFS("quota_id", "group")

    assert lfs.hsm_restore("/released") is None


@pytest.mark.lfs_hsm_state(
    {
        "/missing": "missing",
        "/unarchived": "unarchived",
        "/restored": "restored",
        "/released": "released",
    }
)
@pytest.mark.run_command_result(0, "", None)
@pytest.mark.lfs_dont_mock("hsm_release")
def test_hsm_release(mock_lfs, mock_run_command):
    """Test hsm_restore()."""

    lfs = LFS("quota_id", "group")

    assert not lfs.hsm_release("/missing")
    assert not lfs.hsm_release("/unarchived")
    assert lfs.hsm_release("/released")

    # None of these should have called run_lfs
    assert "cmd" not in mock_run_command()

    # Release is run here
    assert lfs.hsm_release("/restored")
    assert "hsm_release" in mock_run_command()["cmd"]
    assert "/restored" in mock_run_command()["cmd"]
