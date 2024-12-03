"""Test alpenhorn.common.logger"""

import logging
import pathlib
import socket
from unittest.mock import MagicMock, patch

import pytest

import alpenhorn.common.logger


def test_max_bytes_from_config():
    """Test throwing things at _max_bytes_from_config"""

    assert alpenhorn.common.logger._max_bytes_from_config("1") == 1
    assert alpenhorn.common.logger._max_bytes_from_config(12) == 12
    assert alpenhorn.common.logger._max_bytes_from_config("1k") == 1024
    assert alpenhorn.common.logger._max_bytes_from_config("1M") == 1024 * 1024
    assert alpenhorn.common.logger._max_bytes_from_config("1G") == 1024 * 1024 * 1024
    assert alpenhorn.common.logger._max_bytes_from_config("3.3") == 3
    assert alpenhorn.common.logger._max_bytes_from_config(3.3) == 3
    assert alpenhorn.common.logger._max_bytes_from_config("3.3k") == int(3.3 * 1024)
    assert alpenhorn.common.logger._max_bytes_from_config("3.3M") == int(
        3.3 * 1024 * 1024
    )

    for string in ["", 0, -1, "3.3T", "words"]:
        with pytest.raises(ValueError):
            alpenhorn.common.logger._max_bytes_from_config(string)


def test_log_buffer_gone(set_config, logger):
    """configure_logging() deletes logger.log_buffer"""

    assert logger.log_buffer is not None
    logger.configure_logging()
    assert logger.log_buffer is None


@pytest.mark.alpenhorn_config({"logging": {"syslog": {"test": True}}})
def test_config_sys_logging(set_config, logger):
    """Check that configure_logging calls configure_sys_logging() when a
    "logging.syslog" section is present."""

    with patch("alpenhorn.common.logger.configure_sys_logging") as mock:
        logger.configure_logging()

    mock.assert_called_once_with({"test": True})


@pytest.mark.alpenhorn_config({"logging": {"syslog": {"enable": 1}}})
def test_syslog_bad_enable(set_config, logger):
    """Test non-bool syslog.enable."""

    with pytest.raises(ValueError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config({"logging": {"syslog": {"facility": "invalid"}}})
def test_syslog_bad_facility(set_config, logger):
    """Test invalid syslog.facility."""

    with pytest.raises(ValueError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config({"logging": {"syslog": {"enable": False}}})
def test_syslog_disabled(set_config, logger):
    """Test explicit disable of syslog."""

    with patch("logging.handlers.SysLogHandler") as mock:
        logger.configure_logging()

    mock.assert_not_called()


@pytest.mark.alpenhorn_config({"logging": {"syslog": {"enable": True}}})
def test_syslog_default(set_config, logger):
    """Test default syslog config."""

    mock = MagicMock()
    mock.facility_names = {"user": 123}

    with patch("logging.handlers.SysLogHandler", mock):
        logger.configure_logging()

    mock.assert_called_with(
        address=("localhost", 514), facility=123, socktype=socket.SOCK_DGRAM
    )


@pytest.mark.alpenhorn_config(
    {
        "logging": {
            "syslog": {
                "address": "addr",
                "port": 12,
                "facility": "SOMETHING",
                "use_tcp": True,
            }
        }
    }
)
def test_syslog_params(set_config, logger):
    """Test syslog config params."""

    mock = MagicMock()
    mock.facility_names = {"user": 123, "something": 45}

    with patch("logging.handlers.SysLogHandler", mock):
        logger.configure_logging()

    mock.assert_called_with(
        address=("addr", 12), facility=45, socktype=socket.SOCK_STREAM
    )


@pytest.mark.alpenhorn_config({"logging": {"syslog": {"address": "sock", "port": 0}}})
def test_syslog_domain_socket(set_config, logger):
    """Test syslog domain socket config."""

    mock = MagicMock()
    mock.facility_names = {"user": 123}

    with patch("logging.handlers.SysLogHandler", mock):
        logger.configure_logging()

    mock.assert_called_with(address="sock", facility=123, socktype=socket.SOCK_DGRAM)


@pytest.mark.alpenhorn_config({"logging": {"syslog": {"use_tcp": "sometimes"}}})
def test_syslog_bad_use_tcp(set_config, logger):
    """Test non-bool syslog.use_tcp."""

    with pytest.raises(ValueError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config({"logging": {"file": {"test": True}}})
def test_config_file_logging(set_config, logger):
    """Check that configure_logging calls configure_file_logging() when a
    "logging.file" section is present."""

    with patch("alpenhorn.common.logger.configure_file_logging") as mock:
        logger.configure_logging()

    mock.assert_called_once_with({"test": True})


@pytest.mark.alpenhorn_config({"logging": {"file": {"watch": True}}})
def test_file_no_name(set_config, logger):
    """Can't have logger.file without logger.file.name."""

    with pytest.raises(KeyError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config({"logging": {"file": {"name": "/log", "watch": "sure"}}})
def test_file_bad_watch(set_config, logger):
    """Test non-bool file.watch."""

    with pytest.raises(ValueError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config(
    {"logging": {"file": {"name": "/log", "rotate": "maybe"}}}
)
def test_file_bad_rotate(set_config, logger):
    """Test non-bool file.rotate."""

    with pytest.raises(ValueError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config(
    {"logging": {"file": {"name": "/log", "watch": True, "rotate": True}}}
)
def test_file_watch_rotate(set_config, logger):
    """Can't set both file.watch and file.rotate to True."""

    with pytest.raises(ValueError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config({"logging": {"file": {"name": "/log"}}})
def test_file_logging(set_config, logger, xfs):
    """Test file logging.

    Also tests that log messages emitted before configurin_logging()
    do end up in the resultant logs.
    """

    log = logging.getLogger()

    log.warning("PRE-START")
    logger.configure_logging()
    log.warning("POST-START")

    with open("/log") as f:
        whole_log = f.read()

    assert "PRE-START" in whole_log
    assert "POST-START" in whole_log


@pytest.mark.alpenhorn_config({"logging": {"file": {"name": "/log", "watch": True}}})
def test_watchfile_logging(set_config, logger, xfs):
    """Test file logging and watching."""

    with patch("logging.handlers.WatchedFileHandler") as mock:
        logger.configure_logging()

    mock.assert_called_with(pathlib.Path("/log"))


@pytest.mark.alpenhorn_config(
    {"logging": {"file": {"name": "/log", "backup_count": "0", "rotate": True}}}
)
def test_rotate_bad_count(set_config, logger, xfs):
    """Test a bad backup_count while rotating."""

    with pytest.raises(ValueError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config(
    {"logging": {"file": {"name": "/log", "max_bytes": "Pi", "rotate": True}}}
)
def test_rotate_bad_size(set_config, logger, xfs):
    """Test a bad max_bytes while rotating."""

    with pytest.raises(ValueError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config(
    {
        "logging": {
            "file": {
                "name": "/log",
                "max_bytes": 1234,
                "backup_count": 567,
                "rotate": True,
            }
        }
    }
)
def test_rotatefile_logging(set_config, logger, xfs):
    """Test file logging and rotating."""

    with patch("alpenhorn.common.logger.RotatingFileHandler") as mock:
        logger.configure_logging()

    mock.assert_called_with(pathlib.Path("/log"), maxBytes=1234, backupCount=567)


@pytest.mark.alpenhorn_config(
    {
        "logging": {
            "level": "WARNING",
            "module_levels": {"mod1": "INFO", "mod2": "DEBUG"},
        }
    }
)
def test_logger_levels(set_config, logger):
    """Test setting logging levels."""

    logger.configure_logging()

    root = logging.getLogger()
    assert root.getEffectiveLevel() == logging.WARNING

    log = logging.getLogger("mod1")
    assert log.getEffectiveLevel() == logging.INFO

    log = logging.getLogger("mod2")
    assert log.getEffectiveLevel() == logging.DEBUG


@pytest.mark.alpenhorn_config({"logging": {"level": "INVALID"}})
def test_logger_bad_level(set_config, logger):
    """Test bad logging levels."""

    with pytest.raises(ValueError):
        logger.configure_logging()


@pytest.mark.alpenhorn_config({"logging": {"module_levels": {"alpenhorn": "INVALID"}}})
def test_logger_bad_module_levels(set_config, logger):
    """Test bad logging levels."""

    with pytest.raises(ValueError):
        logger.configure_logging()
