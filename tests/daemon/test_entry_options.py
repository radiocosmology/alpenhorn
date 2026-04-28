"""Test daemon options."""

import pytest
from click.testing import CliRunner

from alpenhorn.daemon.entry import entry
from alpenhorn.db import StorageHost


@pytest.mark.alpenhorn_config({"daemon": {"archive_copy_count": 0}})
def test_no_integrity(xfs, clidb, config_file, hostname):
    """Test fatal error on integrity problems."""

    # Need a StorageHost to run on
    StorageHost.create(name=hostname)

    runner = CliRunner()

    # We use --once to avoid accidentally get stuck in a loop
    # in case this doesn't work
    result = runner.invoke(entry, ["--once"], catch_exceptions=False)

    # To get the daemon output in the test output in case of error
    print(result.stdout)

    assert result.exit_code != 0


@pytest.mark.alpenhorn_config({"daemon": {"archive_copy_count": 0}})
def test_no_integrity_override(xfs, clidb, config_file, hostname):
    """Test --disable-archive-integrity."""

    # Need a StorageHost to run on
    StorageHost.create(name=hostname)

    runner = CliRunner()

    # We use --once to avoid accidentally get stuck in a loop
    # in case this doesn't work
    result = runner.invoke(
        entry, ["--once", "--disable-archive-integrity"], catch_exceptions=False
    )

    # To get the daemon output in the test output in case of error
    print(result.stdout)

    assert result.exit_code == 0
