"""Test CLI: alpenhorn acq create"""

from alpenhorn.db import ArchiveAcq


def test_create(clidb, cli):
    """Test creating an acq."""

    cli(0, ["acq", "create", "TEST"])

    # Check created acq
    ArchiveAcq.get(name="TEST")


def test_bad_name(clidb, cli):
    """Test rejection of invalid name."""

    cli(1, ["acq", "create", "/test/"])

    # No acq was created
    assert ArchiveAcq.select().count() == 0


def test_create_existing(clidb, cli):
    """Test creating an acq that already exists."""

    # Add the acq
    ArchiveAcq.create(name="TEST")

    cli(1, ["acq", "create", "TEST"])

    # Check that no extra acq was created
    assert ArchiveAcq.select().count() == 1
