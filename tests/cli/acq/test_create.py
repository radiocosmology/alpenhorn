"""Test CLI: alpenhorn acq create"""

import json
import pytest
from alpenhorn.db import ArchiveAcq


def test_create(clidb, cli):
    """Test creating an acq."""

    cli(0, ["acq", "create", "TEST"])

    # Check created acq
    ArchiveAcq.get(name="TEST")


def test_create_existing(clidb, cli):
    """Test creating an acq that already exists."""

    # Add the acq
    ArchiveAcq.create(name="TEST")

    cli(1, ["acq", "create", "TEST"])

    # Check that no extra acq was created
    assert ArchiveAcq.select().count() == 1
