"""An end-to-end test of the alpenhorn daemon.

Things that this end-to-end test does:
    - checks a corrupt file

The purpose of this test isn't to exhaustively exercise all parts
of the daemon, but to check the connectivity of the high-level blocks
of the main update loop.
"""

import sys
import yaml
import pytest
import peewee as pw
from click.testing import CliRunner
from urllib.parse import quote as urlquote

from alpenhorn.service import cli
from alpenhorn.db import database_proxy, EnumField
from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.archive import ArchiveFileCopy, ArchiveFileCopyRequest
from alpenhorn.acquisition import ArchiveAcq, ArchiveFile

from examples import pattern_importer

# Make it easy for alpenhornd to find the extension
sys.modules["pattern_importer"] = pattern_importer

# database URI for a shared in-memory database
DB_URI = "file:e2edb?mode=memory&cache=shared"


@pytest.fixture
def e2e_db(xfs, hostname):
    """Create and populate the DB for the end-to-end test."""

    # Open
    db = pw.SqliteDatabase(DB_URI, uri=True)
    assert db is not None
    database_proxy.initialize(db)
    EnumField.native = False

    # Create tables
    db.create_tables(
        [
            ArchiveFileCopy,
            ArchiveFileCopyRequest,
            StorageGroup,
            StorageNode,
            pattern_importer.AcqType,
            pattern_importer.FileType,
            pattern_importer.ExtendedAcq,
            pattern_importer.ExtendedFile,
        ]
    )

    # Populate tables
    # ---------------

    # A Default-IO group with one node
    dftgrp = StorageGroup.create(name="dftgroup")
    dftnode = StorageNode.create(
        name="dftnode",
        group=dftgrp,
        root="/dft",
        host=hostname,
        active=True,
    )
    xfs.create_file("/dft/ALPENHORN_NODE", contents="dftnode")

    # The only acqtype
    pattern_importer.AcqType.create(name="acqtype", glob=True, patterns='["acq?"]')

    # The only (existing) acq
    acq1 = ArchiveAcq.create(name="acq1")

    # The only filetype
    pattern_importer.FileType.create(
        name="filetype",
        glob=True,
        patterns='["*.me"]',
    )

    # A file to check
    checkme = ArchiveFile.create(name="check.me", acq=acq1, size_b=0, md5sum="0")
    ArchiveFileCopy.create(
        file=checkme, node=dftnode, has_file="M", wants_file="Y", ready=True
    )
    xfs.create_file("/dft/acq1/check.me")

    yield


@pytest.fixture
def e2e_config(xfs, hostname):
    """Fixture creating the config file for the end-to-end test."""

    # The config.
    #
    # The weird value for "url" here gets around playhouse.db_url not
    # url-decoding the netloc of the supplied URL.  The netloc is used
    # as the "database" value, so to get the URI in there, we need to pass
    # it as a parameter, which WILL get urldecoded and supercede the empty
    # netloc.
    config = {
        "base": {"hostname": hostname},
        "extensions": [
            "pattern_importer",
        ],
        "database": {"url": "sqlite:///?database=" + urlquote(DB_URI) + "&uri=true"},
        "service": {"num_workers": 4},
    }

    # Put it in a file
    xfs.create_file("/etc/alpenhorn/alpenhorn.conf", contents=yaml.dump(config))


def test_cli(e2e_db, e2e_config, loop_once):
    runner = CliRunner()

    result = runner.invoke(cli, catch_exceptions=False)

    assert result.exit_code == 0

    # Check results

    # check.me is corrupt
    checkme = ArchiveFile.get(name="check.me")
    assert ArchiveFileCopy.get(file=checkme).has_file == "X"
