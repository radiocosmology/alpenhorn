"""An end-to-end test of the alpenhorn daemon.

Things that this end-to-end test does:
    - checks a corrupt file
    - deletes a file

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

    # Future PR will turn this into a transport fleet
    fleet = StorageGroup.create(name="fleet")
    tp1 = StorageNode.create(
        name="tp1",
        storage_type="T",
        group=fleet,
        root="/tp/one",
        host=hostname,
        active=True,
    )
    xfs.create_file("/tp/one/ALPENHORN_NODE", contents="tp1")

    # A future PR will turn this into a Nearline group
    nlgrp = StorageGroup.create(name="nl1")
    sf1 = StorageNode.create(
        name="sf1", group=nlgrp, root="/sf1", host=hostname, active=True
    )
    xfs.create_file("/sf1/ALPENHORN_NODE", contents="sf1")

    # The only acqtype
    pattern_importer.AcqType.create(name="acqtype", glob=True, patterns='["acq?"]')

    # The only (existing) acq
    acq1 = ArchiveAcq.create(name="acq1")

    # The only filetype
    filetype = pattern_importer.FileType.create(
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

    # A file to delete -- also need two archive copies to allow deletion
    deleteme = ArchiveFile.create(name="delete.me", type=filetype, acq=acq1, size_b=0)
    ArchiveFileCopy.create(
        file=deleteme, node=dftnode, has_file="Y", wants_file="Y", ready=True
    )
    ArchiveFileCopy.create(
        file=deleteme, node=sf1, has_file="Y", wants_file="Y", ready=True
    )
    ArchiveFileCopy.create(
        file=deleteme, node=tp1, has_file="Y", wants_file="N", ready=True
    )
    xfs.create_file("/dft/acq1/delete.me")
    xfs.create_file("/sf/acq1/delete.me")
    xfs.create_file("/tp/one/acq1/delete.me")

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

    tp1 = StorageNode.get(name="tp1")

    # check.me is corrupt
    checkme = ArchiveFile.get(name="check.me")
    assert ArchiveFileCopy.get(file=checkme).has_file == "X"

    # delete.me is gone from tp1
    deleteme = ArchiveFile.get(name="delete.me")
    copy = ArchiveFileCopy.get(file=deleteme, node=tp1)
    assert copy.has_file == "N"
    assert not copy.path.exists()
