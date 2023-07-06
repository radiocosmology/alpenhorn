"""An end-to-end test of the alpenhorn daemon.

Things that this end-to-end test does:
    - auto-imports a file
    - auto-verifies a file
    - recalls a released file out of LustreHSM
    - pulls a file onto the Transport group
    - releases a file on HSM to free space
    - checks a corrupt file
    - deletes a file

The purpose of this test isn't to exhaustively exercise all parts
of the daemon, but to check the connectivity of the high-level blocks
of the main update loop.
"""

import os
import sys
import yaml
import pytest
import shutil
import pathlib
import peewee as pw
from unittest.mock import patch
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
        auto_import=True,
    )
    xfs.create_file("/dft/ALPENHORN_NODE", contents="dftnode")

    # A transport fleet
    fleet = StorageGroup.create(name="fleet", io_class="Transport")
    tp1 = StorageNode.create(
        name="tp1",
        storage_type="T",
        group=fleet,
        root="/tp/one",
        host=hostname,
        active=True,
    )
    tp2 = StorageNode.create(
        name="tp2",
        storage_type="T",
        group=fleet,
        root="/tp/two",
        host=hostname,
        active=True,
        auto_verify=1,
    )
    xfs.create_file("/tp/one/ALPENHORN_NODE", contents="tp1")
    xfs.create_file("/tp/two/ALPENHORN_NODE", contents="tp2")

    # A couple of HSM groups
    nlgrp = StorageGroup.create(
        name="nl1", io_class="LustreHSM", io_config='{"threshold": 1000}'
    )
    nl1 = StorageNode.create(
        name="nl1",
        group=nlgrp,
        io_class="LustreHSM",
        root="/nl1",
        host=hostname,
        active=True,
        # This is mostly ignored
        io_config='{"quota_group": "qgroup", "headroom": 10}',
    )
    sf1 = StorageNode.create(
        name="sf1", group=nlgrp, root="/sf1", host=hostname, active=True
    )
    xfs.create_file("/sf1/ALPENHORN_NODE", contents="sf1")

    nlgrp = StorageGroup.create(
        name="nl2", io_class="LustreHSM", io_config='{"threshold": 1000}'
    )
    nl2 = StorageNode.create(
        name="nl2",
        group=nlgrp,
        io_class="LustreHSM",
        root="/nl2",
        host=hostname,
        active=True,
        io_config='{"quota_group": "qgroup", "headroom": 1, "release_check_count": 1}',
    )
    StorageNode.create(name="sf2", group=nlgrp, root="/sf2", host=hostname, active=True)
    xfs.create_file("/sf2/ALPENHORN_NODE", contents="sf2")

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

    # A file that needs to be pulled onto the Transport group
    pullme = ArchiveFile.create(name="pull.me", acq=acq1, size_b=0)
    ArchiveFileCopy.create(
        file=pullme, node=dftnode, has_file="Y", wants_file="Y", size_b=0, ready=False
    )
    ArchiveFileCopyRequest.create(file=pullme, node_from=dftnode, group_to=fleet)
    xfs.create_file("/dft/acq1/pull.me")

    # A file that's going to be released to free space on HSM
    releaseme = ArchiveFile.create(name="release.me", acq=acq1, size_b=8000)
    ArchiveFileCopy.create(
        file=releaseme,
        node=nl1,
        has_file="Y",
        wants_file="Y",
        size_b=8000,
        ready=True,
    )
    xfs.create_file("/nl1/acq1/release.me", st_size=8000)

    # A file needing recall from HSM to be pulled to Transport
    restoreme = ArchiveFile.create(
        name="restore.me",
        acq=acq1,
        size_b=0,
    )
    ArchiveFileCopy.create(
        file=restoreme, node=nl1, has_file="Y", wants_file="Y", size_b=0, ready=False
    )
    ArchiveFileCopyRequest.create(file=restoreme, node_from=nl1, group_to=fleet)
    xfs.create_file("/nl1/acq1/restore.me")

    # A file that needs correcting for HSM state
    correctme = ArchiveFile.create(name="correct.me", acq=acq1, size_b=8000)
    ArchiveFileCopy.create(
        file=correctme,
        node=nl2,
        has_file="Y",
        wants_file="Y",
        size_b=8000,
        ready=False,
    )
    xfs.create_file("/nl2/acq1/correct.me", st_size=8000)

    # A file to check
    checkme = ArchiveFile.create(name="check.me", acq=acq1, size_b=0, md5sum="0")
    ArchiveFileCopy.create(
        file=checkme, node=dftnode, has_file="M", wants_file="Y", ready=True
    )
    xfs.create_file("/dft/acq1/check.me")

    # A file to delete -- also need two archive copies to allow deletion
    deleteme = ArchiveFile.create(name="delete.me", acq=acq1, size_b=0)
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

    # A file to auto-verify
    verifyme = ArchiveFile.create(
        name="verify.me",
        acq=acq1,
        md5sum="d41d8cd98f00b204e9800998ecf8427e",
        size_b=0,
    )
    ArchiveFileCopy.create(
        file=verifyme, node=tp2, has_file="Y", wants_file="Y", ready=True
    )
    xfs.create_file("/tp/two/acq1/verify.me")

    # A file to auto-import
    xfs.create_file("/dft/acq2/find.me", contents="")

    yield


@pytest.fixture
def mock_rsync(xfs):
    """Mocks rsync."""

    original_which = shutil.which

    def _mocked_which(cmd, mode=os.F_OK | os.X_OK, path=None):
        """A mock of shutil.which that fakes having rsync."""

        nonlocal original_which
        if cmd == "rsync":
            return "RSYNC"

        return original_which(cmd, mode, path)

    def _mocked_rsync(from_path, to_dir, size_b, local):
        """An ioutil.rsync mock."""

        nonlocal xfs

        filename = pathlib.PurePath(from_path).name
        destpath = pathlib.Path(to_dir, filename)

        xfs.create_file(destpath)

        return {"ret": 0, "stdout": "", "md5sum": True}

    with patch("shutil.which", _mocked_which):
        with patch("alpenhorn.io.ioutil.rsync", _mocked_rsync):
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


@pytest.mark.lfs_quota_remaining(2000)
@pytest.mark.lfs_hsm_state(
    {
        "/nl2/acq1/correct.me": "restored",
        "/nl1/acq1/restore.me": "released",
        "/nl1/acq1/release.me": "restored",
    }
)
def test_cli(e2e_db, e2e_config, mock_lfs, mock_rsync, loop_once):
    runner = CliRunner()

    result = runner.invoke(cli, catch_exceptions=False)

    assert result.exit_code == 0

    # Check HSM
    lfs = mock_lfs(quota_group="qgroup")
    assert lfs.hsm_state("/nl2/acq1/correct.me") == lfs.HSM_RESTORED
    assert lfs.hsm_state("/nl1/acq1/restore.me") == lfs.HSM_RESTORED
    assert lfs.hsm_state("/nl1/acq1/release.me") == lfs.HSM_RELEASED

    # Check results

    # find.me has been imported
    assert ArchiveAcq.get(name="acq2")
    assert ArchiveFile.get(name="find.me")

    # correct.me has been marked ready
    correctme = ArchiveFile.get(name="correct.me")
    assert ArchiveFileCopy.get(file=correctme).ready

    # restore.me is not ready (because the main loop only ran once).
    restoreme = ArchiveFile.get(name="restore.me")
    assert not ArchiveFileCopy.get(file=restoreme).ready

    # pull.me has been pulled
    tp1 = StorageNode.get(name="tp1")
    pullme = ArchiveFile.get(name="pull.me")
    afcr = ArchiveFileCopyRequest.get(file=pullme)
    assert afcr.completed
    assert not afcr.cancelled
    copy = ArchiveFileCopy(file=pullme, node=tp1)
    assert copy.path.exists()

    # release.me is not ready
    releaseme = ArchiveFile.get(name="release.me")
    assert not ArchiveFileCopy.get(file=releaseme).ready

    # check.me is corrupt
    checkme = ArchiveFile.get(name="check.me")
    assert ArchiveFileCopy.get(file=checkme).has_file == "X"

    # verify.me is okay
    verifyme = ArchiveFile.get(name="verify.me")
    assert ArchiveFileCopy.get(file=verifyme).has_file == "Y"

    # delete.me is gone from tp1
    deleteme = ArchiveFile.get(name="delete.me")
    copy = ArchiveFileCopy.get(file=deleteme, node=tp1)
    assert copy.has_file == "N"
    assert not copy.path.exists()
