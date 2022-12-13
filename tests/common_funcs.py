"""
common_fixtures
------------------

Common test fixutres
"""

import os

import pytest
import yaml

from alpenhorn import db
from alpenhorn import config
from alpenhorn import extensions
import alpenhorn.acquisition as ac
import alpenhorn.archive as ar
import alpenhorn.generic as ge
import alpenhorn.storage as st
import test_archive_model as ta


tests_path = os.path.abspath(os.path.dirname(__file__))


# Create handlers for the acquisition and file types
class ZabInfo(ge.GenericAcqInfo):
    _acq_type = "zab"
    _file_types = ["zxc", "log"]
    patterns = ["**zab"]


class QuuxInfo(ge.GenericAcqInfo):
    _acq_type = "quux"
    _file_types = ["zxc", "log"]
    patterns = ["*quux", "x"]


class ZxcInfo(ge.GenericFileInfo):
    _file_type = "zxc"
    patterns = ["**.zxc", "jim*", "sheila"]


class SpqrInfo(ge.GenericFileInfo):
    _file_type = "spqr"
    patterns = ["*spqr*"]


class LogInfo(ge.GenericFileInfo):
    _file_type = "log"
    patterns = ["*.log"]


def load_fixtures(tmpdir):
    """Loads data from tests/fixtures into the connected database"""
    fs = ta.load_fixtures()

    p = tmpdir.join("ROOT")

    (st.StorageNode.update(root=str(p)).where(st.StorageNode.name == "x").execute())

    # Register new handlers
    ac.AcqType.register_type(ZabInfo)
    ac.AcqType.register_type(QuuxInfo)
    ac.FileType.register_type(ZxcInfo)
    ac.FileType.register_type(SpqrInfo)
    ac.FileType.register_type(LogInfo)

    db.database_proxy.create_tables([ZabInfo, QuuxInfo, ZxcInfo, SpqrInfo, LogInfo])

    with open(os.path.join(tests_path, "fixtures/files.yml")) as f:
        fixtures = yaml.safe_load(f)

    def make_files(dir_name, files, root):
        d = root.mkdir(dir_name)
        rel_path = os.path.relpath(str(d), str(p))
        for file_name, file_data in files.items():
            if "md5" in file_data:
                f = d.join(file_name)
                f.write(file_data["contents"])
                for archive_file in (
                    ac.ArchiveFile.select()
                    .join(ac.ArchiveAcq)
                    .where(
                        ac.ArchiveAcq.name + "/" + ac.ArchiveFile.name
                        == rel_path + "/" + file_name
                    )
                ):
                    archive_file.size_b = len(file_data["contents"])
                    archive_file.md5sum = file_data["md5"]
                    archive_file.save()
                    break
            else:  # it's really a directory, recurse!
                make_files(file_name, file_data, d)

    make_files(p.basename, fixtures, tmpdir)

    return {"root": p, "files": fixtures}

@pytest.fixture
def use_chimedb():
    cdb = pytest.importorskip("chimedb.core")
    cdb.test_enable()

    # Add extension to config
    config.merge_config({"extensions", [ "chimedb.core.alpenhorn" ]})

def test_schema(fixtures):
    """Basic sanity test of fixtures used"""
    assert set(db.database_proxy.get_tables()) == {
        "storagegroup",
        "storagenode",
        "acqtype",
        "archiveacq",
        "filetype",
        "archivefile",
        "archivefilecopyrequest",
        "archivefilecopy",
        "zabinfo",
        "quuxinfo",
        "zxcinfo",
        "spqrinfo",
        "loginfo",
    }
    assert fixtures["root"].basename == "ROOT"
    assert st.StorageNode.get(st.StorageNode.name == "x").root == fixtures["root"]

    tmpdir = fixtures["root"]
    assert len(tmpdir.listdir()) == 3
    acq_dir = tmpdir.join("12345678T000000Z_inst_zab")
    assert len(acq_dir.listdir()) == 4
