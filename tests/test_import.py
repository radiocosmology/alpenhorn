"""
test_import
------------------

Tests for `alpenhorn.auto_import` module.
"""

import pytest
import yaml
import os

import alpenhorn.db as db
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.acquisition as ac
import alpenhorn.auto_import as auto_import
import alpenhorn.generic as ge

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
def fixtures(tmpdir):
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db._connect()

    yield load_fixtures(tmpdir)

    db.database_proxy.close()


def test_schema(fixtures):
    """Basic sanity test of fixtures used"""
    assert set(db.database_proxy.get_tables()) == {
        u"storagegroup",
        u"storagenode",
        u"acqtype",
        u"archiveacq",
        u"filetype",
        u"archivefile",
        u"archivefilecopyrequest",
        u"archivefilecopy",
        u"zabinfo",
        u"quuxinfo",
        u"zxcinfo",
        u"spqrinfo",
        u"loginfo",
    }
    assert fixtures["root"].basename == "ROOT"
    assert st.StorageNode.get(st.StorageNode.name == "x").root == fixtures["root"]

    tmpdir = fixtures["root"]
    assert len(tmpdir.listdir()) == 3
    acq_dir = tmpdir.join("12345678T000000Z_inst_zab")
    assert len(acq_dir.listdir()) == 4


def test_import(fixtures):
    tmpdir = fixtures["root"]

    acq_dir = tmpdir.join("12345678T000000Z_inst_zab")

    node = st.StorageNode.get(st.StorageNode.name == "x")

    # import for hello.txt should be ignored while creating the acquisition
    # because 'zab' acq type only tracks *.zxc and *.log files
    auto_import.import_file(node, acq_dir.join("hello.txt").relto(tmpdir))
    assert ac.AcqType.get(ac.AcqType.name == "zab") is not None

    # the acquisition is still created
    acq = ac.ArchiveAcq.get(ac.ArchiveAcq.name == acq_dir.basename)
    assert acq is not None
    assert acq.name == acq_dir.basename
    assert acq.type.name == "zab"

    # while no file has been imported yet
    assert (ac.ArchiveFile.select().where(ac.ArchiveFile.acq == acq).count()) == 0

    # now import 'ch_master.log', which should succeed
    auto_import.import_file(node, acq_dir.join("ch_master.log").relto(tmpdir))
    file = ac.ArchiveFile.get(ac.ArchiveFile.name == "ch_master.log")
    assert file is not None
    assert file.acq == acq
    assert file.type.name == "log"
    assert file.size_b == len(
        fixtures["files"][acq_dir.basename][file.name]["contents"]
    )
    assert file.md5sum == fixtures["files"][acq_dir.basename][file.name]["md5"]

    file_copy = ar.ArchiveFileCopy.get(
        ar.ArchiveFileCopy.file == file, ar.ArchiveFileCopy.node == node
    )
    assert file_copy is not None
    assert file_copy.file == file
    assert file_copy.has_file == "Y"
    assert file_copy.wants_file == "Y"

    # re-importing ch_master.log should be a no-op
    auto_import.import_file(node, acq_dir.join("ch_master.log").relto(tmpdir))

    assert list(
        ac.ArchiveFile.select().where(ac.ArchiveFile.name == "ch_master.log")
    ) == [file]

    assert list(
        ar.ArchiveFileCopy.select().where(
            ar.ArchiveFileCopy.file == file, ar.ArchiveFileCopy.node == node
        )
    ) == [file_copy]


def test_import_existing(fixtures):
    """Checks for importing from an acquisition that is already in the archive"""
    tmpdir = fixtures["root"]

    acq_dir = tmpdir.join("x")

    node = st.StorageNode.get(st.StorageNode.name == "x")

    assert (ac.ArchiveAcq.select().where(ac.ArchiveAcq.name == "x").count()) == 1

    ## import an unknown file
    auto_import.import_file(node, acq_dir.join("foo.log").relto(tmpdir))
    assert (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "foo.log")
        .count()
    ) == 1

    ## import file for which ArchiveFile entry exists but not ArchiveFileCopy
    assert (
        ar.ArchiveFileCopy.select()  # no ArchiveFileCopy for 'jim'
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "jim")
        .count()
    ) == 0
    auto_import.import_file(node, acq_dir.join("jim").relto(tmpdir))
    assert (
        ar.ArchiveFileCopy.select()  # now we have an ArchiveFileCopy for 'jim'
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "jim")
        .count()
    ) == 1


def test_import_locked(fixtures):
    tmpdir = fixtures["root"]

    acq_dir = tmpdir.join("12345678T000000Z_inst_zab")

    node = st.StorageNode.get(st.StorageNode.name == "x")

    # import for foo.zxc should be ignored because there is also the
    # foo.zxc.lock file
    auto_import.import_file(node, acq_dir.join("foo.zxc").relto(tmpdir))
    assert (
        ac.ArchiveFile.select().where(ac.ArchiveFile.name == "foo.zxc").count()
    ) == 0

    # now delete the lock and try reimport, which should succeed
    acq_dir.join(".foo.zxc.lock").remove()
    auto_import.import_file(node, acq_dir.join("foo.zxc").relto(tmpdir))
    file = ac.ArchiveFile.get(ac.ArchiveFile.name == "foo.zxc")
    assert file.acq.name == acq_dir.basename
    assert file.type.name == "zxc"
    assert file.size_b == len(
        fixtures["files"][acq_dir.basename][file.name]["contents"]
    )
    assert file.md5sum == fixtures["files"][acq_dir.basename][file.name]["md5"]

    file_copy = ar.ArchiveFileCopy.get(
        ar.ArchiveFileCopy.file == file, ar.ArchiveFileCopy.node == node
    )
    assert file_copy.file == file
    assert file_copy.has_file == "Y"
    assert file_copy.wants_file == "Y"


def test_import_corrupted(fixtures):
    """Checks for importing from an acquisition that is already in the archive"""
    tmpdir = fixtures["root"]

    acq_dir = tmpdir.join("x")

    node = st.StorageNode.get(st.StorageNode.name == "x")
    ## reimport a file for which we have a copy that is corrupted
    assert list(
        ar.ArchiveFileCopy.select(
            ar.ArchiveFileCopy.has_file, ar.ArchiveFileCopy.wants_file
        )
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "sheila")
        .dicts()
    ) == [{"has_file": "X", "wants_file": "M"}]
    auto_import.import_file(node, acq_dir.join("sheila").relto(tmpdir))
    assert list(
        ar.ArchiveFileCopy.select(
            ar.ArchiveFileCopy.has_file, ar.ArchiveFileCopy.wants_file
        )
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "sheila")
        .dicts()
    ) == [{"has_file": "M", "wants_file": "Y"}]


def test_watchdog(fixtures):
    """Checks that the file system observer triggers imports on new/changed files"""
    tmpdir = fixtures["root"]

    acq_dir = tmpdir.join("12345678T000000Z_inst_zab")

    node = st.StorageNode.get(st.StorageNode.name == "x")

    import watchdog.events as ev

    watchdog_handler = auto_import.RegisterFile(node)

    # new acquisition file
    f = acq_dir.join("new_file.log")
    f.write("")
    assert file_copy_count("new_file.log") == 0
    watchdog_handler.on_created(ev.FileCreatedEvent(str(f)))
    assert file_copy_count("new_file.log") == 1

    # this file is outside any acqs and should be ignored
    g = tmpdir.join("some_file.log")
    g.write("Where is my acq?!")
    assert file_copy_count("some_file.log") == 0
    watchdog_handler.on_created(ev.FileCreatedEvent(str(g)))
    assert file_copy_count("some_file.log") == 0

    # now delete the lock and try reimport, which should succeed
    lock = acq_dir.join(".foo.zxc.lock")
    lock.remove()
    assert file_copy_count("foo.zxc") == 0
    watchdog_handler.on_deleted(ev.FileDeletedEvent(str(lock)))
    assert file_copy_count("foo.zxc") == 1


def test_import_nested(fixtures):
    tmpdir = fixtures["root"]

    acq_dir = tmpdir.join("alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab")

    node = st.StorageNode.get(st.StorageNode.name == "x")

    # import for hello.txt should be ignored while creating the acquisition
    # because 'zab' acq type only tracks *.zxc and *.log files
    auto_import.import_file(node, acq_dir.join("summary.txt").relto(tmpdir))
    assert ac.AcqType.get(ac.AcqType.name == "zab") is not None

    # the acquisition is still created
    acq = ac.ArchiveAcq.get(ac.ArchiveAcq.name == acq_dir.relto(tmpdir))
    assert acq is not None
    assert acq.name == acq_dir.relto(tmpdir)
    assert acq.type.name == "zab"

    # while no file has been imported yet
    assert (ac.ArchiveFile.select().where(ac.ArchiveFile.acq == acq).count()) == 0

    # now import 'acq_123_1.zxc', which should succeed
    auto_import.import_file(
        node, acq_dir.join("acq_data/x_123_1_data/raw/acq_123_1.zxc").relto(tmpdir)
    )
    file = ac.ArchiveFile.get(
        ac.ArchiveFile.name == "acq_data/x_123_1_data/raw/acq_123_1.zxc"
    )
    assert file.acq.name == acq_dir.relto(tmpdir)
    assert file.type.name == "zxc"
    assert file.size_b == len(
        fixtures["files"]["alp_root"]["2017"]["03"]["21"][
            "acq_xy1_45678901T000000Z_inst_zab"
        ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["contents"]
    )
    assert (
        file.md5sum
        == fixtures["files"]["alp_root"]["2017"]["03"]["21"][
            "acq_xy1_45678901T000000Z_inst_zab"
        ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["md5"]
    )

    file_copy = ar.ArchiveFileCopy.get(
        ar.ArchiveFileCopy.file == file, ar.ArchiveFileCopy.node == node
    )
    assert file_copy.file == file
    assert file_copy.has_file == "Y"
    assert file_copy.wants_file == "Y"


def file_copy_count(file_name):
    return (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == file_name)
        .count()
    )
