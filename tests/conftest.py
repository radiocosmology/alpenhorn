"""Common fixtures"""
import os
import pytest
import pathlib
import shutil
from unittest.mock import patch

from alpenhorn import config, db, extensions, util
from alpenhorn.info_base import _NoInfo
from alpenhorn.queue import FairMultiFIFOQueue
from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.acquisition import (
    AcqType,
    ArchiveAcq,
    ArchiveFile,
    FileType,
    AcqFileTypes,
)
from alpenhorn.archive import ArchiveFile, ArchiveFileCopy, ArchiveFileCopyRequest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "run_command_result(ret, stdout, stderr): "
        "used on tests which mock alpenhorn.util.run_command to "
        "set the desired return value for the mocked call.",
    )
    config.addinivalue_line(
        "markers",
        "lfs_hsm_state(dict): "
        "used on tests which mock alpenhorn.io.lfs.LFS "
        "to indicate the desired HSM State value(s) to return. "
        "The keys of dict are the paths; the values should be "
        "one of: 'missing', 'unarchived', 'released', 'restored'.",
    )
    config.addinivalue_line(
        "markers",
        "lfs_quota_remaining(quota): "
        "used on tests which mock alpenhorn.io.lfs.LFS "
        "to indicate the desired quota that LFS.quota_remaining "
        "should return.",
    )
    config.addinivalue_line(
        "markers",
        "lfs_dont_mock(*method_names): "
        "used on tests which mock alpenhorn.io.lfs.LFS "
        "to indicate which parts of LFS should _not_ be mocked.",
    )
    config.addinivalue_line(
        "markers",
        "alpenhorn_config(*config_dict): "
        "used to set the alpenhorn.config for testing.  config_dict"
        "is merged with the default config.",
    )


@pytest.fixture
def set_config(request):
    """Set alpenhorn.config.config for testing.

    Any value given in the alpenhorn_config mark is merged into the
    default config.

    Yields alpenhorn.config.config.

    After the test completes, alpenhorn.config.config is set to None."""
    # Initialise with the default
    config.config = config._default_config.copy()

    marker = request.node.get_closest_marker("alpenhorn_config")
    if marker is not None:
        config.config = config.merge_dict_tree(config.config, marker.args[0])

    yield config.config

    # Reset globals
    config.config = None
    extensions._db_ext = None
    extensions._ext = None


@pytest.fixture
def mock_run_command(request):
    """Mock alpenhorn.util.run_command to _not_ run a command.

    The value returned by run_command() can be set by the test via the
    run_command_result mark.

    This fixture yields a function which returns a dictionary containing
    the agruments passed to run_command.
    """
    run_command_report = dict()

    # Ensure loaded
    from alpenhorn.util import run_command

    marker = request.node.get_closest_marker("run_command_result")
    if marker is None:
        run_command_result = (0, "", "")
    else:
        run_command_result = tuple(marker.args)

    def _mocked_run_command(cmd, timeout=None, **kwargs):
        nonlocal run_command_report
        nonlocal run_command_result

        # This just reports its input
        run_command_report["cmd"] = cmd
        run_command_report["timeout"] = timeout
        run_command_report["kwargs"] = kwargs

        # Return the requested value (or maybe the default)
        return run_command_result

    def _get_run_command_report():
        nonlocal run_command_report
        return run_command_report

    with patch("alpenhorn.util.run_command", _mocked_run_command):
        yield _get_run_command_report


@pytest.fixture
def mock_filesize():
    """Mocks DefaultNodeIO.filesize to return a fake file size."""

    # Load class first
    from alpenhorn.io.Default import DefaultNodeIO

    def _mock_filesize(self, path, actual=False):
        return 512 * 3 if actual else 1234

    with patch("alpenhorn.io.Default.DefaultNodeIO.filesize", _mock_filesize):
        yield


@pytest.fixture
def queue():
    """A test queue."""
    yield FairMultiFIFOQueue()


@pytest.fixture
def have_lfs():
    """Mock shutil.which to indicate "lfs" is present."""

    def _mocked_which(cmd, mode=os.F_OK | os.X_OK, path=None):
        """A mock of shutil.which that points to our test LFS command."""
        if cmd == "lfs":
            return "LFS"

        return shutil.which(cmd, mode, path)

    with patch("shutil.which", _mocked_which):
        yield


@pytest.fixture
def mock_lfs(have_lfs, request):
    """Mocks methods of alpenhorn.lfs.LFS for testing.

    the mocked hsm_state() method will return values specified in the
    lfs_hsm_state marker.  Passing a path not specified in the marker returns
    HSMState.MISSING.  The values in this dict may be updated by calling
    hms_restore() and hsm_release()

    The mocked quota_remaining() method will retun the value of the
    lfs_quota_remaining marker.  If that marker isn't set, behaves as if
    quota_remaining failed.

    Yields the LFS class.
    """

    from alpenhorn.io.lfs import LFS, HSMState

    marker = request.node.get_closest_marker("lfs_dont_mock")
    if marker is None:
        lfs_dont_mock = list()
    else:
        lfs_dont_mock = marker.args

    marker = request.node.get_closest_marker("lfs_hsm_state")
    if marker is None:
        lfs_hsm_state = dict()
    else:
        lfs_hsm_state = marker.args[0]

    def _mocked_lfs_hsm_state(self, path):
        nonlocal lfs_hsm_state

        # de-pathlib-ify
        path = str(path)

        state = lfs_hsm_state.get(path, "missing")
        if state == "missing":
            return HSMState.MISSING
        if state == "unarchived":
            return HSMState.UNARCHIVED
        if state == "restored":
            return HSMState.RESTORED
        if state == "released":
            return HSMState.RELEASED

        raise ValueError("Bad state in lfs_hsm_state marker: {state} for path {path}")

    def _mocked_lfs_hsm_restore(self, path):
        nonlocal lfs_hsm_state

        # de-pathlib-ify
        path = str(path)

        state = lfs_hsm_state.get(path, "missing")
        if state == "missing":
            return False
        if state == "released":
            lfs_hsm_state[path] = "restored"
        return True

    def _mocked_lfs_hsm_release(self, path):
        nonlocal lfs_hsm_state

        # de-pathlib-ify
        path = str(path)

        state = lfs_hsm_state.get(path, "missing")
        if state == "missing" or state == "unarchived":
            return False
        if state == "restored":
            lfs_hsm_state[path] = "released"

        return True

    marker = request.node.get_closest_marker("lfs_quota_remaining")
    if marker is None:
        lfs_quota = None
    else:
        lfs_quota = marker.args[0]

    def _mocked_lfs_quota_remaining(self, path):
        nonlocal lfs_quota
        return lfs_quota

    patches = list()
    if "hsm_state" not in lfs_dont_mock:
        patches.append(patch("alpenhorn.io.lfs.LFS.hsm_state", _mocked_lfs_hsm_state))
    if "hsm_release" not in lfs_dont_mock:
        patches.append(
            patch("alpenhorn.io.lfs.LFS.hsm_release", _mocked_lfs_hsm_release),
        )
    if "hsm_restore" not in lfs_dont_mock:
        patches.append(
            patch("alpenhorn.io.lfs.LFS.hsm_restore", _mocked_lfs_hsm_restore),
        )
    if "quota_remaining" not in lfs_dont_mock:
        patches.append(
            patch("alpenhorn.io.lfs.LFS.quota_remaining", _mocked_lfs_quota_remaining),
        )

    for p in patches:
        p.start()

    yield LFS

    for p in patches:
        p.stop()


@pytest.fixture
def mock_statvfs(fs):
    """Mocks os.statvfs to work with pyfakefs."""

    def _mocked_statvfs(path):
        """A mock of os.statvfs that reports the size of the pyfakefs filesystem."""
        nonlocal fs

        # Anything with a __dict__ works here.
        class Result:
            f_bavail = fs.get_disk_usage().free
            f_bsize = 1

        return Result

    with patch("os.statvfs", _mocked_statvfs):
        yield


@pytest.fixture
def mock_stat(fs):
    """Mocks pathlib.PosixPath.stat to work with pyfakefs."""

    def _mocked_stat(path):
        """A mock of pathlib.PosixPath.stat that reports the size of files in a pyfakefs filesystem."""
        nonlocal fs

        from math import ceil

        file = fs.get_object(path)
        size = file.size

        # Anything with a __dict__ works here.
        class Result:
            # stat reports sizes in 512-byte blocks
            st_blocks = ceil(size / 512)
            st_size = size
            st_mode = file.st_mode

        return Result

    with patch("pathlib.PosixPath.stat", _mocked_stat):
        yield


@pytest.fixture
def mock_observer():
    """Mocks the DefaultIO observer so its always the PollingObserver"""
    from watchdog.observers.polling import PollingObserver

    with patch("alpenhorn.io.Default.DefaultNodeIO.observer", PollingObserver):
        yield


@pytest.fixture
def xfs(fs, mock_observer, mock_statvfs, mock_stat):
    """An extended pyfakefs.

    Patches more stuff for proper behaviour with alpenhorn unittests"""
    return fs


@pytest.fixture
def hostname(set_config):
    """Ensure our hostname is set.

    Returns the hostname."""

    config.config = config.merge_dict_tree(
        set_config, {"base": {"hostname": "alpenhost"}}
    )

    yield "alpenhost"


@pytest.fixture
def use_chimedb(set_config):
    """Use chimedb, if possible.

    If chimedb.core can't be imported, tests
    using this fixture will be skipped.
    """
    cdb = pytest.importorskip("chimedb.core")
    cdb.test_enable()

    config.config = config.merge_dict_tree(
        set_config, {"extensions": ["chimedb.core.alpenhorn"]}
    )


@pytest.fixture
def dbproxy(set_config):
    """Database init and teardown.

    The fixture returns the database proxy after initialisation.
    """
    # Load extensions
    extensions.load_extensions()

    # DB start
    db.init()
    db.connect()

    yield db.database_proxy

    db.close()


@pytest.fixture
def dbtables(dbproxy):
    """Create all the usual tables in the database."""

    dbproxy.create_tables(
        [
            StorageGroup,
            StorageNode,
            AcqType,
            ArchiveAcq,
            FileType,
            ArchiveFile,
            ArchiveFileCopy,
            ArchiveFileCopyRequest,
        ]
    )


# Data table fixtures.  Each of these will add a row with the specified
# data to the appropriate table in the DB, creating the table first if
# necessary


@pytest.fixture
def factory_factory(dbproxy):
    """Fixture which creates a factory which, in turn, creates a factory fixture
    for inserting data into the database."""

    def _factory_factory(model):
        nonlocal dbproxy

        def _factory(**kwargs):
            nonlocal dbproxy
            nonlocal model

            # This does nothing if the table already exists
            dbproxy.create_tables([model])

            # Add and return the record
            return model.create(**kwargs)

        return _factory

    return _factory_factory


@pytest.fixture
def storagegroup(factory_factory):
    return factory_factory(StorageGroup)


@pytest.fixture
def storagenode(factory_factory):
    return factory_factory(StorageNode)


@pytest.fixture
def acqtype(factory_factory):
    return factory_factory(AcqType)


@pytest.fixture
def acqfiletypes(factory_factory):
    return factory_factory(AcqFileTypes)


@pytest.fixture
def archiveacq(factory_factory):
    return factory_factory(ArchiveAcq)


@pytest.fixture
def filetype(factory_factory):
    return factory_factory(FileType)


@pytest.fixture
def archivefile(factory_factory):
    return factory_factory(ArchiveFile)


@pytest.fixture
def archivefilecopy(factory_factory):
    return factory_factory(ArchiveFileCopy)


@pytest.fixture
def archivefilecopyrequest(factory_factory):
    return factory_factory(ArchiveFileCopyRequest)


# Generic versions of the above.  When you just want a record, but don't care
# what it is.


@pytest.fixture
def simplegroup(storagegroup):
    """Create a simple StorageGroup record."""
    return storagegroup(name="simplegroup")


@pytest.fixture
def simplenode(storagenode, storagegroup):
    """Create a simple StorageNode record.

    Creates all necessary backrefs.
    """
    group = storagegroup(name="simplenode_group")
    return storagenode(name="simplenode", group=group, root="/node")


@pytest.fixture
def simpleacqtype(acqtype):
    """Create a simple AcqType record."""

    return acqtype(name="simpleacqtype", info_config='{"patterns": ["simpleacq"]}')


@pytest.fixture
def simpleacqinfo():
    """Create a SimpleAcqInfo class."""

    class SimpleAcqInfo(_NoInfo):
        _type = "simpleacqtype"
        patterns = ["acq"]

    return SimpleAcqInfo


@pytest.fixture
def simpleacq(simpleacqtype, archiveacq):
    """Create a simple ArchiveAcq record."""
    return archiveacq(name="simpleacq", type=simpleacqtype)


@pytest.fixture
def simplefiletype(filetype, acqtype, acqfiletypes):
    """Create a simple FileType record attached to an acqtype."""
    at = acqtype(
        name="simplefiletype_acqtype", info_config='{"patterns": ["simplefile_acq"]}'
    )
    ft = filetype(name="simplefiletype", info_config='{"patterns": ["simplefile"]}')
    acqfiletypes(acq_type=at, file_type=ft)

    return ft


@pytest.fixture
def simplefileinfo(simplefiletype):
    """Create a SimpleFileInfo class."""

    class SimpleFileInfo(_NoInfo):
        _type = "simplefiletype"
        patterns = ["file"]

    return SimpleFileInfo


@pytest.fixture
def simplefile(simpleacqtype, archiveacq, simplefiletype, archivefile):
    """Create a simple ArchiveFile record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="simplefile_acq", type=simpleacqtype)
    return archivefile(
        name="simplefile",
        acq=acq,
        type=simplefiletype,
        md5sum="d41d8cd98f00b204e9800998ecf8427e",
        size_b=2**20,
    )


@pytest.fixture
def simplecopy(
    simpleacqtype,
    archiveacq,
    simplefiletype,
    archivefile,
    archivefilecopy,
    storagenode,
    storagegroup,
):
    """Create a simple ArchiveFileCopy record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="simplecopy_acq", type=simpleacqtype)
    file = archivefile(
        name="simplecopy_file", acq=acq, type=simplefiletype, size_b=2**20
    )
    group = storagegroup(name="simplecopy_group")
    node = storagenode(name="simplecopy_node", group=group)
    return archivefilecopy(file=file, node=node)


@pytest.fixture
def simplerequest(
    simpleacqtype,
    archiveacq,
    simplefiletype,
    archivefile,
    archivefilecopyrequest,
    storagenode,
    storagegroup,
):
    """Create a simple ArchiveFileCopyRequest record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="simplerequest_acq", type=simpleacqtype)
    file = archivefile(
        name="simplerequest_file", acq=acq, type=simplefiletype, size_b=2**20
    )
    group1 = storagegroup(name="simplerequest_group1")
    group2 = storagegroup(name="simplerequest_group2")
    node = storagenode(name="simplerequest_node", group=group1)
    return archivefilecopyrequest(file=file, node_from=node, group_to=group2)
