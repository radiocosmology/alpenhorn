"""Common fixtures"""
import os
import pytest
import pathlib
import shutil
from unittest.mock import patch

from alpenhorn import config, db, extensions, util
from alpenhorn.queue import FairMultiFIFOQueue
from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.acquisition import AcqType, ArchiveAcq, ArchiveFile, FileType
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
        "used on tests which mock alpenhorn.io.lfs.LFS.hsm_state() "
        "to indicate the desired HSM State value(s) to return. "
        "The keys of dict are the paths; the values should be "
        "one of: 'missing', 'unarchived', 'released', 'restored'.",
    )


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
def hostname():
    """Returns the current hostname."""
    return util.get_short_hostname()


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
    """Mocks alpenhorn.lfs.LFS.hsm_state for testing.

    the mocked hsm_state() method will return values specified in the
    lfs_hsm_state marker.  Passing a path not specified in the marker returns HSMState.MISSING."""

    from alpenhorn.io.lfs import LFS, HSMState

    marker = request.node.get_closest_marker("lfs_hsm_state")
    if marker is None:
        lfs_hsm_state = dict()
    else:
        lfs_hsm_state = marker.args[0]

    def _mocked_lfs_hsm_state(self, path):
        nonlocal lfs_hsm_state

        value = lfs_hsm_state.get(path, "missing")
        if value == "missing":
            return HSMState.MISSING
        if value == "unarchived":
            return HSMState.UNARCHIVED
        if value == "restored":
            return HSMState.RESTORED
        if value == "released":
            return HSMState.RELEASED

        raise ValueError("Bad value in lfs_hsm_state marker: {value} for path {path}")

    with patch("alpenhorn.io.lfs.LFS.hsm_state", _mocked_lfs_hsm_state):
        yield


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
def xfs(fs, mock_statvfs, mock_stat):
    """An extended pyfakefs which patches more os functions
    for proper behaviour with alpenhorn"""
    return fs


@pytest.fixture
def hostname():
    """Ensure our hostname is set.

    Returns the hostname."""

    config.merge_config({"base": {"hostname": "alpenhost"}})

    yield "alpenhost"

    # Reset the config
    config.merge_config(dict(), replace=True)


@pytest.fixture
def use_chimedb():
    """Use chimedb, if possible.

    If chimedb.core can't be imported, tests
    using this fixture will be skipped.
    """
    cdb = pytest.importorskip("chimedb.core")
    cdb.test_enable()

    config.merge_config({"extensions": ["chimedb.core.alpenhorn"]})

    yield

    # Reset the config
    config.merge_config(dict(), replace=True)


@pytest.fixture
def dbproxy():
    """Database init and teardown.

    The fixture returns the database proxy after initialisation.
    """
    # Ensure config is initialised
    config.merge_config(dict())

    # Load extensions
    extensions.load_extensions()

    # DB start
    db.init()
    db.connect()

    yield db.database_proxy

    db.close()


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
def genericgroup(storagegroup):
    """Create a generic StorageGroup record."""
    return storagegroup(name="genericgroup")


@pytest.fixture
def genericnode(storagenode, storagegroup):
    """Create a generic StorageNode record.

    Creates all necessary backrefs.
    """
    group = storagegroup(name="genericnode_group")
    return storagenode(name="genericnode", group=group, root="/node")


@pytest.fixture
def genericacqtype(acqtype):
    """Create a generic FileType record."""
    return acqtype(name="genericacqtype")


@pytest.fixture
def genericacq(genericacqtype, archiveacq):
    """Create a generic ArchiveAcq record."""
    return archiveacq(name="genericacq", type=genericacqtype)


@pytest.fixture
def genericfiletype(filetype):
    """Create a generic FileType record."""
    return filetype(name="genericfiletype")


@pytest.fixture
def genericfile(genericacqtype, archiveacq, genericfiletype, archivefile):
    """Create a generic ArchiveFile record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="genericfile_acq", type=genericacqtype)
    return archivefile(
        name="genericfile",
        acq=acq,
        type=genericfiletype,
        md5sum="d41d8cd98f00b204e9800998ecf8427e",
        size_b=2**20,
    )


@pytest.fixture
def genericcopy(
    genericacqtype,
    archiveacq,
    genericfiletype,
    archivefile,
    archivefilecopy,
    storagenode,
    storagegroup,
):
    """Create a generic ArchiveFileCopy record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="genericcopy_acq", type=genericacqtype)
    file = archivefile(
        name="genericcopy_file", acq=acq, type=genericfiletype, size_b=2**20
    )
    group = storagegroup(name="genericcopy_group")
    node = storagenode(name="genericcopy_node", group=group)
    return archivefilecopy(file=file, node=node)


@pytest.fixture
def genericrequest(
    genericacqtype,
    archiveacq,
    genericfiletype,
    archivefile,
    archivefilecopyrequest,
    storagenode,
    storagegroup,
):
    """Create a generic ArchiveFileCopyRequest record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="genericrequest_acq", type=genericacqtype)
    file = archivefile(
        name="genericrequest_file", acq=acq, type=genericfiletype, size_b=2**20
    )
    group1 = storagegroup(name="genericrequest_group1")
    group2 = storagegroup(name="genericrequest_group2")
    node = storagenode(name="genericrequest_node", group=group1)
    return archivefilecopyrequest(file=file, node_from=node, group_to=group2)
