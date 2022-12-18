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
        "run_command_result(ret, stdout, stderr):"
        "the desired return value for util.run_command() in "
        "the mock_run_command fixture.",
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
def hostname():
    """Returns the current hostname."""
    return util.get_short_hostname()


@pytest.fixture
def queue():
    """A test queue."""
    yield FairMultiFIFOQueue()


@pytest.fixture
def lfs():
    """Set up the test lfs by fixing the location of the lfs binary to our test lfs.py.

    Must be used by any test that instantiates LFSQuotaNodeIO or NearlineNodeIO (because
    they, in turn, instantiate alpenhorn.io.lfs.LFS).
    """

    def _mocked_which(cmd, mode=os.F_OK | os.X_OK, path=None):
        """A mock of shutil.which that points to our test LFS command."""
        if cmd == "lfs":
            return pathlib.Path(__file__).with_name("lfs").absolute()

        return shutil.which(cmd, mode, path)

    with patch("shutil.which", _mocked_which):
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
    return storagenode(name="genericnode", group=group, root="/root")


@pytest.fixture
def genericacq(acqtype, archiveacq, filetype, archivefile):
    """Create a generic ArchiveAcq record.

    Creates all necessary backrefs.
    """
    acqtype = acqtype(name="genericacq_acqetype")
    return archiveacq(name="genericacq", type=acqtype)


@pytest.fixture
def genericfile(acqtype, archiveacq, filetype, archivefile):
    """Create a generic ArchiveFile record.

    Creates all necessary backrefs.
    """
    acqtype = acqtype(name="genericfile_actype")
    acq = archiveacq(name="genericfile_acq", type=acqtype)
    filetype = filetype(name="genericfile_filetype")
    return archivefile(
        name="genericfile",
        acq=acq,
        type=filetype,
        md5sum="d41d8cd98f00b204e9800998ecf8427e",
        size_b=2**20,
    )


@pytest.fixture
def genericcopy(
    acqtype,
    archiveacq,
    filetype,
    archivefile,
    archivefilecopy,
    storagenode,
    storagegroup,
):
    """Create a generic ArchiveFileCopy record.

    Creates all necessary backrefs.
    """
    acqtype = acqtype(name="genericcopy_actype")
    acq = archiveacq(name="genericcopy_acq", type=acqtype)
    filetype = filetype(name="genericcopy_filetype")
    file = archivefile(name="genericcopy_file", acq=acq, type=filetype, size_b=2**20)
    group = storagegroup(name="genericcopy_group")
    node = storagenode(name="genericcopy_node", group=group)
    return archivefilecopy(file=file, node=node)


@pytest.fixture
def genericrequest(
    acqtype,
    archiveacq,
    filetype,
    archivefile,
    archivefilecopyrequest,
    storagenode,
    storagegroup,
):
    """Create a generic ArchiveFileCopyRequest record.

    Creates all necessary backrefs.
    """
    acqtype = acqtype(name="genericrequest_actype")
    acq = archiveacq(name="genericrequest_acq", type=acqtype)
    filetype = filetype(name="genericrequest_filetype")
    file = archivefile(
        name="genericrequest_file", acq=acq, type=filetype, size_b=2**20
    )
    group1 = storagegroup(name="genericrequest_group1")
    group2 = storagegroup(name="genericrequest_group2")
    node = storagenode(name="genericrequest_node", group=group1)
    return archivefilecopyrequest(file=file, node_from=node, group_to=group2)
