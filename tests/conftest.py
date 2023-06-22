"""Common fixtures"""
import pytest
from unittest.mock import patch, MagicMock

from alpenhorn import config, db, extensions
from alpenhorn.queue import FairMultiFIFOQueue
from alpenhorn.storage import StorageGroup, StorageNode
from alpenhorn.acquisition import ArchiveAcq, ArchiveFile
from alpenhorn.archive import ArchiveFileCopy, ArchiveFileCopyRequest
from alpenhorn.update import UpdateableNode, UpdateableGroup


def pytest_configure(config):
    """This function extends the pytest config file."""

    config.addinivalue_line(
        "markers",
        "run_command_result(ret, stdout, stderr): "
        "used on tests which mock alpenhorn.util.run_command to "
        "set the desired return value for the mocked call.",
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

    After the test completes, alpenhorn.config.config is set to None.
    """
    # Initialise with the default
    config.config = config._default_config.copy()

    marker = request.node.get_closest_marker("alpenhorn_config")
    if marker is not None:
        config.config = config.merge_dict_tree(config.config, marker.args[0])

    yield config.config

    # Reset globals
    config.config = None
    extensions._db_ext = None
    extensions._id_ext = None
    extensions._io_ext = dict()


@pytest.fixture
def mock_run_command(request, set_config):
    """Mock alpenhorn.util.run_command to _not_ run a command.

    The value returned by run_command() can be set by the test via the
    run_command_result mark.

    This fixture yields a function which returns a dictionary containing
    the arguments passed to run_command.
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
def mock_filesize():
    """Mocks DefaultNodeIO.filesize to return a fake file size."""

    def _mock_filesize(self, path, actual=False):
        return 512 * 3 if actual else 1234

    with patch("alpenhorn.io.default.DefaultNodeIO.filesize", _mock_filesize):
        yield


@pytest.fixture
def queue():
    """A test queue."""
    yield FairMultiFIFOQueue()


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
        """Mock of pathlib.PosixPath.stat to report the size of pyfakefs files."""
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

    return "alpenhost"


@pytest.fixture
def use_chimedb(set_config):
    """Use chimedb, if possible.

    Use this fixture before dbproxy, i.e.:

        def test_chimedb(use_chimedb, dbproxy):
            [...]

    Tests using this fixture will use the test-safe mode of `chimedb`
    (by calling `chimedb.core.test_enable()` first), which means they
    won't ever be run against the production database.  Typically, in
    this mode, an in-memory SQLite3 database is used, but that can be
    changed at runtime through environmental variables.  See the
    documentation of `chimedb.core.connectdb` for information on how
    to control `chimedb` test-safe mode.

    If `chimedb.core` can't be imported, tests using this fixture will
    be skipped.
    """
    cdb = pytest.importorskip("chimedb.core")
    cdb.test_enable()

    config.config = config.merge_dict_tree(
        set_config, {"extensions": ["chimedb.core.alpenhorn"]}
    )


@pytest.fixture
def dbproxy(set_config):
    """Database init and teardown.

    This fixture yields the database proxy after initialisation.
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
            ArchiveAcq,
            ArchiveFile,
            ArchiveFileCopy,
            ArchiveFileCopyRequest,
        ]
    )


@pytest.fixture
def loop_once(dbtables):
    """Ensure the main loop runs at most once."""

    waited = False

    def _wait(timeout=None):
        nonlocal waited
        waited = True

    def _is_set():
        nonlocal waited
        return waited

    mock = MagicMock()
    mock.wait = _wait
    mock.is_set = _is_set

    # This mocks the imported global_abort in update.py
    with patch("alpenhorn.update.global_abort", mock):
        yield mock


@pytest.fixture
def unode(simplenode, queue):
    """Returns an UpdateableNode."""
    return UpdateableNode(queue, simplenode)


@pytest.fixture
def mockio():
    """A mocked I/O module.

    Access the mocks via mockio.group and mockio.node.
    """
    # The I/O instances
    node = MagicMock()
    node.bytes_avail.return_value = 10000
    group = MagicMock()

    # This is our mock I/O module
    class MockIO:
        # The I/O "classes"
        def MockNodeIO(*args, **kwargs):
            nonlocal node
            node._instance_args = args
            node._instance_kwargs = kwargs
            return node

        def MockGroupIO(*args, **kwargs):
            nonlocal group
            group._instance_args = args
            node._instance_kwargs = kwargs
            return group

    MockIO.node = node
    MockIO.group = group

    # Patch extensions._io_ext so alpenhorn can find our module
    with patch.dict("alpenhorn.extensions._io_ext", mock=MockIO):
        yield MockIO


@pytest.fixture
def mockgroupandnode(hostname, queue, storagenode, storagegroup, mockio):
    """An UpdateableGroup and Updateablenode with mocked I/O classes.

    Yields the group and node.
    """

    stgroup = storagegroup(name="mockgroup", io_class="Mock")
    stnode = storagenode(
        name="mocknode",
        group=stgroup,
        root="/mocknode",
        host=hostname,
        active=True,
        io_class="Mock",
    )

    # Fix set_nodes
    mockio.group.set_nodes = lambda nodes: nodes

    node = UpdateableNode(queue, stnode)
    yield mockio, UpdateableGroup(group=stgroup, nodes=[node], idle=True), node


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
def archiveacq(factory_factory):
    return factory_factory(ArchiveAcq)


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
def simpleacq(archiveacq):
    """Create a simple ArchiveAcq record."""
    return archiveacq(name="simpleacq")


@pytest.fixture
def simplefile(archiveacq, archivefile):
    """Create a simple ArchiveFile record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="simplefile_acq")
    return archivefile(
        name="simplefile",
        acq=acq,
        md5sum="d41d8cd98f00b204e9800998ecf8427e",
        size_b=2**20,
    )


@pytest.fixture
def simplecopy(
    archiveacq,
    archivefile,
    archivefilecopy,
    storagenode,
    storagegroup,
):
    """Create a simple ArchiveFileCopy record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="simplecopy_acq")
    file = archivefile(name="simplecopy_file", acq=acq, size_b=2**20)
    group = storagegroup(name="simplecopy_group")
    node = storagenode(name="simplecopy_node", group=group)
    return archivefilecopy(file=file, node=node)


@pytest.fixture
def simplerequest(
    archiveacq,
    archivefile,
    archivefilecopyrequest,
    storagenode,
    storagegroup,
):
    """Create a simple ArchiveFileCopyRequest record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="simplerequest_acq")
    file = archivefile(name="simplerequest_file", acq=acq, size_b=2**20)
    group1 = storagegroup(name="simplerequest_group1")
    group2 = storagegroup(name="simplerequest_group2")
    node = storagenode(name="simplerequest_node", group=group1)
    return archivefilecopyrequest(file=file, node_from=node, group_to=group2)
