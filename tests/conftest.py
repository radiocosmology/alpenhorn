"""Common fixtures"""

import fileinput
import logging
import os
import re
import shutil
import traceback
from unittest.mock import MagicMock, patch
from urllib.parse import quote as urlquote

import pytest
import yaml
from peewee import SqliteDatabase

import alpenhorn.common.logger
from alpenhorn import db
from alpenhorn.common import config, extensions
from alpenhorn.daemon.update import UpdateableGroup, UpdateableNode
from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    ArchiveFileImportRequest,
    DataIndexVersion,
    StorageGroup,
    StorageNode,
    StorageTransferAction,
)
from alpenhorn.scheduler import FairMultiFIFOQueue


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
        "lfs_hsm_restore_result(result): "
        "used on tests which mock alpenhorn.io.lfs.LFS "
        "to indicate the result of the hsm_restore call.  result "
        "may be 'fail', 'timeout', 'wait', or 'restore'",
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
    config.addinivalue_line(
        "markers",
        "clirunner_args(**kwargs): "
        "set arguments used to instantiate the click.testing.CliRunner.",
    )


@pytest.fixture
def logger():
    """Set up for log testing

    Yields alpenhorn.common.logger.
    """

    alpenhorn.common.logger.init_logging(False)

    yield alpenhorn.common.logger

    # Teardown
    root = logging.getLogger()

    # Remove all handlers from the root logger
    handlers = root.handlers
    for handler in handlers:
        root.removeHandler(handler)

    alpenhorn.common.logger.log_buffer = None


@pytest.fixture
def set_config(request, logger):
    """Set alpenhorn.common.config.config for testing.

    Any value given in the alpenhorn_config mark is merged into the
    default config.

    Yields alpenhorn.common.config.config.

    After the test completes, alpenhorn.common.config.config is set to None.
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
    extensions._io_ext = {}


@pytest.fixture
def mock_run_command(request, set_config):
    """Mock alpenhorn.common.util.run_command to _not_ run a command.

    The value returned by run_command() can be set by the test via the
    run_command_result mark.

    This fixture yields a function which returns a dictionary containing
    the arguments passed to run_command.
    """
    run_command_report = {}

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

    with patch("alpenhorn.common.util.run_command", _mocked_run_command):
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
def have_lfs():
    """Mock shutil.which to indicate "lfs" is present."""

    original_which = shutil.which

    def _mocked_which(cmd, mode=os.F_OK | os.X_OK, path=None):
        """A mock of shutil.which that points to our test LFS command."""

        nonlocal original_which
        if cmd == "lfs":
            return "LFS"

        return original_which(cmd, mode, path)

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
        lfs_dont_mock = []
    else:
        lfs_dont_mock = marker.args

    marker = request.node.get_closest_marker("lfs_hsm_state")
    if marker is None:
        lfs_hsm_state = {}
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
        if state == "restoring":
            return HSMState.RESTORING
        if state == "released":
            return HSMState.RELEASED

        raise ValueError("Bad state in lfs_hsm_state marker: {state} for path {path}")

    def _mocked_lfs_hsm_restore(self, path):
        nonlocal request, lfs_hsm_state

        # de-pathlib-ify
        path = str(path)

        marker = request.node.get_closest_marker("lfs_hsm_restore_result")
        if marker:
            if marker.args[0] == "fail":
                # Return failure
                return False
            if marker.args[0] == "timeout":
                # Return timeout
                return None
            if marker.args[0] == "wait":
                # Return true (successful request)
                # without full restore
                lfs_hsm_state[path] = "restoring"
                return True
            if marker.args[0] == "restore":
                # Return true (successful request)
                # with full restore
                lfs_hsm_state[path] = "restored"
                return True

        state = lfs_hsm_state.get(path, "missing")
        if state == "missing":
            return False
        if state == "released":
            lfs_hsm_state[path] = "restoring"
        elif state == "restoring":
            lfs_hsm_state[path] = "restored"
        return True

    def _mocked_lfs_hsm_release(self, path):
        nonlocal lfs_hsm_state

        # de-pathlib-ify
        path = str(path)

        state = lfs_hsm_state.get(path, "missing")
        if state == "released":
            return True
        if state == "restored":
            lfs_hsm_state[path] = "released"
            return True

        # Missing, unarchived, or restoring
        return False

    marker = request.node.get_closest_marker("lfs_quota_remaining")
    if marker is None:
        lfs_quota = None
    else:
        lfs_quota = marker.args[0]

    def _mocked_lfs_quota_remaining(self, path):
        nonlocal lfs_quota
        return lfs_quota

    patches = []
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
def mock_exists(fs):
    """Mocks pathlib.PosixPath.exists to work with pyfakefs."""

    def _mocked_exists(path):
        """Mock of pathlib.PosixPath.exists to work better with pyfakefs.

        The problem here is if there's an unreadable file in a readable
        directory, pyfakefs raises PermissionError in pathlib.Path.exists,
        even though it should return True (since only the directory needs to
        be read for an existence check.)
        """
        nonlocal fs

        try:
            dir_ = fs.get_object(path.parent)
            # Parent directory not readable
            if not dir_.st_mode & 0o222:
                raise PermissionError("Permission denied")
        except FileNotFoundError:
            return False

        # Directory is readable
        try:
            fs.get_object(path)
        except PermissionError:
            pass
        except FileNotFoundError:
            return False

        return True

    with patch("pathlib.PosixPath.exists", _mocked_exists):
        yield


@pytest.fixture
def mock_observer():
    """Mocks the DefaultIO observer so its always the PollingObserver"""
    from watchdog.observers.polling import PollingObserver

    with patch("alpenhorn.io.default.DefaultNodeIO.observer", PollingObserver):
        yield


@pytest.fixture
def xfs(monkeypatch, fs, mock_observer, mock_statvfs, mock_exists):
    """An extended pyfakefs.

    Patches more stuff for proper behaviour with alpenhorn unittests"""

    # Run tests without a pre-set ALPENHORN_CONFIG_FILE envar
    try:
        monkeypatch.delenv("ALPENHORN_CONFIG_FILE")
    except KeyError:
        pass

    return fs


@pytest.fixture
def hostname(set_config):
    """Ensure our hostname is set.

    Returns the hostname."""

    config.config = config.merge_dict_tree(
        config.config, {"base": {"hostname": "alpenhost"}}
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
        config.config, {"extensions": ["chimedb.core.alpenhorn"]}
    )


@pytest.fixture
def dbproxy(set_config):
    """Database init and teardown.

    This fixture yields the database proxy after initialisation.
    """
    # Load extensions
    extensions.load_extensions()

    # Set database.url if not already present
    config.config = config.merge_dict_tree(
        {"database": {"url": "sqlite:///:memory:"}}, config.config
    )

    # DB start
    db.connect()

    yield db.database_proxy

    db.close()


@pytest.fixture
def dbtables(dbproxy):
    """Create all the usual tables in the database."""

    dbproxy.create_tables(db.gamut)

    # Set schema
    DataIndexVersion.create(component="alpenhorn", version=db.current_version)


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
    with patch("alpenhorn.daemon.update.global_abort", mock):
        yield mock


@pytest.fixture
def unode(dbtables, simplenode, queue):
    """Returns an UpdateableNode."""
    return UpdateableNode(queue, simplenode)


@pytest.fixture
def mockio():
    """A mocked I/O module.

    Access the mocks via mockio.group and mockio.node.
    """
    # The I/O instances
    remote = MagicMock()
    remote.pull_ready.return_value = False
    node = MagicMock()
    node.bytes_avail.return_value = 10000
    node.fifo = "n:mockio"
    group = MagicMock()
    group.fifo = "g:mockio"

    # This is our mock I/O module
    class MockIO:
        # The I/O "classes"
        def MockNodeRemote(*args, **kwargs):
            nonlocal remote
            remote._instance_args = args
            remote._instance_kwargs = kwargs
            return remote

        def MockNodeIO(*args, **kwargs):
            nonlocal node
            node._instance_args = args
            node._instance_kwargs = kwargs
            return node

        MockNodeIO.remote_class = MockNodeRemote

        def MockGroupIO(*args, **kwargs):
            nonlocal group
            group._instance_args = args
            node._instance_kwargs = kwargs
            return group

    MockIO.remote = remote
    MockIO.node = node
    MockIO.group = group

    # Patch extensions._io_ext so alpenhorn can find our module
    with patch.dict("alpenhorn.common.extensions._io_ext", mock=MockIO):
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
    yield mockio, UpdateableGroup(
        queue=queue, group=stgroup, nodes=[node], idle=True
    ), node


@pytest.fixture
def cli(request, xfs, cli_config):
    """Set up CLI tests using click

    Yields a wrapper around click.testing.CliRunner().invoke.
    The first parameter passed to the wrapper should be
    the expected exit code.  Other parameters are passed
    to CliRunner.invoke (including the list of command
    line parameters).

    The wrapper performs rudimentary checks on the result,
    then returns the click.result so the caller can inspect
    the result further, if desired.
    """

    from click.testing import CliRunner

    marker = request.node.get_closest_marker("clirunner_args")
    if marker is not None:
        kwargs = marker.kwargs
    else:
        kwargs = {}

    runner = CliRunner(**kwargs)

    def _cli_wrapper(expected_result, *args, **kwargs):
        from alpenhorn.cli import entry

        nonlocal runner

        result = runner.invoke(entry, *args, **kwargs)

        # Clean up fileinput
        fileinput.close()

        # Show traceback if one was created
        if (
            result.exit_code
            and result.exc_info
            and type(result.exception) is not SystemExit
        ):
            traceback.print_exception(*result.exc_info)

        # Print output so it appears in the test log on failure
        print(result.output)

        assert result.exit_code == expected_result
        if expected_result:
            assert type(result.exception) is SystemExit
        else:
            assert result.exception is None

        return result

    yield _cli_wrapper


@pytest.fixture
def clidb_uri():
    """Returns database URI for a shared in-memory database."""
    return "file:clidb?mode=memory&cache=shared"


@pytest.fixture
def clidb(clidb_noinit):
    """Initiliase a peewee connector to the CLI DB and create tables.

    Yields the connector."""

    clidb_noinit.create_tables(db.gamut)

    # Set schema
    DataIndexVersion.create(component="alpenhorn", version=db.current_version)

    yield clidb_noinit


@pytest.fixture
def clidb_noinit(clidb_uri):
    """Initialise a peewee connector to the empty CLI DB.

    Yields the connector."""

    # Open
    connector = SqliteDatabase(clidb_uri, uri=True)
    assert connector is not None
    db.database_proxy.initialize(connector)
    db.EnumField.native = False

    yield connector

    # Drop all the tables after the test
    for table in db.gamut:
        connector.execute_sql(f"DROP TABLE IF EXISTS {table._meta.table_name};")
    connector.close()


@pytest.fixture
def cli_config(xfs, clidb_uri):
    """Fixture creating the config file for CLI tests."""

    # The config.
    #
    # The weird value for "url" here gets around playhouse.db_url not
    # url-decoding the netloc of the supplied URL.  The netloc is used
    # as the "database" value, so to get the URI in there, we need to pass
    # it as a parameter, which WILL get urldecoded and supercede the empty
    # netloc.
    config = {
        "database": {"url": "sqlite:///?database=" + urlquote(clidb_uri) + "&uri=true"},
    }

    # Put it in a file
    xfs.create_file("/etc/alpenhorn/alpenhorn.conf", contents=yaml.dump(config))


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
def storagetransferaction(factory_factory):
    return factory_factory(StorageTransferAction)


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


@pytest.fixture
def archivefileimportrequest(factory_factory):
    return factory_factory(ArchiveFileImportRequest)


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
        size_b=2**30,
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
    node = storagenode(name="simplecopy_node", group=group, root="/simplecopy_node")
    return archivefilecopy(file=file, node=node)


@pytest.fixture
def simplecopyrequest(
    archiveacq,
    archivefile,
    archivefilecopyrequest,
    storagenode,
    storagegroup,
):
    """Create a simple ArchiveFileCopyRequest record.

    Creates all necessary backrefs.
    """
    acq = archiveacq(name="simplecopyrequest_acq")
    file = archivefile(name="simplecopyrequest_file", acq=acq, size_b=2**20)
    group1 = storagegroup(name="simplecopyrequest_group1")
    group2 = storagegroup(name="simplecopyrequest_group2")
    node = storagenode(name="simplecopyrequest_node", group=group1)
    return archivefilecopyrequest(file=file, node_from=node, group_to=group2)


@pytest.fixture
def simpleimportrequest(
    archivefileimportrequest,
    storagenode,
    storagegroup,
):
    """Create a simple ArchiveFileCopyRequest record.

    Creates all necessary backrefs.
    """
    group = storagegroup(name="simpleimportrequest_group")
    node = storagenode(name="simpleimportrequest_node", group=group)
    return archivefileimportrequest(path="simpleimportrequest", node=node)


@pytest.fixture
def assert_row_present():
    """Returns a function which checks for a row output in a table."""

    def _assert_row_present(text, *cells):
        """Check `text` for a row of data.

        Raises pytest.fail unless the list of `cells` comprise
        a row in the `tabulate` table output in `text`.
        """

        # stringify
        cells = [str(cell) for cell in cells]
        text_row = "  |  ".join(cells)

        # Now remove empty cells (because they're difficult to match) and armour
        cells = [re.escape(cell) for cell in cells if cell != ""]

        # Suppress traceback in pytest output, unless --full-trace is used
        __tracebackhide__ = True

        regex = r"^\s*" + r"\s+".join(cells) + r"\s*$"

        if re.search(regex, text, flags=re.MULTILINE) is None:
            pytest.fail("Row not found:\n   " + text_row)

    return _assert_row_present


@pytest.fixture
def cli_wrong_schema(clidb):
    """Set the schema of the CLI data index to the wrong version.

    DB must already be initialised.
    """

    DataIndexVersion.update(version=db.current_version + 1).where(
        DataIndexVersion.component == "alpenhorn"
    ).execute()
