"""
test_client_transport
----------------------------------

Tests for `alpenhorn.client.transport` module.
"""

import re

import pytest
from click.testing import CliRunner

try:
    from unittest.mock import call, patch
except ImportError:
    from mock import patch, call

import alpenhorn.client as cli
import alpenhorn.db as db
import alpenhorn.storage as st
ti = None

# XXX: client is broken
pytest.skip("client is broken", allow_module_level=True)


@pytest.fixture
def fixtures(tmpdir):
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db.init()
    db.connect()

    yield ti.load_fixtures(tmpdir)

    db.database_proxy.close()


@pytest.fixture(autouse=True)
def no_cli_init(monkeypatch):
    monkeypatch.setattr(cli.node, "config_connect", lambda: None)
    monkeypatch.setattr(cli.transport, "config_connect", lambda: None)


def test_list_transports(fixtures):
    """Test the transport list command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["transport", "list", "--help"])
    assert help_result.exit_code == 0
    assert "List known transport nodes" in help_result.output

    # Modify node 'x' to appear as a transport disk
    x_node = st.StorageNode.get(name="x")
    x_node.storage_type = "T"
    x_node.save(only=x_node.dirty_fields)

    result = runner.invoke(cli.cli, args=["transport", "list"])
    assert result.exit_code == 0
    assert re.match(
        r"Name +Mounted +Host +Root +Notes *\n"
        r"-+  -+  -+  -+  -+\n"
        r"x +Y +foo.example.com +[-_/\w]+\n",
        result.output,
        re.DOTALL,
    )


@patch("alpenhorn.util.alpenhorn_node_check")
@patch("os.path.ismount")
@patch("os.system")
def test_mount_transport(mock, mock_ismount, mock_node_check, fixtures):
    """Test the 'mount_transport' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["transport", "mount", "--help"])
    assert help_result.exit_code == 0
    assert (
        "Mount a transport disk into the system and then make it available"
        in help_result.output
    )
    assert (
        "Options:\n  --user TEXT     username to access this node" in help_result.output
    )

    mock_node_check.return_value = True
    mock_ismount.return_value = False
    result = runner.invoke(cli.cli, args=["transport", "mount", "z"])
    assert result.exit_code == 0
    assert mock.mock_calls == [call("mount /mnt/z")]
    assert re.match(r"Mounting disc at /mnt/z", result.output, re.DOTALL)

    mock_ismount.return_value = True
    result = runner.invoke(cli.cli, args=["transport", "mount", "x"])
    assert result.exit_code == 0
    assert mock.mock_calls == [call("mount /mnt/z")]
    assert re.match(
        r"x is already mounted in the filesystem. Proceeding to activate it.",
        result.output,
        re.DOTALL,
    )


@patch("os.system")
def test_unmount_transport(mock, fixtures):
    """Test the 'unmount_transport' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["transport", "unmount", "--help"])
    assert help_result.exit_code == 0
    assert (
        "Unmount a transport disk from the system and then remove it from alpenhorn."
        in help_result.output
    )
    assert "Options:\n  -h, --help  Show this message and exit." in help_result.output

    result = runner.invoke(cli.cli, args=["transport", "unmount", "x"])
    assert result.exit_code == 0
    assert mock.mock_calls == [call("umount /mnt/x")]
    assert re.match(r"Unmounting disc at /mnt/x", result.output, re.DOTALL)


@patch("alpenhorn.client.transport._get_e2label", return_value=None)
@patch("os.path.realpath", return_value=None)
@patch("os.mkdir")
@patch("subprocess.check_call", return_value=0)
@patch(
    "subprocess.check_output",
    side_effect=[
        "",
        '/dev/foo: LABEL="CH-fake-12-34-56-78" UUID="bar" TYPE="ext4"',
        "",
    ],
)
@patch("glob.glob", return_value=["/dev/disk/by-id/fake-12-34-56-78"])
@patch("os.getuid", return_value=0)
def test_format_transport(
    getuid_mock,
    glob_mock,
    check_output_mock,
    check_call_mock,
    mkdir_mock,
    realpath_mock,
    get_e2label_mock,
    fixtures,
):
    """Test the 'format_transport' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["transport", "format", "--help"])
    assert help_result.exit_code == 0
    assert (
        "Interactive routine for formatting a transport disc as a storage node"
        in help_result.output
    )
    assert "Options:\n  -h, --help  Show this message and exit." in help_result.output

    result = runner.invoke(cli.cli, args=["transport", "format", "12-34-56-78"])
    assert result.exit_code == 0
    assert re.match(
        r".*\nDisc is already formatted\.\n"
        + r'Labelling the disc as "CH-12-34-56-78"'
        + r".*\nSuccessfully created storage node.\n"
        + r"Node created but not activated. Run alpenhorn mount_transport for that.",
        result.output,
        re.DOTALL,
    )
    assert check_output_mock.mock_calls == [
        call(["blkid", "-p", "/dev/disk/by-id/fake-12-34-56-78"]),
        call(["blkid", "-p", "/dev/disk/by-id/fake-12-34-56-78-part1"]),
        call(["df"]),
    ]
    assert check_call_mock.mock_calls == [
        call(
            [
                "/sbin/e2label",
                "/dev/disk/by-id/fake-12-34-56-78-part1",
                "CH-12-34-56-78",
            ]
        )
    ]
    assert mkdir_mock.mock_calls == [call("/mnt/CH-12-34-56-78")]
    node = st.StorageNode.get(name="CH-12-34-56-78")
    assert node.group.name == "transport"
    assert node.root == "/mnt/CH-12-34-56-78"
    assert node.storage_type == "T"
