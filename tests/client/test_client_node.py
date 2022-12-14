"""
test_client_node
----------------------------------

Tests for `alpenhorn.client.node` module.
"""

import re

import pytest
from click.testing import CliRunner

import alpenhorn.acquisition as ac
import alpenhorn.archive as ar
import alpenhorn.client as cli
import alpenhorn.db as db
import alpenhorn.storage as st
import alpenhorn.util as util

# XXX: client is broken
pytest.skip("client is broken", allow_module_level=True)

@pytest.fixture
def fixtures(tmpdir):
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db.init()
    db.connect()

    import test_import as ti

    yield ti.load_fixtures(tmpdir)

    db.database_proxy.close()


@pytest.fixture(autouse=True)
def no_cli_init(monkeypatch):
    monkeypatch.setattr(cli.node, "config_connect", lambda: None)


def test_create_node(fixtures):
    """Test the create node command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["node", "create", "--help"])
    assert help_result.exit_code == 0
    assert (
        "Create a storage NODE within storage GROUP with a ROOT directory on"
        in help_result.output
    )

    tmpdir = fixtures["root"]
    tmpdir.chdir()

    result = runner.invoke(cli.cli, ["node", "create", "y", "root", "hostname", "bar"])
    assert result.exit_code == 0
    assert (
        result.output
        == 'Added node "y" belonging to group "bar" in the directory "root" at host "hostname" to database.\n'
    )

    node = st.StorageNode.get(name="y")

    assert result.exit_code == 0
    assert node.group.name == "bar"
    assert node.name == "y"
    assert node.root == "root"
    assert node.host == "hostname"

    result = runner.invoke(cli.cli, ["node", "create", "y", "root", "hostname", "baba"])

    assert result.exit_code == 1
    assert result.output == 'Requested group "baba" does not exit in DB.\n'

    result = runner.invoke(cli.cli, ["node", "create", "x", "root", "hostname", "bar"])
    assert result.exit_code == 1
    assert result.output == 'Node name "x" already exists! Try a different name!\n'

    result = runner.invoke(
        cli.cli, ["node", "create", "--storage_type=Z", "z", "root", "hostname", "bar"]
    )
    assert result.exit_code == 2  # Click usage error


def test_list_nodes(fixtures):
    """Test the node list command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["node", "list", "--help"])
    assert help_result.exit_code == 0
    assert "List known storage nodes" in help_result.output

    result = runner.invoke(cli.cli, args=["node", "list"])
    assert result.exit_code == 0
    assert re.match(
        r"Name +Group +Type +Host +Root +Notes *\n"
        r"-+  -+  -+  -+  -+  -+\n"
        r"x +foo +A +foo.example.com +[-_/\w]+\n"
        r"z +bar +A +bar.example.com *\n",
        result.output,
        re.DOTALL,
    )


def test_rename_node(fixtures):
    """Test the node rename command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["node", "rename", "--help"])
    assert help_result.exit_code == 0
    assert "Change the name of a storage NODE to NEW-NAME" in help_result.output

    result = runner.invoke(cli.cli, args=["node", "rename", "x", "z"])
    assert result.exit_code == 1
    assert result.output == 'Node "z" already exists.\n'

    old_node = st.StorageNode.get(name="x")
    result = runner.invoke(cli.cli, args=["node", "rename", "x", "y"])
    assert result.exit_code == 0
    assert result.output == "Updated.\n"

    new_node = st.StorageNode.get(name="y")
    assert old_node.id == new_node.id


def test_modify_node(fixtures):
    """Test the node modify command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["node", "modify", "--help"])
    assert help_result.exit_code == 0
    assert "Change the properties of a storage NODE" in help_result.output

    result = runner.invoke(cli.cli, args=["node", "modify", "bla"])
    assert result.exit_code == 1
    assert result.output == 'Node "bla" does not exist!\n'

    result = runner.invoke(
        cli.cli, args=["node", "modify", "x", "--notes=Test test test"]
    )
    assert result.exit_code == 0
    assert result.output == "Updated.\n"

    x_node = st.StorageNode.get(name="x")
    assert x_node.notes == "Test test test"

    result = runner.invoke(cli.cli, args=["node", "modify", "x"])
    assert result.exit_code == 0
    assert result.output == "Nothing to do.\n"

    x_node = st.StorageNode.get(name="x")
    assert x_node.notes == "Test test test"
    assert x_node.max_total_gb == 10
    assert x_node.min_avail_gb == 1
    assert x_node.min_delete_age_days == 30

    result = runner.invoke(
        cli.cli,
        args=[
            "node",
            "modify",
            "x",
            "--min_avail_gb=5",
            "--min_delete_age_days=5",
            "--notes=",
        ],
    )
    assert result.exit_code == 0
    assert result.output == "Updated.\n"

    x_node = st.StorageNode.get(name="x")
    assert x_node.notes is None
    assert x_node.max_total_gb == 10
    assert x_node.min_avail_gb == 5
    assert x_node.min_delete_age_days == 5


def test_activate(fixtures):
    """Test the 'activate' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["node", "activate", "--help"])
    assert help_result.exit_code == 0
    assert (
        "Interactive routine for activating a storage node located at ROOT."
        in help_result.output
    )
    assert "Options:\n  --path TEXT      Root path for this node" in help_result.output

    # test for error when mounting a non-existent node
    result = runner.invoke(cli.cli, ["node", "activate", "nonexistent"])
    assert result.exit_code == 1
    output = result.output.splitlines()
    assert len(output) == 1
    assert 'Storage node "nonexistent" does not exist. I quit.' in output[0]

    # test for error when trying to mount a node that's already mounted
    result = runner.invoke(cli.cli, ["node", "activate", "x"])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert 'Node "x" is already active.' in output[0]

    # now pretend the node is inactive so we can try to activate it
    node = st.StorageNode.get(name="x")
    node.active = False
    node.save()

    # test for error when check for ALPENHORN_NODE fails
    result = runner.invoke(cli.cli, ["node", "activate", "x"])
    assert result.exit_code == 1
    assert 'Node "x" does not match ALPENHORN_NODE' in result.output
    assert not st.StorageNode.get(name="x").active

    # test for success when check for ALPENHORN_NODE passes and the node is
    # mounted
    x_root = fixtures["root"].join("x")
    x_root.join("ALPENHORN_NODE").write("x")
    result = runner.invoke(
        cli.cli,
        args=[
            "node",
            "activate",
            "--path=" + str(x_root),
            "--user=bozo",
            "--address=foobar.example.com",
            "x",
        ],
    )
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 2
    assert re.match(r'^I will set the host to ".+"\.$', output[0])
    assert 'Successfully activated "x".' == output[1]

    node = st.StorageNode.get(name="x")
    assert node.active
    assert node.root == x_root
    assert node.username == "bozo"
    assert node.address == "foobar.example.com"
    assert node.host == output[0].split('"')[1] == util.get_short_hostname()


def test_deactivate(fixtures):
    """Test the 'deactivate' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["node", "deactivate", "--help"])
    assert help_result.exit_code == 0
    assert (
        "Deactivate a storage node with location or named ROOT_OR_NAME."
        in help_result.output
    )
    assert "Options:\n  -h, --help  Show this message and exit." in help_result.output

    result = runner.invoke(cli.cli, args=["node", "deactivate", "x"])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert "Node successfully deactivated." in output[0]
    node = st.StorageNode.get(name="x")
    assert not node.active

    # deactivate already deactivated node
    result = runner.invoke(cli.cli, args=["node", "deactivate", "x"])
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert "There is no active node there any more" in output[0]

    # deactivate an unknown node
    result = runner.invoke(cli.cli, args=["node", "deactivate", "y"])
    assert result.exit_code == 1
    output = result.output.splitlines()
    assert "That is neither a node name, nor a path on this host. I quit." == output[0]


def test_active(fixtures):
    """Test the output of the 'active' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["node", "active", "--help"])
    assert help_result.exit_code == 0
    assert "List the nodes active on this" in help_result.output
    assert "Options:\n  -H, --host TEXT  Use specified host" in help_result.output

    # there are no files yet on the 'foo' host (i.e., on storage node 'x')
    result = runner.invoke(
        cli.cli, args=["node", "active", "--host", "foo.example.com"]
    )
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert re.match(r"^x\s+" + str(fixtures["root"]) + r"\s+0 files$", output[0])

    # now pretend node 'x' has a copy of 'fred'
    file_copy = (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "fred")
        .get()
    )
    file_copy.has_file = "Y"
    file_copy.save()
    file_copy.file.save()

    # now `active` should report one file
    result = runner.invoke(
        cli.cli, args=["node", "active", "--host", "foo.example.com"]
    )
    assert result.exit_code == 0
    output = result.output.splitlines()
    assert len(output) == 1
    assert re.match(r"^x\s+" + str(fixtures["root"]) + r"\s+1 files$", output[0])


def test_verify(fixtures):
    """Test the output of the 'verify' command"""
    tmpdir = fixtures["root"]

    runner = CliRunner()

    # test for error when mounting a non-existent node
    result = runner.invoke(cli.cli, ["node", "verify", "foo"])
    assert result.exit_code == 1
    assert 'Storage node "foo" does not exist.' in result.output

    # test for error when check when the node is not active
    node = st.StorageNode.get(name="x")
    node.active = False
    node.save()

    result = runner.invoke(cli.cli, ["node", "verify", "x"])
    assert result.exit_code == 1
    assert 'Node "x" is not active.' in result.output

    # test for error when check for ALPENHORN_NODE fails
    node.active = True
    node.root = str(tmpdir)
    node.save()

    result = runner.invoke(cli.cli, ["node", "verify", "x"])
    assert result.exit_code == 1
    assert 'Node "x" does not match ALPENHORN_NODE: '.format(node.root) in result.output

    # test for 'x' when it is mounted, but contains no files
    tmpdir.join("ALPENHORN_NODE").write("x")

    result = runner.invoke(cli.cli, ["node", "verify", "x"])
    assert result.exit_code == 0
    assert re.search(
        r"\n=== Summary ===\n"
        + r"  0 total files\n"
        + r"  0 missing files\n"
        + r"  0 corrupt files",
        result.output,
        re.DOTALL,
    )

    # now pretend node 'x' has a copy of 'fred'
    file_copy = (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "fred")
        .get()
    )
    file_copy.has_file = "Y"
    file_copy.save()
    result = runner.invoke(cli.cli, ["node", "verify", "x"])
    assert result.exit_code == 2
    assert re.search(
        r"\n=== Missing files ===\n"
        + str(tmpdir.join(file_copy.file.acq.name, file_copy.file.name)),
        result.output,
        re.DOTALL,
    )
    assert re.search(
        r"\n=== Summary ===\n"
        + r"  1 total files\n"
        + r"  1 missing files\n"
        + r"  0 corrupt files",
        result.output,
        re.DOTALL,
    )

    ## now add a known file ('fred')
    tmpdir.join("x", "fred").write("")
    result = runner.invoke(cli.cli, ["node", "verify", "--md5", "x"])
    assert result.exit_code == 1
    assert re.search(
        r"\n=== Corrupt files ===\n" + r"/.*/ROOT/x/fred\n"
        r".*\n=== Summary ===\n"
        + r"  1 total files\n"
        + r"  0 missing files\n"
        + r"  1 corrupt files",
        result.output,
        re.DOTALL,
    )


def test_verify_acqs(fixtures):
    """Test the output of the 'verify' command limited to certain acqs"""
    tmpdir = fixtures["root"]

    runner = CliRunner()

    # now pretend node 'x' is present and has a copy of 'fred'
    tmpdir.join("ALPENHORN_NODE").write("x")
    file_copy = (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "fred")
        .get()
    )
    file_copy.has_file = "Y"
    file_copy.save()

    # Verification ignores errors in acquisitions other than those specified by the `--acq` option
    result = runner.invoke(cli.cli, ["node", "verify", "--acq=z", "x"])
    assert result.exit_code == 0
    assert re.search(
        r"\n=== Summary ===\n"
        + "  0 total files\n"
        + "  0 missing files\n"
        + "  0 corrupt files",
        result.output,
        re.DOTALL,
    )

    # Having multiple acqs works as an OR filter
    result = runner.invoke(cli.cli, ["node", "verify", "--acq=z", "--acq=x", "x"])
    assert result.exit_code == 2
    assert re.search(
        r"\n=== Missing files ===\n"
        + str(tmpdir.join(file_copy.file.acq.name, file_copy.file.name)),
        result.output,
        re.DOTALL,
    )
    assert re.search(
        r"\n=== Summary ===\n"
        + "  1 total files\n"
        + "  1 missing files\n"
        + "  0 corrupt files",
        result.output,
        re.DOTALL,
    )

    ## Now add a known file ('fred'), but its content is wrong so it will fail the md5 check
    tmpdir.join("x", "fred").write("")

    # This still doesn't cause an error if the acquisition is ignored
    result = runner.invoke(cli.cli, ["node", "verify", "--acq=z", "--md5", "x"])
    assert result.exit_code == 0
    assert re.search(
        r"\n=== Summary ===\n"
        + "  0 total files\n"
        + "  0 missing files\n"
        + "  0 corrupt files",
        result.output,
        re.DOTALL,
    )

    ## When the acquisition matches the filter, the corruption is detected
    result = runner.invoke(cli.cli, ["node", "verify", "--md5", "--acq=x", "x"])
    assert result.exit_code == 1
    assert re.search(
        r"\n=== Corrupt files ===\n" + "/.*/ROOT/x/fred\n"
        ".*\n=== Summary ===\n"
        + "  1 total files\n"
        + "  0 missing files\n"
        + "  1 corrupt files",
        result.output,
        re.DOTALL,
    )


def test_clean(fixtures):
    """Test the 'clean' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["node", "clean", "--help"])
    assert help_result.exit_code == 0
    assert (
        "Clean up NODE by marking older files as potentially removable."
        in help_result.output
    )
    assert (
        "Options:\n  -d, --days INTEGER     Clean files older than <days>."
        in help_result.output
    )

    ## pretend 'fred' is 1 GB in size
    f = ac.ArchiveFile.get(ac.ArchiveFile.name == "fred")
    f.size_b = 1073741824.0
    f.save()

    # By default a FileCopy is set to has_file='N'
    file_copy = (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "fred")
    ).get()
    file_copy.has_file = "Y"
    file_copy.save()

    tmpdir = fixtures["root"]
    tmpdir.chdir()
    result = runner.invoke(cli.cli, args=["node", "clean", "-f", "x"])
    assert result.exit_code == 0
    assert re.search(
        r'\nMark 1 files \(1\.0 GB\) from "x" available for removal\.\n.*'
        + r"Marked 1 files available for removal.\n",
        result.output,
        re.DOTALL,
    )

    ## by default, the cleaned copy should be marked as 'maybe wanted'
    file_copy = (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "fred")
    ).get()
    assert file_copy.wants_file == "M"

    ## if we clean with the '--now' option, the copy should be marked as 'not wanted'
    file_copy.wants_file = "Y"
    file_copy.save()
    result = runner.invoke(cli.cli, args=["node", "clean", "-f", "--now", "x"])
    assert result.exit_code == 0
    assert re.search(
        r'\nMark 1 files \(1\.0 GB\) from "x" available for removal\.\n.*'
        + r"Marked 1 files available for removal.\n",
        result.output,
        re.DOTALL,
    )

    file_copy = (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "fred")
    ).get()
    assert file_copy.wants_file == "N"

    ## if we clean with the '--cancel' option, all unwanted copies should again be marked wanted
    result = runner.invoke(cli.cli, args=["node", "clean", "-f", "--cancel", "x"])
    assert result.exit_code == 0
    assert re.search(
        r'\nMark 1 files \(1\.0 GB\) from "x" for keeping\.\n.*'
        + r"Marked 1 files for keeping.\n",
        result.output,
        re.DOTALL,
    )

    file_copy = (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "fred")
    ).get()
    assert file_copy.wants_file == "Y"

    ## '--cancel' and '--now' are mutually exclusive options
    result = runner.invoke(cli.cli, args=["node", "clean", "--now", "--cancel", "x"])
    assert result.exit_code == 1
    assert "Options --cancel and --now are mutually exclusive." in result.output

    # using a non-existent node should be reported as an error
    result = runner.invoke(cli.cli, args=["node", "clean", "--force", "--cancel", "y"])
    assert result.exit_code == 1
    assert 'Storage node "y" does not exist.' in result.output

    # cleaning an archive node without the force flag or interactive
    # confirmation should be an error
    result = runner.invoke(cli.cli, args=["node", "clean", "z"])
    assert result.exit_code == 1
    assert 'Cannot clean archive node "z" without forcing.' in result.output


def test_scan(fixtures):
    """Test the 'node scan' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, args=["node", "scan", "--help"])
    assert help_result.exit_code == 0
    assert (
        "Scan the current directory for known acquisition files" in help_result.output
    )
    assert (
        "Options:\n  -v, --verbose\n  --acq TEXT      Limit import to specified acquisition directories."
        in help_result.output
    )

    tmpdir = fixtures["root"]
    tmpdir.chdir()

    # corrupt 'x/jim':
    tmpdir.join("x", "jim").write("Corrupted for the test")

    result = runner.invoke(cli.cli, args=["node", "scan", "-vv", "x"])
    assert result.exit_code == 0
    assert re.search(
        r"\n==== Summary ====\n\n"
        + r"Added 0 files\n\n"
        + r"1 corrupt files\.\n"
        + r"0 files already registered\.\n"
        + r"1 files not known\n"
        + r"2 directories were not acquisitions\.\n\n"
        + r"Added files:\n\n"
        + r"Corrupt:\n"
        + r"x/jim\n\n"
        + r"Unknown files:\n"
        + r"x/foo\.log\n\n"
        + r"Unknown acquisitions:\n"
        + r"12345678T000000Z_inst_zab\n"
        + r"alp_root\n\n$",
        result.output,
        re.DOTALL,
    )

    ## now add a known file ('fred') and restore 'jim' to correct contents
    tmpdir.join("x", "fred").write("")
    tmpdir.join("x", "jim").write("")

    result = runner.invoke(cli.cli, args=["node", "scan", "-vv", "--dry", "x"])
    assert result.exit_code == 0
    assert re.search(
        r"\n==== Summary ====\n\n"
        + r"Added 1 files\n\n"
        + r"0 corrupt files\.\n"
        + r"1 files already registered\.\n"
        + r"1 files not known\n"
        + r"2 directories were not acquisitions\.\n\n"
        + r"Added files:\n"
        + r"x/jim\n\n"
        + r"Corrupt:\n\n"
        + r"Unknown files:\n"
        + r"x/foo\.log\n\n"
        + r"Unknown acquisitions:\n"
        + r"12345678T000000Z_inst_zab\n"
        + r"alp_root\n\n$",
        result.output,
        re.DOTALL,
    )
    ## Because we're running in dry mode the database is not updated
    assert (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "jim")
        .count()
    ) == 0

    ## now repeat but allowing database change
    result = runner.invoke(cli.cli, args=["node", "scan", "-vv", "x"])
    assert result.exit_code == 0
    assert re.search(
        r"\n==== Summary ====\n\n"
        + r"Added 1 files\n\n"
        + r"0 corrupt files\.\n"
        + r"1 files already registered\.\n"
        + r"1 files not known\n"
        + r"2 directories were not acquisitions\.\n\n"
        + r"Added files:\n"
        + r"x/jim\n\n"
        + r"Corrupt:\n\n"
        + r"Unknown files:\n"
        + r"x/foo\.log\n\n"
        + r"Unknown acquisitions:\n"
        + r"12345678T000000Z_inst_zab\n"
        + r"alp_root\n\n$",
        result.output,
        re.DOTALL,
    )
    ## check the database state
    jims = list(
        ar.ArchiveFileCopy.select(
            ac.ArchiveFile.name,
            ar.ArchiveFileCopy.has_file,
            ar.ArchiveFileCopy.wants_file,
        )
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "jim")
        .dicts()
    )
    assert jims == [{"name": "jim", "has_file": "Y", "wants_file": "Y"}]


def test_scan_register_new(fixtures):
    """Test the 'node scan' command with the `--register-new` flag"""
    runner = CliRunner()

    tmpdir = fixtures["root"]
    tmpdir.chdir()

    ## check the starting database state
    assert (
        ac.ArchiveAcq.select()
        .where(ac.ArchiveAcq.name == "12345678T000000Z_inst_zab")
        .count()
    ) == 0

    result = runner.invoke(cli.cli, args=["node", "scan", "--register-new", "-vv", "x"])
    assert result.exit_code == 0
    assert re.search(
        r"\n==== Summary ====\n\n"
        + r"Registered 2 new acquisitions\n"
        + r"Added 6 files\n\n"
        + r"0 corrupt files\.\n"
        + r"0 files already registered\.\n"
        + r"2 files not known\n"
        + r"0 directories were not acquisitions\.\n\n"
        + r"New acquisitions:\n"
        + r"12345678T000000Z_inst_zab\n"
        + r"alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab\n\n"
        + r"Added files:\n"
        + r"12345678T000000Z_inst_zab/ch_master.log\n"
        + r"alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/proc/acq_123_1_proc.zxc\n"
        + r"alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/raw/acq_123_1.zxc\n"
        + r"alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_2_data/raw/acq_123_2.zxc\n"
        + r"x/foo.log\n"
        + r"x/jim\n\n"
        + r"Corrupt:\n"
        + r"\n"
        + r"Unknown files:\n"
        + r"12345678T000000Z_inst_zab/hello.txt\n"
        + r"alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/summary.txt\n"
        + r"\n"
        + r"Unknown acquisitions:\n\n$",
        result.output,
        re.DOTALL,
    )

    ## check the database state
    assert (
        ac.ArchiveAcq.select()
        .where(ac.ArchiveAcq.name == "12345678T000000Z_inst_zab")
        .count()
    ) == 1

    foo_logs = list(
        ar.ArchiveFileCopy.select(
            ac.ArchiveFile.name,
            ar.ArchiveFileCopy.has_file,
            ar.ArchiveFileCopy.wants_file,
        )
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "foo.log")
        .dicts()
    )
    assert foo_logs == [{"name": "foo.log", "has_file": "Y", "wants_file": "Y"}]


def test_nested_scan(fixtures):
    """Test the 'node scan' command"""
    runner = CliRunner()

    tmpdir = fixtures["root"]
    tmpdir.chdir()

    ## corrupt 'jim' and pretend an acquisition in 'alp_root' should be added
    tmpdir.join("x", "jim").write("Corrupted for the test")
    acq_type = ac.AcqType.create(name="zab")
    acq = ac.ArchiveAcq.create(
        name="alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab", type=acq_type
    )
    file_type = ac.FileType.get(name="zxc")
    acq_file = ac.ArchiveFile.create(
        name="acq_data/x_123_1_data/raw/acq_123_1.zxc",
        acq=acq,
        type=file_type,
        size_b=len(
            fixtures["files"]["alp_root"]["2017"]["03"]["21"][
                "acq_xy1_45678901T000000Z_inst_zab"
            ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["contents"]
        ),
        md5sum=fixtures["files"]["alp_root"]["2017"]["03"]["21"][
            "acq_xy1_45678901T000000Z_inst_zab"
        ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["md5"],
    )

    result = runner.invoke(cli.cli, args=["node", "scan", "-vv", "x"])
    assert result.exit_code == 0
    assert re.search(
        r"\n==== Summary ====\n\n"
        + r"Added 1 files\n\n"
        + r"1 corrupt files\.\n"
        + r"0 files already registered\.\n"
        + r"4 files not known\n"
        + r"1 directories were not acquisitions\.\n\n"
        + r"Added files:\n"
        + r"alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/raw/acq_123_1.zxc\n\n"
        + r"Corrupt:\n"
        + r"x/jim\n\n"
        + r"Unknown files:\n"
        + r"alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/proc/acq_123_1_proc.zxc\n"
        + r"alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_2_data/raw/acq_123_2.zxc\n"
        + r"alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/summary.txt\n"
        + r"x/foo\.log\n\n"
        + r"Unknown acquisitions:\n"
        + r"12345678T000000Z_inst_zab\n$",
        result.output,
        re.DOTALL,
    )
    ## check the database state
    acq_files = list(
        ar.ArchiveFileCopy.select(
            ac.ArchiveFile.name,
            ar.ArchiveFileCopy.has_file,
            ar.ArchiveFileCopy.wants_file,
        )
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == acq_file.name)
        .dicts()
    )
    assert acq_files == [{"name": acq_file.name, "has_file": "Y", "wants_file": "Y"}]


def test_scan_from_acq_dir(fixtures):
    """Test the 'node scan' command run from the acquisition directory"""
    tmpdir = fixtures["root"]
    runner = CliRunner()

    # fixup 'jim' details in the DB
    jim = ac.ArchiveFile.get(name="jim")
    jim.size_b = 0
    jim.md5sum = fixtures["files"]["x"]["jim"]["md5"]
    jim.save(only=jim.dirty_fields)
    assert (
        jim.copies.join(st.StorageNode).where(st.StorageNode.name == "x").count() == 0
    )

    # switch inside the acquisition
    tmpdir.join("x").chdir()

    result = runner.invoke(cli.cli, args=["node", "scan", "-vv", "x"])
    assert result.exit_code == 0
    expected_output = """
        ==== Summary ====

        Added 1 files

        0 corrupt files.
        0 files already registered.
        1 files not known
        0 directories were not acquisitions.

        Added files:
        x/jim

        Corrupt:

        Unknown files:
        x/foo.log

        """
    import textwrap

    assert textwrap.dedent(expected_output) in result.output

    assert (
        jim.copies.join(st.StorageNode).where(st.StorageNode.name == "x").count() == 1
    )


def test_scan_within_acq_dir(fixtures):
    """Test the 'node scan' command run from an acquisition subdirectory"""
    tmpdir = fixtures["root"]
    runner = CliRunner()

    acq_type = ac.AcqType.create(name="zab")
    acq = ac.ArchiveAcq.create(
        name="alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab", type=acq_type
    )
    file_type = ac.FileType.get(name="zxc")
    acq_file = ac.ArchiveFile.create(
        name="acq_data/x_123_1_data/raw/acq_123_1.zxc",
        acq=acq,
        type=file_type,
        size_b=len(
            fixtures["files"]["alp_root"]["2017"]["03"]["21"][
                "acq_xy1_45678901T000000Z_inst_zab"
            ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["contents"]
        ),
        md5sum=fixtures["files"]["alp_root"]["2017"]["03"]["21"][
            "acq_xy1_45678901T000000Z_inst_zab"
        ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["md5"],
    )
    assert (
        acq_file.copies.join(st.StorageNode).where(st.StorageNode.name == "x").count()
        == 0
    )

    # switch inside the acquisition
    tmpdir.join(
        "alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data"
    ).chdir()

    result = runner.invoke(cli.cli, args=["node", "scan", "-vv", "x"])
    assert result.exit_code == 0
    expected_output = """
        ==== Summary ====

        Added 1 files

        0 corrupt files.
        0 files already registered.
        1 files not known
        0 directories were not acquisitions.

        Added files:
        alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/raw/acq_123_1.zxc

        Corrupt:

        Unknown files:
        alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/proc/acq_123_1_proc.zxc

        """
    import textwrap

    assert textwrap.dedent(expected_output) in result.output

    assert (
        acq_file.copies.join(st.StorageNode).where(st.StorageNode.name == "x").count()
        == 1
    )


def test_scan_within_acq_dir_register_new(fixtures):
    """Test the 'node scan' command from within a directory, combined with the --register-new flag"""
    tmpdir = fixtures["root"]
    runner = CliRunner()

    # switch inside the acquisition
    tmpdir.join(
        "alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data"
    ).chdir()

    result = runner.invoke(cli.cli, args=["node", "scan", "--register-new", "-vv", "x"])
    assert result.exit_code == 0
    expected_output = """
        ==== Summary ====

        Registered 1 new acquisitions
        Added 2 files

        0 corrupt files.
        0 files already registered.
        0 files not known
        0 directories were not acquisitions.

        New acquisitions:
        alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab

        Added files:
        alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/proc/acq_123_1_proc.zxc
        alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/raw/acq_123_1.zxc

        Corrupt:

        Unknown files:

        """
    import textwrap

    assert textwrap.dedent(expected_output) in result.output

    acq = (
        ac.ArchiveAcq.select()
        .where(
            ac.ArchiveAcq.name
            == "alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab"
        )
        .get()
    )
    acq_file = (
        ac.ArchiveFile.select()
        .where(
            ac.ArchiveFile.acq == acq,
            ac.ArchiveFile.name == "acq_data/x_123_1_data/raw/acq_123_1.zxc",
        )
        .get()
    )
    assert acq_file.size_b == len(
        fixtures["files"]["alp_root"]["2017"]["03"]["21"][
            "acq_xy1_45678901T000000Z_inst_zab"
        ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["contents"]
    )
    assert (
        acq_file.md5sum
        == fixtures["files"]["alp_root"]["2017"]["03"]["21"][
            "acq_xy1_45678901T000000Z_inst_zab"
        ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["md5"]
    )
    assert (
        acq_file.copies.join(st.StorageNode).where(st.StorageNode.name == "x").count()
        == 1
    )


def test_scan_with_limiting(fixtures):
    """Test the 'node scan' command in combination with the `--acq` option"""
    tmpdir = fixtures["root"]
    runner = CliRunner()

    acq_type = ac.AcqType.create(name="zab")
    acq = ac.ArchiveAcq.create(
        name="alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab", type=acq_type
    )
    file_type = ac.FileType.get(name="zxc")
    acq_file = ac.ArchiveFile.create(
        name="acq_data/x_123_1_data/raw/acq_123_1.zxc",
        acq=acq,
        type=file_type,
        size_b=len(
            fixtures["files"]["alp_root"]["2017"]["03"]["21"][
                "acq_xy1_45678901T000000Z_inst_zab"
            ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["contents"]
        ),
        md5sum=fixtures["files"]["alp_root"]["2017"]["03"]["21"][
            "acq_xy1_45678901T000000Z_inst_zab"
        ]["acq_data"]["x_123_1_data"]["raw"]["acq_123_1.zxc"]["md5"],
    )
    assert (
        acq_file.copies.join(st.StorageNode).where(st.StorageNode.name == "x").count()
        == 0
    )

    # switch inside the acquisition so the `--acq x` is completely outside the current path
    tmpdir.join(
        "alp_root/2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data"
    ).chdir()

    node = st.StorageNode.get(name="x")
    result = runner.invoke(cli.cli, args=["node", "scan", "-vv", "--acq", "x", "x"])
    assert result.exit_code == 0
    assert (
        'Acquisition "x" is outside the current directory and will be ignored.'
        in result.output
    )
    assert (
        acq_file.copies.join(st.StorageNode).where(st.StorageNode.name == "x").count()
        == 0
    )
