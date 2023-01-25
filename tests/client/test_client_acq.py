"""
test_client_acq
----------------------------------

Tests for `alpenhorn.client.acq` module.
"""

import re

import pytest
from click.testing import CliRunner

import alpenhorn.acquisition as ac
import alpenhorn.archive as ar
import alpenhorn.client as cli
import alpenhorn.db as db
import alpenhorn.storage as st


@pytest.fixture
def fixtures(tmpdir):
    """Initializes an in-memory Sqlite database with data in tests/fixtures"""
    db._connect()

    import test_import as ti

    yield ti.load_fixtures(tmpdir)

    db.database_proxy.close()


@pytest.fixture(autouse=True)
def no_cli_init(monkeypatch):
    monkeypatch.setattr(cli.acq, "config_connect", lambda: None)


def test_help(fixtures):
    """Test the acq command help"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["acq", "--help"])
    assert help_result.exit_code == 0
    assert """Commands operating on archival data products.""" in help_result.output


def test_list(fixtures):
    """Test the 'acq list' command"""
    runner = CliRunner()

    help_result = runner.invoke(cli.cli, ["acq", "list", "--help"])
    assert help_result.exit_code == 0
    assert "List known acquisitions." in help_result.output

    # List all registered acquisitions (there is only one, 'x', with files 'red', 'jim', and 'sheila')
    result = runner.invoke(cli.cli, ["acq", "list"])
    assert result.exit_code == 0
    assert re.match(r"Name +Files\n" r"-+  -+\n" r"x +3 *\n", result.output, re.DOTALL)

    # Fail when given a non-existent storage node
    result = runner.invoke(cli.cli, ["acq", "list", "y"])
    assert result.exit_code == 1
    assert "No such storage node: y" in result.output

    # Check acquisitions present on node 'x' (only 'fred' and 'sheila' from 'x', 'jim' is missing)
    result = runner.invoke(cli.cli, ["acq", "list", "x"])
    assert result.exit_code == 0
    assert re.match(r"Name +Files\n" r"-+  -+\n" r"x +2 *\n", result.output, re.DOTALL)


def test_files(fixtures):
    """Test the 'acq files' command"""
    runner = CliRunner()

    # Check help output
    help_result = runner.invoke(cli.cli, ["acq", "files", "--help"])
    assert help_result.exit_code == 0
    assert "List files that are in the ACQUISITION." in help_result.output

    # Fail when given a non-existent acquisition
    result = runner.invoke(cli.cli, ["acq", "files", "z"])
    assert result.exit_code == 1
    assert "No such acquisition: z" in result.output

    # Check regular case
    result = runner.invoke(cli.cli, ["acq", "files", "x"])
    assert result.exit_code == 0
    assert re.match(
        r".*Name +Size +MD5 *\n"
        r"-+  -+  -+\n"
        r"fred *\n"
        r"jim +0 +d41d8cd98f00b204e9800998ecf8427e *\n"
        r"sheila *\n$",
        result.output,
        re.DOTALL,
    )

    # Fail when given a non-existent storage node
    result = runner.invoke(cli.cli, ["acq", "files", "x", "y"])
    assert result.exit_code == 1
    assert "No such storage node: y" in result.output

    # Check files from 'x' present on node 'x' (only 'fred' and 'sheila', with 'missing' and 'corrupted' has_file status)
    result = runner.invoke(cli.cli, ["acq", "files", "x", "x"])
    assert result.exit_code == 0
    assert re.match(
        r".*Name +Size +Has +Wants\n"
        r"-+  -+  -+  -+\n"
        r"fred +512 +N +Y *\n"
        r"sheila +512 +X +M *\n$",
        result.output,
        re.DOTALL,
    )


def test_where(fixtures):
    """Test the 'acq where' command"""
    runner = CliRunner()

    # Check help output
    help_result = runner.invoke(cli.cli, ["acq", "where", "--help"])
    assert help_result.exit_code == 0
    assert "List locations of files that are in the ACQUISITION." in help_result.output

    # Fail when given a non-existent acquisition
    result = runner.invoke(cli.cli, ["acq", "where", "z"])
    assert result.exit_code == 1
    assert "No such acquisition: z" in result.output

    # Check regular case
    result = runner.invoke(cli.cli, ["acq", "where", "x"], catch_exceptions=False)
    assert result.exit_code == 0
    assert re.match(
        r"Storage node: x\n"
        r".*Name +Size +Has +Wants\n"
        r"-+  -+  -+  -+\n"
        r"fred +512 +N +Y *\n"
        r"sheila +512 +X +M *\n$",
        result.output,
        re.DOTALL,
    )

    # Now pretend node 'z' also has a copy of 'fred'
    z_node = st.StorageNode.get(name="z")
    fred_file = ar.ArchiveFile.get(name="fred")
    fred2_copy = ar.ArchiveFileCopy.create(
        file=fred_file, node=z_node, has_file="Y", wants_file="Y", size_b=123
    )
    result = runner.invoke(cli.cli, ["acq", "where", "x"], catch_exceptions=False)
    assert result.exit_code == 0
    assert re.match(
        r"Storage node: x\n"
        r".*Name +Size +Has +Wants\n"
        r"-+  -+  -+  -+\n"
        r"fred +512 +N +Y *\n"
        r"sheila +512 +X +M *\n\n"
        r"Storage node: z\n"
        r".*Name +Size +Has +Wants\n"
        r"-+  -+  -+  -+\n"
        r"fred +123 +Y +Y *\n$",
        result.output,
        re.DOTALL,
    )

    # Check when an acquisition does not have any files
    zoo_acq = ac.ArchiveAcq.create(name="zoo", type=ac.AcqType.get(name="zab"))
    result = runner.invoke(cli.cli, ["acq", "where", "zoo"], catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output == "No registered archive files.\n"

    # Check when there are no copies of an acquisition's files
    ac.ArchiveFile.create(name="boo", acq=zoo_acq, type=ac.FileType.get(name="spqr"))
    result = runner.invoke(cli.cli, ["acq", "where", "zoo"], catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output == "No registered archive files.\n"


def test_syncable(fixtures):
    """Test the 'acq syncable' command"""
    runner = CliRunner()

    # Check help output
    help_result = runner.invoke(cli.cli, ["acq", "syncable", "--help"])
    assert help_result.exit_code == 0
    assert "List all files that are in the ACQUISITION" in help_result.output

    # Fail when given a non-existent acquisition
    result = runner.invoke(cli.cli, ["acq", "syncable", "z", "x", "bar"])
    assert result.exit_code == 1
    assert "No such acquisition: z" in result.output

    # Fail when given a non-existent source node
    result = runner.invoke(cli.cli, ["acq", "syncable", "x", "doesnotexist", "bar"])
    assert result.exit_code == 1
    assert "No such storage node: doesnotexist" in result.output

    # Fail when given a non-existent target group node
    result = runner.invoke(cli.cli, ["acq", "syncable", "x", "x", "doesnotexist"])
    assert result.exit_code == 1
    assert "No such storage group: doesnotexist" in result.output

    # Check that initially there are no files in 'x' acq to copy from 'x' to 'bar'
    result = runner.invoke(cli.cli, ["acq", "syncable", "x", "x", "bar"])
    assert result.exit_code == 0
    assert "No files to copy from node 'x' to group 'bar'" in result.output

    # Now pretend node 'x' has a copy of 'fred', 1 GB in size
    fred_file = ar.ArchiveFile.get(name="fred")
    fred_file.size_b = 1073741824.0
    fred_copy = fred_file.copies[0]
    fred_copy.has_file = "Y"
    fred_copy.save()
    fred_file.save()

    # ...and a copy of 'sheila', 0 bytes in size
    sheila_copy = (
        ar.ArchiveFileCopy.select()
        .join(ac.ArchiveFile)
        .where(ac.ArchiveFile.name == "sheila")
        .get()
    )
    sheila_copy.has_file = "Y"
    sheila_copy.file.size_b = 0
    sheila_copy.save()
    sheila_copy.file.save()

    # And so 'fred' and 'sheila' should be syncable from 'x' to 'bar'
    result = runner.invoke(cli.cli, ["acq", "syncable", "x", "x", "bar"])
    assert result.exit_code == 0
    assert re.match(
        r".*Name +Size\n" r"-+  -+\n" r"fred +1073741824 *\n" r"sheila +0 *\n$",
        result.output,
        re.DOTALL,
    )

    # Now pretend node 'z' also has a copy of 'fred'
    z_node = st.StorageNode.get(name="z")
    fred2_copy = ar.ArchiveFileCopy.create(
        file=fred_file, node=z_node, has_file="Y", wants_file="Y", size_b=123
    )

    # And so only 'sheila' should be syncable from 'x' to 'bar'
    result = runner.invoke(cli.cli, ["acq", "syncable", "x", "x", "bar"])
    assert result.exit_code == 0
    assert re.match(
        r".*Name +Size\n" r"-+  -+\n" r"sheila +0 *\n$", result.output, re.DOTALL
    )
