"""Test CLI: alpenhorn node verify"""

import pytest

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


@pytest.fixture
def file_gamut(clidb):
    """Fixture to create the has_file/wants_file gamut of files.

    Yields node and a list of 3-tuples:
        (has_file, wants_file, ArchiveFileCopy)
    """
    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    acq = ArchiveAcq.create(name="Acq")
    files = []
    for has in ["Y", "M", "X", "N"]:
        for wants in ["Y", "M", "N"]:
            file = ArchiveFile.create(name="File" + has + wants, acq=acq, size_b=1234)
            ArchiveFileCopy.create(node=node, file=file, has_file=has, wants_file=wants)
            files.append((has, wants, file))

    return node, files


def test_no_node(clidb, cli):
    """Test clean with no node."""

    cli(1, ["node", "verify", "TEST"])


def test_check_force(clidb, cli):
    """Test --check --force fails."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="NODE", group=group, storage_type="F")

    cli(2, ["node", "verify", "NODE", "--check", "--force"])


def test_cancel_multistatus(clidb, cli):
    """Test --cancel with multiple statuses chosen."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="NODE", group=group, storage_type="F")

    cli(2, ["node", "verify", "NODE", "--cancel", "--corrupt", "--healthy"])
    cli(2, ["node", "verify", "NODE", "--cancel", "--corrupt", "--missing"])
    cli(2, ["node", "verify", "NODE", "--cancel", "--healthy", "--missing"])


def test_run(clidb, cli, file_gamut):
    """Test a simple verify."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE"], input="Y\n")

    # Verify should have run on 'XY', 'XM', 'NY'
    for item in files:
        if item[0] == "X" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "X" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "N" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    # i.e. we didn't update the existing has_file == 'M' copies
    assert "Updated 3 files" in result.output


def test_verify_corrupt(clidb, cli, file_gamut):
    """Test verify --corupt."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--corrupt"], input="Y\n")

    # Verify should have run on 'XY', 'XM'
    for item in files:
        if item[0] == "X" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "X" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 2 files" in result.output


def test_verify_missing(clidb, cli, file_gamut):
    """Test verify --missing."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--missing"], input="Y\n")

    # Verify should have run on 'NY' only
    for item in files:
        if item[0] == "N" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 1 file" in result.output


def test_verify_healthy(clidb, cli, file_gamut):
    """Test verify --healthy."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--healthy"], input="Y\n")

    # Verify should have run on 'YY', 'YM'
    for item in files:
        if item[0] == "Y" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "Y" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 2 files" in result.output


def test_corrupt_missing(clidb, cli, file_gamut):
    """Test explicit verify --corrupt --missing."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--corrupt", "--missing"], input="Y\n")

    # Verify should have run on 'XY', 'XM', 'NY'
    for item in files:
        if item[0] == "X" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "X" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "N" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    # i.e. we didn't update the existing has_file == 'M' copies
    assert "Updated 3 files" in result.output


def test_corrupt_healtyh(clidb, cli, file_gamut):
    """Test verify --corrupt --healthy."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--corrupt", "--healthy"], input="Y\n")

    # Verify should have run on 'YY', 'YM', 'XY', 'XM'
    for item in files:
        if item[0] == "Y" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "Y" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "X" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "X" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 4 files" in result.output


def test_healthy_missing(clidb, cli, file_gamut):
    """Test verify --healthy --missing."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--healthy", "--missing"], input="Y\n")

    # Verify should have run on 'YY', 'YM', 'NY'
    for item in files:
        if item[0] == "Y" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "Y" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "N" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 3 files" in result.output


def test_hmc(clidb, cli, file_gamut):
    """Test verify --healthy --missing --corrupt."""

    node, files = file_gamut

    result = cli(
        0,
        ["node", "verify", "NODE", "--healthy", "--missing", "--corrupt"],
        input="Y\n",
    )

    # Verify should have run on 'YY', 'YM', 'XY', 'XM', 'NY'
    for item in files:
        if item[0] == "Y" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "Y" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "X" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "X" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "N" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 5 files" in result.output


def test_all(clidb, cli, file_gamut):
    """Test verify --all.

    Should be the same as the previous --healthy --missing --corrupt test.
    """

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--all"], input="Y\n")

    # Verify should have run on 'YY', 'YM', 'XY', 'XM', 'NY'
    for item in files:
        if item[0] == "Y" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "Y" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "X" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "X" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        elif item[0] == "N" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "M"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 5 files" in result.output


def test_cancel(clidb, cli, file_gamut):
    """Test verify --cancel."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--cancel"], input="Y\n")

    # should have run on 'MY', 'MM', and set has_file to 'X'
    for item in files:
        if item[0] == "M" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "X"
        elif item[0] == "M" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "X"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 2 files" in result.output


def test_cancel_corrupt(clidb, cli, file_gamut):
    """Test verify --cancel --corrupt."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--cancel", "--corrupt"], input="Y\n")

    # should have run on 'MY', 'MM', and set has_file to 'X'
    for item in files:
        if item[0] == "M" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "X"
        elif item[0] == "M" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "X"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 2 files" in result.output


def test_cancel_missing(clidb, cli, file_gamut):
    """Test verify --cancel --healthy."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--cancel", "--healthy"], input="Y\n")

    # should have run on 'MY', 'MM', and set has_file to 'N'
    for item in files:
        if item[0] == "M" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "Y"
        elif item[0] == "M" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "Y"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 2 files" in result.output


def test_cancel_all(clidb, cli, file_gamut):
    """Test --cancel --all (--all should be ignored)."""

    node, files = file_gamut

    result = cli(0, ["node", "verify", "NODE", "--cancel", "--all"], input="Y\n")

    # should have run on 'MY', 'MM', and set has_file to 'X'
    for item in files:
        if item[0] == "M" and item[1] == "Y":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "X"
        elif item[0] == "M" and item[1] == "M":
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == "X"
        else:
            assert ArchiveFileCopy.get(node=node, file=item[2]).has_file == item[0]

    assert "Updated 2 files" in result.output


def test_verify_bad_acq(clidb, cli, file_gamut):
    """Test verify with bad --acq."""

    cli(1, ["node", "verify", "NODE", "--acq=BAD"])


def test_verify_acq(clidb, cli):
    """Test verify with some --acq constraints."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    acq = ArchiveAcq.create(name="Acq1")
    file1 = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(node=node, file=file1, has_file="X", wants_file="Y")
    acq = ArchiveAcq.create(name="Acq2")
    file2 = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(node=node, file=file2, has_file="X", wants_file="Y")
    acq = ArchiveAcq.create(name="Acq3")
    file3 = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(node=node, file=file3, has_file="X", wants_file="Y")

    cli(0, ["node", "verify", "NODE", "--acq=Acq1", "--acq=Acq2"], input="Y\n")

    # only 1 and 2 should be updated
    assert ArchiveFileCopy.get(node=node, file=file1).has_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file2).has_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file3).has_file == "X"


def test_file_list(clidb, cli, xfs):
    """Test verify with --file-list."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="F", root="/NODE")
    acq = ArchiveAcq.create(name="Acq")
    file1 = ArchiveFile.create(name="File1", acq=acq)
    ArchiveFileCopy.create(node=node, file=file1, has_file="X", wants_file="Y")
    file2 = ArchiveFile.create(name="File2", acq=acq)
    ArchiveFileCopy.create(node=node, file=file2, has_file="X", wants_file="Y")
    file3 = ArchiveFile.create(name="File3", acq=acq)
    ArchiveFileCopy.create(node=node, file=file3, has_file="X", wants_file="Y")

    xfs.create_file("/file_list", contents="Acq/File1\n/NODE/Acq/File3\n")

    cli(0, ["node", "verify", "NODE", "--file-list=/file_list"], input="Y\n")

    # only 1 and 3 should be updated
    assert ArchiveFileCopy.get(node=node, file=file1).has_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file2).has_file == "X"
    assert ArchiveFileCopy.get(node=node, file=file3).has_file == "M"
