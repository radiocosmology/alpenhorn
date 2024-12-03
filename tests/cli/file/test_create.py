"""Test CLI: alpenhorn file create"""

from os import getcwd

from alpenhorn.db import ArchiveAcq, ArchiveFile, utcnow


def test_no_data(clidb, cli):
    """Test providing not all data, in various ways."""

    ArchiveAcq.create(name="Acq")

    cli(2, ["file", "create", "name", "Acq"])
    cli(2, ["file", "create", "name", "Acq", "--md5=0123456789ABCDEF0123456789ABCDEF"])
    cli(2, ["file", "create", "name", "Acq", "--size=3"])


def test_bad_name(clidb, cli):
    """Test an invalid file name."""

    ArchiveAcq.create(name="Acq")

    cli(
        1,
        [
            "file",
            "create",
            "name/../name",
            "Acq",
            "--md5=0123456789ABCDEF0123456789ABCDEF",
            "--size=3",
        ],
    )


def test_negative_size(clidb, cli):
    """Test a negative size."""

    ArchiveAcq.create(name="Acq")

    cli(
        1,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--md5=0123456789ABCDEF0123456789ABCDEF",
            "--size=-3",
        ],
    )


def test_bad_md5(clidb, cli):
    """Test bad --md5 values."""

    ArchiveAcq.create(name="Acq")

    cli(
        1,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--md5=123456789ABCDEF0123456789ABCDEF",
            "--size=3",
        ],
    )
    cli(
        1,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--md5=Q123456789ABCDEF0123456789ABCDEF",
            "--size=3",
        ],
    )
    cli(
        1,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--md5=F0123456789ABCDEF0123456789ABCDEF",
            "--size=3",
        ],
    )


def test_prefix_no_scan(clidb, cli):
    """Test --prefix without --from-file."""

    ArchiveAcq.create(name="Acq")

    cli(
        2,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--prefix=.",
            "--md5=0123456789ABCDEF0123456789ABCDEF",
            "--size=3",
        ],
    )


def test_data_clash(clidb, cli):
    """Test scanning and providing data together."""

    ArchiveAcq.create(name="Acq")

    cli(
        2,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--from-file",
            "--md5=0123456789ABCDEF0123456789ABCDEF",
            "--size=3",
        ],
    )
    cli(
        2,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--from-file",
            "--md5=0123456789ABCDEF0123456789ABCDEF",
        ],
    )
    cli(2, ["file", "create", "name", "Acq", "--from-file", "--size=3"])


def test_no_acq(clidb, cli):
    """Test with non-existent acq."""

    cli(
        1,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--md5=0123456789ABCDEF0123456789ABCDEF",
            "--size=3",
        ],
    )


def test_exists(clidb, cli):
    """Test with exising file."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="name", acq=acq)

    cli(
        1,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--md5=0123456789ABCDEF0123456789ABCDEF",
            "--size=3",
        ],
    )


def test_with_data(clidb, cli):
    """Test creation with data provided."""

    before = utcnow().replace(microsecond=0)

    acq = ArchiveAcq.create(name="Acq")

    cli(
        0,
        [
            "file",
            "create",
            "name",
            "Acq",
            "--md5=0123456789ABCDEF0123456789ABCDEF",
            "--size=3",
        ],
    )

    file = ArchiveFile.get(name="name")
    assert file.acq == acq
    assert file.md5sum.upper() == "0123456789ABCDEF0123456789ABCDEF"
    assert file.size_b == 3
    assert file.registered >= before


def test_file_not_found(clidb, cli, xfs):
    """Test --from-file with missing file."""

    ArchiveAcq.create(name="Acq")

    cli(1, ["file", "create", "name", "Acq", "--from-file"])


def test_file_not_file(clidb, cli, xfs):
    """Test --from-file with non-file."""

    xfs.create_dir(getcwd() + "/Acq/name")

    ArchiveAcq.create(name="Acq")

    cli(1, ["file", "create", "name", "Acq", "--from-file"])


def test_file_access(clidb, cli, xfs):
    """Test --from-file with unreadable file."""

    xfs.create_file(getcwd() + "/Acq/name", st_mode=0)

    ArchiveAcq.create(name="Acq")

    cli(1, ["file", "create", "name", "Acq", "--from-file"])


def test_from_file(clidb, cli, xfs):
    """Test creating --from-file."""

    before = utcnow().replace(microsecond=0)

    xfs.create_file(getcwd() + "/Acq/name", contents="contents")

    acq = ArchiveAcq.create(name="Acq")

    cli(0, ["file", "create", "name", "Acq", "--from-file"])

    file = ArchiveFile.get(name="name")
    assert file.acq == acq
    assert file.md5sum.lower() == "98bf7d8c15784f0a3d63204441e1e2aa"
    assert file.size_b == len("contents")
    assert file.registered >= before


def test_from_file_relprefix(clidb, cli, xfs):
    """Test creating --from-file with relative --prefix."""

    before = utcnow().replace(microsecond=0)

    xfs.create_file(getcwd() + "/prefix/Acq/name", contents="contents")

    acq = ArchiveAcq.create(name="Acq")

    cli(0, ["file", "create", "name", "Acq", "--from-file", "--prefix=prefix"])

    file = ArchiveFile.get(name="name")
    assert file.acq == acq
    assert file.md5sum.lower() == "98bf7d8c15784f0a3d63204441e1e2aa"
    assert file.size_b == len("contents")
    assert file.registered >= before


def test_from_file_absprefix(clidb, cli, xfs):
    """Test creating --from-file with absolute --prefix."""

    before = utcnow().replace(microsecond=0)

    xfs.create_file("/prefix/Acq/name", contents="contents")

    acq = ArchiveAcq.create(name="Acq")

    cli(0, ["file", "create", "name", "Acq", "--from-file", "--prefix=/prefix"])

    file = ArchiveFile.get(name="name")
    assert file.acq == acq
    assert file.md5sum.lower() == "98bf7d8c15784f0a3d63204441e1e2aa"
    assert file.size_b == len("contents")
    assert file.registered >= before
