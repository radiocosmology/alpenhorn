"""Test CLI: alpenhorn node clean"""

from datetime import timedelta

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
    utcnow,
)


def test_no_node(clidb, cli):
    """Test clean with no node."""

    cli(1, ["node", "clean", "TEST"])


def test_cancel_now(clidb, cli):
    """Test --cancel --now fails."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="NODE", group=group, storage_type="F")

    cli(2, ["node", "clean", "NODE", "--cancel", "--now"])


def test_cancel_size(clidb, cli):
    """Test --cancel --size fails."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="NODE", group=group, storage_type="F")

    cli(2, ["node", "clean", "NODE", "--cancel", "--size=3"])


def test_check_force(clidb, cli):
    """Test --check --force fails."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="NODE", group=group, storage_type="F")

    cli(2, ["node", "clean", "NODE", "--check", "--force"])


def test_bad_days(clidb, cli):
    """Test non-positive --days."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="NODE", group=group, storage_type="F")

    cli(2, ["node", "clean", "NODE", "--days=0"])
    cli(2, ["node", "clean", "NODE", "--days=-1"])


def test_bad_size(clidb, cli):
    """Test non-positive --size."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="NODE", group=group, storage_type="F")

    cli(2, ["node", "clean", "NODE", "--size=0"])
    cli(2, ["node", "clean", "NODE", "--size=-1"])


def test_run(clidb, cli):
    """Test a simple clean."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    acq = ArchiveAcq.create(name="Acq")
    fileYY = ArchiveFile.create(name="FileYY", acq=acq, size_b=2345)
    ArchiveFileCopy.create(node=node, file=fileYY, has_file="Y", wants_file="Y")
    fileYN = ArchiveFile.create(name="FileYN", acq=acq, size_b=3456)
    ArchiveFileCopy.create(node=node, file=fileYN, has_file="Y", wants_file="N")
    fileMY = ArchiveFile.create(name="FileMY", acq=acq, size_b=3456)
    ArchiveFileCopy.create(node=node, file=fileMY, has_file="M", wants_file="Y")
    fileXY = ArchiveFile.create(name="FileXY", acq=acq, size_b=3456)
    ArchiveFileCopy.create(node=node, file=fileXY, has_file="X", wants_file="Y")

    cli(0, ["node", "clean", "NODE"], input="Y\n")

    # Clean should have run, but only on the 'YY' file
    assert ArchiveFileCopy.get(node=node, file=fileYY).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=fileYN).wants_file == "N"
    assert ArchiveFileCopy.get(node=node, file=fileMY).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=fileXY).wants_file == "Y"


def test_now(clidb, cli):
    """Test clean --now."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    acq = ArchiveAcq.create(name="Acq")
    fileY = ArchiveFile.create(name="FileY", acq=acq, size_b=4567)
    ArchiveFileCopy.create(node=node, file=fileY, has_file="Y", wants_file="Y")
    fileM = ArchiveFile.create(name="FileM", acq=acq, size_b=5678)
    ArchiveFileCopy.create(node=node, file=fileM, has_file="Y", wants_file="M")

    cli(0, ["node", "clean", "NODE", "--now"], input="Y\n")

    # Clean should have run
    assert ArchiveFileCopy.get(node=node, file=fileY).wants_file == "N"


def test_cancel(clidb, cli):
    """Test clean --cancel."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    acq = ArchiveAcq.create(name="Acq")
    fileM = ArchiveFile.create(name="FileM", acq=acq, size_b=6789)
    ArchiveFileCopy.create(node=node, file=fileM, has_file="Y", wants_file="M")
    fileN = ArchiveFile.create(name="FileN", acq=acq, size_b=1234)
    ArchiveFileCopy.create(node=node, file=fileN, has_file="Y", wants_file="N")

    cli(0, ["node", "clean", "NODE", "--cancel"], input="Y\n")

    # Clean should have run
    assert ArchiveFileCopy.get(node=node, file=fileM).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=fileN).wants_file == "Y"


def test_check(clidb, cli):
    """Test clean --check."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=2345)
    ArchiveFileCopy.create(node=node, file=file, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--check"])

    # Clean should not have run
    assert ArchiveFileCopy.get(node=node, file=file).wants_file == "Y"


def test_force(clidb, cli):
    """Test clean --force."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=3456)
    ArchiveFileCopy.create(node=node, file=file, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--force"])

    # Clean should have run
    assert ArchiveFileCopy.get(node=node, file=file).wants_file == "M"


def test_archive(clidb, cli):
    """Test clean on an archive node."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="A")
    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=4567)
    ArchiveFileCopy.create(node=node, file=file, has_file="Y", wants_file="Y")

    cli(1, ["node", "clean", "NODE"], input="Y\n")

    # Clean should not have run
    assert ArchiveFileCopy.get(node=node, file=file).wants_file == "Y"


def test_archive_ok(clidb, cli):
    """Test clean on an archive node with --archive-ok."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="NODE", group=group, storage_type="A")
    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=5678)
    ArchiveFileCopy.create(node=node, file=file, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--archive-ok"], input="Y\n")

    # Clean should have run
    assert ArchiveFileCopy.get(node=node, file=file).wants_file == "M"


def test_bad_target(clidb, cli):
    """Test clean with a non-exsitent target."""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=6789)
    ArchiveFileCopy.create(node=node, file=file, has_file="Y", wants_file="Y")

    cli(1, ["node", "clean", "NODE", "--force", "--target=Target"])

    # File wasn't cleaned
    assert ArchiveFileCopy.get(node=node, file=file).wants_file == "Y"


def test_target(clidb, cli):
    """Test clean with some targets."""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    target1 = StorageGroup.create(name="Target1")
    tnode1 = StorageNode.create(name="TNode1", group=target1)
    target2 = StorageGroup.create(name="Target2")
    tnode2 = StorageNode.create(name="TNode2", group=target2)
    target3 = StorageGroup.create(name="Target3")
    tnode3 = StorageNode.create(name="TNode3", group=target3)

    acq = ArchiveAcq.create(name="Acq")

    # File1 is in Target1
    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(node=node, file=file1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(node=tnode1, file=file1, has_file="Y", wants_file="Y")

    # File2 is in Target2
    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=2345)
    ArchiveFileCopy.create(node=node, file=file2, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(node=tnode2, file=file2, has_file="Y", wants_file="Y")

    # File3 is in Target1 and Target2
    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=3456)
    ArchiveFileCopy.create(node=node, file=file3, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(node=tnode1, file=file3, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(node=tnode2, file=file3, has_file="Y", wants_file="Y")

    # File4 is in Target3, but that's not one of the ones we're using as a target
    file4 = ArchiveFile.create(name="File4", acq=acq, size_b=4567)
    ArchiveFileCopy.create(node=node, file=file4, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(node=tnode3, file=file4, has_file="Y", wants_file="Y")

    # File5 is in no target
    file5 = ArchiveFile.create(name="File5", acq=acq, size_b=5678)
    ArchiveFileCopy.create(node=node, file=file5, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--force", "--target=Target1", "--target=Target2"])

    # Only File3 was cleaned
    assert ArchiveFileCopy.get(node=node, file=file1).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=file2).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=file3).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file4).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=file5).wants_file == "Y"


def test_empty_target(clidb, cli):
    """Test clean with no files in the target."""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    target1 = StorageGroup.create(name="Target1")
    tnode1 = StorageNode.create(name="TNode1", group=target1)
    target2 = StorageGroup.create(name="Target2")
    tnode2 = StorageNode.create(name="TNode2", group=target2)
    target3 = StorageGroup.create(name="Target3")
    tnode3 = StorageNode.create(name="TNode3", group=target3)

    acq = ArchiveAcq.create(name="Acq")

    # File1 is in Target1
    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1234)
    ArchiveFileCopy.create(node=node, file=file1, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(node=tnode1, file=file1, has_file="Y", wants_file="Y")

    # File2 is in Target2
    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=2345)
    ArchiveFileCopy.create(node=node, file=file2, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(node=tnode2, file=file2, has_file="Y", wants_file="Y")

    # File3 is in Target3, but that's not one of the ones we're using as a target
    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=3456)
    ArchiveFileCopy.create(node=node, file=file3, has_file="Y", wants_file="Y")
    ArchiveFileCopy.create(node=tnode3, file=file3, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--force", "--target=Target1", "--target=Target2"])

    # No files were cleaned
    assert ArchiveFileCopy.get(node=node, file=file1).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=file2).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=file3).wants_file == "Y"


def test_bad_acq(clidb, cli):
    """Test clean with a non-exsitent acq."""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")
    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq, size_b=4567)
    ArchiveFileCopy.create(node=node, file=file, has_file="Y", wants_file="Y")

    cli(1, ["node", "clean", "NODE", "--force", "--acq=BadAcq"])

    # File wasn't cleaned
    assert ArchiveFileCopy.get(node=node, file=file).wants_file == "Y"


def test_acq(clidb, cli):
    """Test clean with some acq."""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")

    # File1 is in Acq1
    acq1 = ArchiveAcq.create(name="Acq1")
    file1 = ArchiveFile.create(name="File", acq=acq1, size_b=5678)
    ArchiveFileCopy.create(node=node, file=file1, has_file="Y", wants_file="Y")

    # File2 is in Acq2
    acq2 = ArchiveAcq.create(name="Acq2")
    file2 = ArchiveFile.create(name="File", acq=acq2, size_b=6789)
    ArchiveFileCopy.create(node=node, file=file2, has_file="Y", wants_file="Y")

    # File3 is in Acq3
    acq3 = ArchiveAcq.create(name="Acq3")
    file3 = ArchiveFile.create(name="File", acq=acq3, size_b=1234)
    ArchiveFileCopy.create(node=node, file=file3, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--force", "--acq=Acq1", "--acq=Acq2"])

    # Files 1 and 2 were cleaned
    assert ArchiveFileCopy.get(node=node, file=file1).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file2).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file3).wants_file == "Y"


def test_empty_acq(clidb, cli):
    """Test clean with no files in target acq."""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")

    # File1 is in Acq1
    acq1 = ArchiveAcq.create(name="Acq1")
    file1 = ArchiveFile.create(name="File", acq=acq1, size_b=2345)
    ArchiveFileCopy.create(node=node, file=file1, has_file="Y", wants_file="Y")

    # File2 is in Acq2
    acq2 = ArchiveAcq.create(name="Acq2")
    file2 = ArchiveFile.create(name="File", acq=acq2, size_b=3456)
    ArchiveFileCopy.create(node=node, file=file2, has_file="Y", wants_file="Y")

    # Acq3 is empty
    ArchiveAcq.create(name="Acq3")

    cli(0, ["node", "clean", "NODE", "--force", "--acq=Acq3"])

    # No files were cleaned
    assert ArchiveFileCopy.get(node=node, file=file1).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=file2).wants_file == "Y"


def test_days(clidb, cli):
    """Test clean with --days"""

    now = utcnow()

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(
        name="File1", acq=acq, size_b=4567, registered=now - timedelta(days=-5)
    )
    ArchiveFileCopy.create(node=node, file=file1, has_file="Y", wants_file="Y")
    file2 = ArchiveFile.create(
        name="File2", acq=acq, size_b=5678, registered=now - timedelta(days=-4)
    )
    ArchiveFileCopy.create(node=node, file=file2, has_file="Y", wants_file="Y")
    file3 = ArchiveFile.create(
        name="File3", acq=acq, size_b=6789, registered=now - timedelta(days=-2)
    )
    ArchiveFileCopy.create(node=node, file=file3, has_file="Y", wants_file="Y")
    file4 = ArchiveFile.create(
        name="File4", acq=acq, size_b=1234, registered=now - timedelta(days=-1)
    )
    ArchiveFileCopy.create(node=node, file=file4, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--force", "--days=3"])

    # Only files 1 and 2 were cleaned
    assert ArchiveFileCopy.get(node=node, file=file1).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file2).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file3).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=file4).wants_file == "Y"


def test_size(clidb, cli):
    """Test clean with --size"""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1 * 2**30)
    ArchiveFileCopy.create(node=node, file=file1, has_file="Y", wants_file="Y")
    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=2 * 2**30)
    ArchiveFileCopy.create(node=node, file=file2, has_file="Y", wants_file="Y")
    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=3 * 2**30)
    ArchiveFileCopy.create(node=node, file=file3, has_file="Y", wants_file="Y")
    file4 = ArchiveFile.create(name="File4", acq=acq, size_b=4 * 2**30)
    ArchiveFileCopy.create(node=node, file=file4, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--force", "--size=5."])

    # Only files 1, 2, 3 were cleaned (6 GiB)
    assert ArchiveFileCopy.get(node=node, file=file1).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file2).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file3).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file4).wants_file == "Y"


def test_size_part(clidb, cli):
    """Test clean with --size partially satisfied"""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1 * 2**30)
    ArchiveFileCopy.create(node=node, file=file1, has_file="Y", wants_file="N")
    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=2 * 2**30)
    ArchiveFileCopy.create(node=node, file=file2, has_file="Y", wants_file="Y")
    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=3 * 2**30)
    ArchiveFileCopy.create(node=node, file=file3, has_file="Y", wants_file="Y")
    file4 = ArchiveFile.create(name="File4", acq=acq, size_b=4 * 2**30)
    ArchiveFileCopy.create(node=node, file=file4, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--force", "--size=5."])

    # Only files 2, 3 were cleaned (6 GiB)
    assert ArchiveFileCopy.get(node=node, file=file1).wants_file == "N"
    assert ArchiveFileCopy.get(node=node, file=file2).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file3).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file4).wants_file == "Y"


def test_size_done(clidb, cli):
    """Test clean with --size already satisfied"""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(name="File1", acq=acq, size_b=1 * 2**30)
    ArchiveFileCopy.create(node=node, file=file1, has_file="Y", wants_file="N")
    file2 = ArchiveFile.create(name="File2", acq=acq, size_b=2 * 2**30)
    ArchiveFileCopy.create(node=node, file=file2, has_file="Y", wants_file="N")
    file3 = ArchiveFile.create(name="File3", acq=acq, size_b=3 * 2**30)
    ArchiveFileCopy.create(node=node, file=file3, has_file="Y", wants_file="Y")
    file4 = ArchiveFile.create(name="File4", acq=acq, size_b=4 * 2**30)
    ArchiveFileCopy.create(node=node, file=file4, has_file="Y", wants_file="Y")

    cli(0, ["node", "clean", "NODE", "--force", "--size=2."])

    # No additional files were cleaned
    assert ArchiveFileCopy.get(node=node, file=file1).wants_file == "N"
    assert ArchiveFileCopy.get(node=node, file=file2).wants_file == "N"
    assert ArchiveFileCopy.get(node=node, file=file3).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=file4).wants_file == "Y"


def test_include_bad(clidb, cli):
    """Test clean with --include-bad"""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F")

    acq = ArchiveAcq.create(name="Acq")

    fileY = ArchiveFile.create(name="FileY", acq=acq)
    ArchiveFileCopy.create(node=node, file=fileY, has_file="Y", wants_file="N")
    fileM = ArchiveFile.create(name="FileM", acq=acq)
    ArchiveFileCopy.create(node=node, file=fileM, has_file="M", wants_file="N")
    fileX = ArchiveFile.create(name="FileX", acq=acq)
    ArchiveFileCopy.create(node=node, file=fileX, has_file="X", wants_file="N")

    cli(0, ["node", "clean", "NODE", "--force", "--include-bad"])

    # All the files are cleaned
    assert ArchiveFileCopy.get(node=node, file=fileY).wants_file == "N"
    assert ArchiveFileCopy.get(node=node, file=fileM).wants_file == "N"
    assert ArchiveFileCopy.get(node=node, file=fileX).wants_file == "N"


def test_file_list(clidb, cli, xfs):
    """Test clean with --file-list"""

    group = StorageGroup.create(name="Group1")
    node = StorageNode.create(name="NODE", group=group, storage_type="F", root="/NODE")

    acq = ArchiveAcq.create(name="Acq")

    file1 = ArchiveFile.create(name="File1", acq=acq)
    ArchiveFileCopy.create(node=node, file=file1, has_file="Y", wants_file="Y")
    file2 = ArchiveFile.create(name="File2", acq=acq)
    ArchiveFileCopy.create(node=node, file=file2, has_file="Y", wants_file="Y")
    file3 = ArchiveFile.create(name="File3", acq=acq)
    ArchiveFileCopy.create(node=node, file=file3, has_file="Y", wants_file="Y")
    file4 = ArchiveFile.create(name="File4", acq=acq)
    ArchiveFileCopy.create(node=node, file=file4, has_file="Y", wants_file="Y")

    xfs.create_file("/file_list", contents="Acq/File1\n\n# Comment\n/NODE/Acq/File3\n")

    cli(0, ["node", "clean", "NODE", "--force", "--file-list=/file_list"])

    # Only File1 and File3 were cleaned
    assert ArchiveFileCopy.get(node=node, file=file1).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file2).wants_file == "Y"
    assert ArchiveFileCopy.get(node=node, file=file3).wants_file == "M"
    assert ArchiveFileCopy.get(node=node, file=file4).wants_file == "Y"
