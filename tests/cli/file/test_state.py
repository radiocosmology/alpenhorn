"""Test CLI: alpenhorn file create"""

from alpenhorn.db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    StorageGroup,
    StorageNode,
)


def test_bad_file(clidb, cli):
    """Test bad file."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["file", "state", "MIS/SING", "Node"])


def test_bad_node(clidb, cli):
    """Test bad node."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(1, ["file", "state", "Acq/File", "Node"])


def test_get(clidb, cli):
    """Test getting state."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    # With no ArchiveFileCopy record
    result = cli(0, ["file", "state", "Acq/File", "Node"])
    assert result.output == "Absent\n"

    copy = ArchiveFileCopy.create(file=file, node=node)

    states = {
        # wants_file = "Y"
        "YY": "Healthy",
        "MY": "Suspect",
        "XY": "Corrupt",
        "NY": "Missing",
        # wants_file = "M"
        "YM": "Healthy Removable",
        "MM": "Suspect Removable",
        "XM": "Corrupt Removable",
        "NM": "Absent",
        # wants_file = "N"
        "YN": "Healthy Released",
        "MN": "Suspect Released",
        "XN": "Corrupt Released",
        "NN": "Absent",
    }

    # Run through all possibilities
    for ready in [True, False]:
        for has in ["Y", "M", "X", "N"]:
            for wants in ["Y", "M", "N"]:
                copy.has_file = has
                copy.wants_file = wants
                copy.ready = ready
                copy.save()

                result = cli(0, ["file", "state", "Acq/File", "Node"])
                assert result.output == states[has + wants] + (
                    " Ready\n" if ready and has != "N" else "\n"
                )


def test_set_bad_file(clidb, cli):
    """Test bad file during set."""

    group = StorageGroup.create(name="Group")
    StorageNode.create(name="Node", group=group)

    cli(1, ["file", "state", "MIS/SING", "Node", "--ready"])


def test_set_bad_node(clidb, cli):
    """Test bad node during set."""

    acq = ArchiveAcq.create(name="Acq")
    ArchiveFile.create(name="File", acq=acq)

    cli(1, ["file", "state", "Acq/File", "Node", "--ready"])


def test_ready_unready(clidb, cli):
    """Can't use both --ready and --unready."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file, node=node, has_file="Y", wants_file="Y")

    cli(2, ["file", "state", "Acq/File", "Node", "--ready", "--unready"])


def test_bad_state(clidb, cli):
    """Test bad --set value."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)
    ArchiveFileCopy.create(file=file, node=node, has_file="Y", wants_file="Y")

    cli(2, ["file", "state", "Acq/File", "Node", "--set=OTHER"])


def test_set(clidb, cli):
    """Test --set."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    ArchiveFileCopy.create(file=file, node=node)

    # Helper to test one case
    def _test(pre_has, pre_wants, state, has, wants):
        copy = ArchiveFileCopy.get(id=1)
        copy.has_file = pre_has
        copy.wants_file = pre_wants
        copy.save()

        cli(0, ["file", "state", "Acq/File", "Node", "--set", state])

        copy = ArchiveFileCopy.get(id=1)
        assert copy.has_file == has
        assert copy.wants_file == wants

    # Run through all the possibilities.  For some states there are two
    # tests because those particular states leave wants_file=='M' alone, if found.
    _test("N", "N", "Healthy", "Y", "Y")
    _test("X", "M", "Healthy", "Y", "M")
    _test("N", "N", "Suspect", "M", "Y")
    _test("X", "M", "Suspect", "M", "M")
    _test("N", "N", "Corrupt", "X", "Y")
    _test("X", "M", "Corrupt", "X", "M")
    _test("Y", "M", "Missing", "N", "Y")
    _test("Y", "Y", "Absent", "N", "N")


def test_creation(clidb, cli):
    """Test record creation with --set"""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    # Doesn't create a record
    cli(0, ["file", "state", "Acq/File", "Node", "--set=Absent"])

    assert ArchiveFileCopy.select().count() == 0

    cli(0, ["file", "state", "Acq/File", "Node", "--set=Suspect"])

    copy = ArchiveFileCopy.get(id=1)
    assert copy.file == file
    assert copy.node == node
    assert copy.has_file == "M"
    assert copy.wants_file == "Y"
    assert not copy.ready


def test_ready(clidb, cli):
    """Test --ready."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    # Doesn't work: bad --set state
    cli(1, ["file", "state", "Acq/File", "Node", "--ready", "--set=Missing"])

    # Doesn't work: no existing record
    cli(1, ["file", "state", "Acq/File", "Node", "--ready"])

    ArchiveFileCopy.create(file=file, node=node, has_file="N", wants_file="N")

    # Doesn't work: current state is not healthy
    cli(1, ["file", "state", "Acq/File", "Node", "--ready"])

    # But this works
    cli(0, ["file", "state", "Acq/File", "Node", "--ready", "--set=Healthy"])

    copy = ArchiveFileCopy.get(id=1)
    assert copy.has_file == "Y"
    assert copy.wants_file == "Y"
    assert copy.ready


def test_unready(clidb, cli):
    """Test --ready."""

    group = StorageGroup.create(name="Group")
    node = StorageNode.create(name="Node", group=group)

    acq = ArchiveAcq.create(name="Acq")
    file = ArchiveFile.create(name="File", acq=acq)

    # Fake-works: a copy with no record is automatically not ready,
    # so this is a successful NOP
    cli(0, ["file", "state", "Acq/File", "Node", "--unready"])

    # No record was created
    assert ArchiveFileCopy.select().count() == 0

    ArchiveFileCopy.create(
        file=file, node=node, has_file="N", wants_file="N", ready=True
    )

    # This is fine
    cli(0, ["file", "state", "Acq/File", "Node", "--unready"])

    copy = ArchiveFileCopy.get(id=1)
    assert copy.has_file == "N"
    assert copy.wants_file == "N"
    assert not copy.ready
    copy.ready = True
    copy.save()

    # This is fine, too
    cli(0, ["file", "state", "Acq/File", "Node", "--unready", "--set=Corrupt"])

    copy = ArchiveFileCopy.get(id=1)
    assert copy.has_file == "X"
    assert copy.wants_file == "Y"
    assert not copy.ready
