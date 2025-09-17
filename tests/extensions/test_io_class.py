"""Test IOClassExtension."""

import pytest

from alpenhorn.extensions import IOClassExtension
from alpenhorn.io.default import DefaultGroupIO, DefaultNodeIO


def test_no_classes():
    """Node or group class (or both) must be provided."""

    with pytest.raises(ValueError):
        IOClassExtension("Test", "1", io_class_name="Class")

    # But these are fine
    IOClassExtension("Test", "1", io_class_name="Class", node_class=DefaultNodeIO)
    IOClassExtension("Test", "1", io_class_name="Class", group_class=DefaultGroupIO)
    IOClassExtension(
        "Test",
        "1",
        io_class_name="Class",
        node_class=DefaultNodeIO,
        group_class=DefaultGroupIO,
    )


def test_bad_node_class():
    """node_class, if given, must derive from base I/O classes."""

    with pytest.raises(TypeError):
        IOClassExtension("Test", "1", io_class_name="Class", node_class=1)

    with pytest.raises(TypeError):
        IOClassExtension(
            "Test", "1", io_class_name="Class", node_class=IOClassExtension
        )

    with pytest.raises(TypeError):
        IOClassExtension("Test", "1", io_class_name="Class", node_class=DefaultGroupIO)

    # But these are fine
    IOClassExtension("Test", "1", io_class_name="Class", node_class=DefaultNodeIO)
    IOClassExtension(
        "Test", "1", io_class_name="Class", node_class=None, group_class=DefaultGroupIO
    )


def test_bad_group_class():
    """group_class, if given, must derive from base I/O classes."""

    with pytest.raises(TypeError):
        IOClassExtension("Test", "1", io_class_name="Class", group_class=1)

    with pytest.raises(TypeError):
        IOClassExtension(
            "Test", "1", io_class_name="Class", group_class=IOClassExtension
        )

    with pytest.raises(TypeError):
        IOClassExtension("Test", "1", io_class_name="Class", group_class=DefaultNodeIO)

    # But these are fine
    IOClassExtension("Test", "1", io_class_name="Class", group_class=DefaultGroupIO)
    IOClassExtension(
        "Test", "1", io_class_name="Class", node_class=DefaultNodeIO, group_class=None
    )


def test_io_class_name():
    """io_class_name cannot be empty."""

    with pytest.raises(TypeError):
        IOClassExtension("Test", "1", io_class_name=None, node_class=DefaultNodeIO)

    with pytest.raises(ValueError):
        IOClassExtension("Test", "1", io_class_name="", node_class=DefaultNodeIO)

    with pytest.raises(ValueError):
        IOClassExtension("Test", "1", io_class_name=" ", node_class=DefaultNodeIO)

    # But this is fine
    IOClassExtension("Test", "1", io_class_name="Class", node_class=DefaultNodeIO)
