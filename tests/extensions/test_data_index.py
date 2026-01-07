"""Test DataIndexExtension."""

import pytest

from alpenhorn.db import base_model
from alpenhorn.extensions import DataIndexExtension


class TableTest(base_model):
    pass


def test_bad_component():
    """Check that invalid component names aren't accepted."""

    # Must be a string
    with pytest.raises(TypeError):
        DataIndexExtension(
            "Test", "1", component=pytest, schema_version=1, tables=[TableTest]
        )

    # Can't be just whitespace
    with pytest.raises(ValueError):
        DataIndexExtension(
            "Test", "1", component=" ", schema_version=1, tables=[TableTest]
        )

    # Can't be "alpenhorn"
    with pytest.raises(ValueError):
        DataIndexExtension(
            "Test", "1", component="alpenhorn", schema_version=1, tables=[TableTest]
        )

    # This is fine
    DataIndexExtension(
        "Test", "1", component="test", schema_version=1, tables=[TableTest]
    )


def test_bad_schema_version():
    """schema_version must be a positive integer."""

    with pytest.raises(ValueError):
        DataIndexExtension(
            "Test", "1", component="test", schema_version=0, tables=[TableTest]
        )

    with pytest.raises(ValueError):
        DataIndexExtension(
            "Test", "1", component="test", schema_version=-1, tables=[TableTest]
        )

    # float is not allowed
    with pytest.raises(TypeError):
        DataIndexExtension(
            "Test", "1", component="test", schema_version=1.2, tables=[TableTest]
        )

    # String coersions that shouldn't work
    with pytest.raises(TypeError):
        DataIndexExtension(
            "Test", "1", component="test", schema_version="1a", tables=[TableTest]
        )
    with pytest.raises(TypeError):
        DataIndexExtension(
            "Test", "1", component="test", schema_version="1.2", tables=[TableTest]
        )
    with pytest.raises(TypeError):
        DataIndexExtension(
            "Test", "1", component="test", schema_version="1.2.3", tables=[TableTest]
        )

    # But this is fine
    DataIndexExtension(
        "Test", "1", component="test", schema_version="1", tables=[TableTest]
    )


def test_no_tables():
    """At least one table must be given."""

    with pytest.raises(ValueError):
        DataIndexExtension("Test", "1", component="test", schema_version=1, tables=[])

    DataIndexExtension(
        "Test", "1", component="test", schema_version=1, tables=[TableTest]
    )


def test_table_nonlist(dbproxy):
    """Tables must be a list (or at least iterable)."""

    with pytest.raises(TypeError):
        DataIndexExtension(
            "Test", "1", component="test", schema_version=1, tables=TableTest
        )

    # But these are all fine
    DataIndexExtension(
        "Test", "1", component="test", schema_version=1, tables=[TableTest]
    )
    DataIndexExtension(
        "Test", "1", component="test", schema_version=1, tables={TableTest}
    )
    DataIndexExtension(
        "Test", "1", component="test", schema_version=1, tables=(TableTest,)
    )


def test_non_tables():
    """All tables must derive from base_model."""

    with pytest.raises(ValueError):
        DataIndexExtension(
            "Test", "1", component="test", schema_version=1, tables=[None]
        )

    DataIndexExtension(
        "Test", "1", component="test", schema_version=1, tables=[TableTest]
    )
