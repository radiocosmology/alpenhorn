"""test alpenhorn.db.data_index."""

import click
import peewee as pw
import pytest

from alpenhorn.db.data_index import DataIndexVersion, current_version, schema_version


def test_data_index_version(dbproxy):
    """Test that the DataIndexVersion table can be created."""
    dbproxy.create_tables([DataIndexVersion])
    assert set(dbproxy.get_tables()) == {"dataindexversion"}


def test_data_index_version_model(dbproxy):
    """Test DataIndexVersion table model."""

    dbproxy.create_tables([DataIndexVersion])

    DataIndexVersion.create(component="test", version=3)

    # component is unique
    with pytest.raises(pw.IntegrityError):
        DataIndexVersion.create(component="test", version=4)

    # component cannot be NULL
    with pytest.raises(pw.IntegrityError):
        DataIndexVersion.create(component=None, version=5)

    # version cannot be NULL
    with pytest.raises(pw.IntegrityError):
        DataIndexVersion.create(component="test2", version=None)

    # Check DB
    assert DataIndexVersion.select().where(
        DataIndexVersion.component == "test"
    ).dicts().get() == {
        "id": 1,
        "component": "test",
        "version": 3,
    }


def test_schema_version(dbtables):
    """schema_version() should return the version."""

    assert schema_version() == current_version


def test_schema_version_check_ok(dbtables):
    """Test successful check in schema_version."""

    # Parameters other than check are ignored
    assert (
        schema_version(
            check=True, component="MISSING", component_version=current_version + 1
        )
        == current_version
    )


def test_schema_version_check_bad(dbtables):
    """Test unsuccessful check in schema_version."""

    # Change version
    DataIndexVersion.update(version=current_version + 1).where(
        DataIndexVersion.component == "alpenhorn"
    ).execute()

    with pytest.raises(click.ClickException):
        schema_version(check=True)


def test_schema_version_check_explicit(dbtables):
    """Test explicit check with schema_version."""

    # Change version
    DataIndexVersion.update(version=current_version + 1).where(
        DataIndexVersion.component == "alpenhorn"
    ).execute()

    assert (
        schema_version(component="alpenhorn", component_version=current_version + 1)
        == current_version + 1
    )

    with pytest.raises(click.ClickException):
        schema_version(component="alpenhorn", component_version=current_version + 2)


def test_schema_version_missing(dbproxy, dbtables):
    """Test schema_version with missing data."""

    # Delete all rows
    DataIndexVersion.delete().execute()

    assert schema_version() == 0

    with pytest.raises(click.ClickException):
        schema_version(check=True)

    # This should also fail (can't match to zero)
    with pytest.raises(click.ClickException):
        schema_version(component="alpenhorn", component_version=0)


def test_schema_version_no_table(dbproxy):
    """Test schema_version with no DataIndexVersion table."""

    # This is one of the few functions that should still work
    # if the table is not present
    assert schema_version() == 0
