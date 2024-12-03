"""Test QueryWalker"""

import peewee as pw
import pytest

from alpenhorn.daemon.querywalker import QueryWalker
from alpenhorn.db import base_model


# Test table
class Table(base_model):
    """Test table"""

    value = pw.IntegerField()


@pytest.fixture
def testtable(dbproxy):
    """Create and populate the test table."""
    dbproxy.create_tables([Table])
    Table.insert_many(
        [
            (1,),
            (2,),
            (3,),
            (4,),
            (5,),
            (6,),
        ]
    ).execute()


@pytest.fixture
def query_walker(testtable):
    """Create the QueryWalker.

    It contains three elements.
    """
    return QueryWalker(Table, Table.value < 4)


def test_empty(testtable):
    """Test empty query returning error."""

    with pytest.raises(pw.DoesNotExist):
        QueryWalker(Table, Table.value < 0)


def test_get(query_walker):
    """Test getting some values."""
    values = query_walker.get(2)
    assert len(values) == 2


def test_emptyget(query_walker):
    """Test get raising when data goes away."""

    # Things are initially okay
    values = query_walker.get()
    assert len(values) == 1

    # Now we delete all the matching records
    Table.delete().where(Table.value < 4).execute()

    # Now the get fails
    with pytest.raises(pw.DoesNotExist):
        values = query_walker.get()


def test_overget(query_walker):
    """Test getting more values than exist."""
    values = query_walker.get(4)
    assert len(values) == 4

    # Since there are only three values in this query,
    # the first and last values are the same.
    assert values[0] == values[3]


def test_pastend(query_walker):
    """Test recovery when the current position ends up past the end of the query."""

    # Advance the query_walker to the last record, possibly by wrapping around once
    while True:
        value = query_walker.get()[0]

        # We were given the penultimate record, so now the QW is on the last record
        if value.value == 2:
            break

    # Delete the last record; now QW is positioned past the end of the query results.
    Table.delete().where(Table.value == 3).execute()

    # The QW should recover by wrapping around and give us the first record.
    value = query_walker.get()[0]
    assert value.value == 1
