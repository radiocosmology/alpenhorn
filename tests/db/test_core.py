"""Tests of alpenhorn.db module."""

import threading

import peewee as pw
import pytest

from alpenhorn.db import _base as db


def test_db(dbproxy):
    """Test starting the default DB."""
    assert db.threadsafe
    assert issubclass(dbproxy.obj.__class__, pw.Database)


def test_chimedb(use_chimedb, dbproxy):
    """Test starting CHIMEdb."""
    assert db.threadsafe
    assert issubclass(dbproxy.obj.__class__, pw.Database)


def test_chimedb_concurrency(use_chimedb, dbproxy):
    """Test concurrency in the chimedb db module."""

    # Test table
    class Values(db.base_model):
        value = pw.IntegerField()

    # Create and populate
    dbproxy.create_tables([Values])
    assert dbproxy.get_tables() == ["values"]
    Values.insert_many([{"value": 12}, {"value": 34}, {"value": 56}]).execute()

    # Verify database before test
    assert list(Values.select().dicts()) == [
        {"id": 1, "value": 12},
        {"id": 2, "value": 34},
        {"id": 3, "value": 56},
    ]

    # Barriers
    before = threading.Barrier(2)
    after = threading.Barrier(2)

    # Threads
    def worker1():
        nonlocal Values

        # Init this thread
        db.connect()
        assert dbproxy.get_tables() == ["values"]

        # Wait
        before.wait()

        # Start a transaction
        with db.database_proxy.atomic():
            Values.update(value=123).where(Values.id == 1).execute()
            Values.update(value=456).where(Values.id == 3).execute()

        # Synchronise
        after.wait()

    def worker2():
        nonlocal Values
        # Init this thread
        db.connect()
        assert dbproxy.get_tables() == ["values"]

        Values.update(value=0).where(Values.id == 2).execute()

        # Wait
        before.wait()
        after.wait()

        Values.insert(value=89).execute()

    t1 = threading.Thread(target=worker1, daemon=True)
    t2 = threading.Thread(target=worker2, daemon=True)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    assert list(Values.select().dicts()) == [
        {"id": 1, "value": 123},
        {"id": 2, "value": 0},
        {"id": 3, "value": 456},
        {"id": 4, "value": 89},
    ]


def test_enum(dbproxy):
    """Test the EnumField"""

    class Enummery(db.base_model):
        abcd = db.EnumField(["a", "b", "c", "d"], default="a")
        efgh = db.EnumField(["ef", "gh"], null=True)

    dbproxy.create_tables([Enummery])

    Enummery.insert(abcd="b", efgh="ef").execute()
    Enummery.insert(efgh=None).execute()

    # Bad values
    with pytest.raises(ValueError):
        Enummery.insert(abcd="e", efgh="a").execute()

    assert list(Enummery.select().dicts()) == [
        {"id": 1, "abcd": "b", "efgh": "ef"},
        {"id": 2, "abcd": "a", "efgh": None},
    ]
