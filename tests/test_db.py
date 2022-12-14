"""
test_db
-------

Tests for `alpenhorn.db` module.
"""

import pytest
import peewee as pw
from alpenhorn import db


def test_db(dbproxy):
    assert not db.threadsafe
    assert issubclass(dbproxy.obj.__class__, pw.Database)


def test_chimedb(use_chimedb, dbproxy):
    assert db.threadsafe
    assert issubclass(dbproxy.obj.__class__, pw.Database)
