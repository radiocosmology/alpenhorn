"""
test_db
-------

Tests for `alpenhorn.db` module.
"""

import peewee as pw
import pytest

import alpenhorn.db as db
import test_import as ti
import test_storage_model as ts

# try:
#     from unittest.mock import patch, call
# except ImportError:
#     from mock import patch, call


class FailingSqliteDatabase(pw.SqliteDatabase):
    def execute_sql(self, sql, *args, **kwargs):
        self.fail ^= True
        if self.fail:
            self.fail_count += 1
            raise pw.OperationalError("Fail every other time")
        else:
            return super(FailingSqliteDatabase, self).execute_sql(sql, *args, **kwargs)

    def close(self):
        if not self.fail:
            return super(FailingSqliteDatabase, self).close()


from alpenhorn.storage import StorageGroup, StorageNode


@pytest.fixture
def fixtures(tmpdir):
    db._connect()

    # the database connection will fail to execute a statement every other time
    db.database_proxy.obj.__class__ = type(
        "FailingRetryableDatabase",
        (db.RetryOperationalError, FailingSqliteDatabase),
        {},
    )
    db.database_proxy.obj.fail_count = 0
    db.database_proxy.obj.fail = False

    yield ti.load_fixtures(tmpdir)

    assert db.database_proxy.obj.fail_count > 0
    db.database_proxy.close()


def test_schema(fixtures):
    setup_fail_count = db.database_proxy.obj.fail_count
    ti.test_schema(fixtures)
    # we have had more failures during test_import
    assert db.database_proxy.obj.fail_count > setup_fail_count


def test_model(fixtures):
    setup_fail_count = db.database_proxy.obj.fail_count
    ti.test_import(fixtures)
    # we have had more failures during test_import
    assert db.database_proxy.obj.fail_count > setup_fail_count
