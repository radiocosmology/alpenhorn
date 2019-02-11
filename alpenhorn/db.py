from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging
import peewee as pw
import playhouse.db_url as db_url

# All peewee-generated logs are logged to this namespace.
logger = logging.getLogger(__name__)


# Global variables and constants.
# ================================

# _property = property  # Do this because we want a class named "property".

class RobustProxy(pw.Proxy):
    """A robust database proxy.

    A merger of peewee's database proxy support and the functionality of the
    `RetryOperationalError` mixin. to try and make the database connection more
    robust by default.
    """

    def execute_sql(self, sql, params=None, require_commit=True):
        # Code in this function is a modified version of some in peewee
        # Copyright (c) 2010 Charles Leifer

        # Check that the proxy exists
        if self.obj is None:
            raise AttributeError('Cannot use uninitialized Proxy.')

        # If it does try to run the query...
        try:
            cursor = self.obj.execute_sql(
                sql, params, require_commit
            )

        # ... if it fails, try to run it again.
        except pw.OperationalError:
            if not self.obj.is_closed():
                self.obj.close()
            cursor = self.obj.get_cursor()
            cursor.execute(sql, params or ())
            if require_commit and self.obj.get_autocommit():
                self.obj.commit()
        return cursor


database_proxy = RobustProxy()


def config_connect():
    """Initiate the database connection from alpenhorns config.

    If an `'url'` entry is present in the `'database'` section of the
    configuration, use this, otherwise try and start the connection using an
    extension.
    """

    from . import config, extensions

    # Connect to the database
    if 'database' in config.config and \
       'url' in config.config['database']:
        _connect(url=config.config['database']['url'])
    else:
        db_ext = extensions.database_extension()

        if db_ext is not None:
            _connect(db=db_ext)
        else:
            raise RuntimeError('No way to connect to the database')


def _connect(url=None, db=None):
    """Set up the database connection from an explicit peewee url, or
    `peewee.Database`.

    If neither argument is specified create and in-memory Sqlite database. If
    both are given, initialisation by `url` is chosen. This routine also adds an
    `EnumField` type to peewee. For databases that support it, this is a native
    implemenation in the database, for those that don't (Sqlite), this is simply
    a `VARCHAR` type with the validation done in Python.

    Generally the database connection should be initiated using the
    `config_connect` routine.

    Parameters
    ----------
    url : str, optional
        Database url using the scheme from `playhouse.db_url`.
    db : `peewee.Database`, optional
        Peewee database instance to connect alpenhorn to.
    """

    global EnumField

    if url is None and db is None:
        url = 'sqlite:///:memory:'

    if url is not None:
        db = db_url.connect(url)

    if isinstance(db, (pw.MySQLDatabase, pw.PostgresqlDatabase)):
        db.field_types['enum'] = 'enum'
        EnumField.native = True
    else:
        EnumField.native = False

    database_proxy.initialize(db)


# Helper classes for the peewee ORM
# =================================


class EnumField(pw.Field):
    """Implements an ENUM field for peewee.

    Only MySQL and PostgreSQL support `ENUM` types natively in the database. For
    Sqlite (and others), the `ENUM` is implemented as an appropriately sized
    `VARCHAR` and the validation is done at the Python level.

    .. warning::
        For the *native* ``ENUM`` to work you *must* register it with peewee by
        doing something like::

            db.register_fields({'enum': 'enum'})

    Parameters
    ----------
    enum_list : list
        A list of the string values for the ENUM.

    Attributes
    ----------
    native : bool
        Attempt to use the native database `ENUM` type. Should be set at the
        *class* level. Only supported for MySQL or PostgreSQL, and will throw
        SQL syntax errors if used for other databases.
    """

    native = True

    @property
    def field_type(self):
        if self.native:
            return 'enum'
        else:
            return 'string'

    def __init__(self, enum_list, *args, **kwargs):
        self.enum_list = enum_list

        self.value = []
        for e in enum_list:
            self.value.append("'%s'" % e)

        self.maxlen = max([len(val) for val in self.enum_list])

        super(EnumField, self).__init__(*args, **kwargs)

    def clone_base(self, **kwargs):
        # Add the extra parameter so the field is cloned properly
        return super(EnumField, self).clone_base(
            enum_list=self.enum_list, **kwargs)

    def get_modifiers(self):
        # This routine seems to be for setting the arguments for creating the
        # column.
        if self.native:
            return self.value or None
        else:
            return [self.maxlen]

    def coerce(self, val):
        # Coerce the db/python value to the correct output. Also perform
        # validation for non native ENUMs.
        if self.native or val in self.enum_list:
            return str(val or '')
        else:
            raise TypeError("Value %s not in ENUM(%s)" % str(self.value))


class base_model(pw.Model):
    """Baseclass for all models."""

    class Meta(object):
        database = database_proxy

        # TODO: consider whether to use only_save_dirty = True here
