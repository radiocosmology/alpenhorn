from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging
import peewee as pw
import playhouse.db_url as db_url

# All peewee-generated logs are logged to this namespace.
logger = logging.getLogger("db")
logger.addHandler(logging.NullHandler())


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
    if 'database' in config.configdict and \
       'url' in config.configdict['database']:
        _connect(url=config.configdict['database']['url'])
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

    if isinstance(db, pw.SqliteDatabase):
        EnumField = EnumFieldPython
    else:
        db.register_fields({'enum': 'enum'})
        EnumField = EnumFieldDB

    database_proxy.initialize(db)


# Helper classes for the peewee ORM
# =================================

EnumField = None


class EnumFieldDB(pw.Field):
    """Implements an enum field for the ORM.

    Why doesn't peewee support enums? That's dumb. We should make one."""
    db_field = 'enum'

    def __init__(self, enum_list, *args, **kwargs):
        self.enum_list = enum_list
        self.value = []
        for e in enum_list:
            self.value.append("'" + str(e) + "'")
        super(EnumField, self).__init__(*args, **kwargs)

    def clone_base(self, **kwargs):
        return super(EnumField, self).clone_base(
            enum_list=self.enum_list, **kwargs)

    def get_modifiers(self):
        return self.value or None

    def coerce(self, val):
        return str(val or '')


class EnumFieldPython(pw.CharField):
    """An EnumField implementation for Sqlite that enforces the type at the Python level.

    Parameters
    ----------
    enum_values : list
        A list of the string values for the ENUM.
    """

    def __init__(self, enum_values, *args, **kwargs):

        self.enum_values = list(enum_values)

        # Get the maximum length of any string in the set of enum values, and
        # use this to initialise the length of the underlying CharField
        maxlen = max([len(val) for val in self.enum_values])
        super(EnumFieldPython, self).__init__(max_length=maxlen, *args, **kwargs)

        def db_value(self, value):
            if value not in self.enum_values:
                raise TypeError("Value %s not in ENUM(%s)" % str(enum_values))
            return super(EnumFieldPython, self).db_field(value)


EnumField = EnumFieldPython


class base_model(pw.Model):
    """Baseclass for all models."""

    class Meta(object):
        database = database_proxy
