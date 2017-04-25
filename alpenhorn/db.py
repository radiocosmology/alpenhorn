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


def connect(url=None):
    # TODO: use connectdb?
    db = db_url.connect(url or 'sqlite:///:memory:')
    db.register_fields({'enum': 'enum'})
    database_proxy.initialize(db)


# Helper classes for the peewee ORM
# =================================

class EnumField(pw.Field):
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


class base_model(pw.Model):
    """Baseclass for all models."""

    class Meta(object):
        database = database_proxy
