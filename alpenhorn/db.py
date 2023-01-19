"""Database connection.

This module abstracts the database connection, providing a minimally
function fallback if no external database module has been provided.

More capable database connectors may be provided by a database extension
module.  The dict returned by the register_extension() call to a database
extension module must contain a "database" key whose value is a second dict
with keys providing the database extensions capabilities.

The following keys are allowed in the "database" dict, all of which are
optional:
    - "reentrant" : boolean
            If True, the database extension is re-entrant (threadsafe), and
            simultaneous independant connections to the database will be
            made to it.  False is assumed if not given.
    - "connect" : callable
            Invoked to create a database connection.  Will be passed a dict
            containing the contents of the "database" section of the
            alpenhorn config as the keyword parameter "config".  Should
            raise pw.OperationalError if a connection could not be
            established.  If not given, the _connect() function in this
            module will be called instead.
    - "close": a callable.
            Invoked to close the database connection.

Before access the attributes of this module, init() must be called to set
up the database.  After that function is called, the following attributes
are available:

- threadsafe: a boolean indicating whether the database can be concurrently
            accessed from multiple threads.

Also after calling init(), a connection to the database may be initialised
by calling the connect() function.
"""
import logging
import peewee as pw
import playhouse.db_url as db_url

from . import config, extensions

# All peewee-generated logs are logged to this namespace.
log = logging.getLogger(__name__)

# We store the database extension here
_db_ext = None

# Module attributes
# =================
# These are all initialised by init()

database_proxy = pw.Proxy()
threadsafe = None


def _capability(key):
    """Returns the value of the capability specified key for the
    loaded database extension, or the default value, if appropriate.

    Raises KeyError on unknown keys and RuntimeError if called before
    database initialisation
    """
    # capability defaults (these implement the fallback database
    # module).
    default_cap = {
        "connect": None,
        "close": None,
        "reentrant": False,
    }

    try:
        return _db_ext[key]
    except TypeError:
        raise RuntimeError("database not initialised")
    except KeyError:
        try:
            return default_cap[key]
        except KeyError:
            raise KeyError(f"unknown database capability: {key}")


def init():
    """Initiate the database connection framework.

    This must be called once, after extensions are loaded, but
    before attempting to use connect() to create a database connection.
    """
    # attempt to load a database extension
    global _db_ext
    _db_ext = extensions.database_extension()
    if _db_ext is None:
        # The fallback gets implemented via the default_cap
        # dict defined in _capability()
        log.debug("Using internal database module.")
        _db_ext = dict()

    # Tell everyone whether we're threadsafe
    global threadsafe
    threadsafe = _capability("reentrant")


def connect():
    """Connect to the database.

    Should be called per-thread before any database operations are attempted.
    """
    # If fetch the database config, if present
    if "database" in config.config:
        database_config = config.config["database"]
    else:
        database_config = dict()

    # Call the connect function from the database extension (or fallback)
    func = _capability("connect")
    if func is None:
        func = _connect
    db = func(config=database_config)

    database_proxy.initialize(db)

    if isinstance(db, (pw.MySQLDatabase, pw.PostgresqlDatabase)):
        db.field_types["enum"] = "enum"
        EnumField.native = True
    else:
        EnumField.native = False


def _connect(config):
    """Set up the fallback database connection from an explicit peewee url

    If no URL is provided in the config, an in-memory Sqlite database is created.

    This function should never be called directly use the db.connect() function
    instead.

    Parameters
    ----------
    config : dict, optional
        If present, the value of the key "url" is passed as the URL to
        `playhouse.db_url`.
    """

    try:
        db = db_url.connect(config.get("url", "sqlite:///:memory:"))
    except RuntimeError as e:
        raise pw.OperationalError("Database connect failed") from e

    # dynamically make the database instance also inherit from
    # `RetryOperationalError`, so that it retries operations in case of
    # transient database failures
    db.__class__ = type("RetryableDatabase", (RetryOperationalError, type(db)), {})

    return db


def close():
    """Close a database connection if it is open."""

    func = _capability("close")
    if func is None:
        if database_proxy.obj is not None:
            database_proxy.close()
    else:
        func()


# Helper classes for the peewee ORM
# =================================


class RetryOperationalError(object):
    """Updated rewrite of the former `peewee.shortcuts.RetryOperationalError` mixin

    Source: https://github.com/coleifer/peewee/issues/1472
    """

    def execute_sql(self, sql, params=None, commit=pw.SENTINEL):
        try:
            cursor = super().execute_sql(sql, params, commit)
        except pw.OperationalError:
            # If we're in a transaction or the database isn't
            # set to autoconnect, there's not much we can do,
            # so just continue to crash
            if not self.autoconnect or self.in_transaction():
                raise

            # Close the broken connector
            if not self.is_closed():
                self.close()

            # Otherwise, retry.  This will re-open the DB because
            # we've just closed it and autoconnect is set.
            cursor = super().execute_sql(sql, params, commit)
        return cursor


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
            return "enum"
        else:
            return "string"

    def __init__(self, enum_list, *args, **kwargs):
        self.enum_list = enum_list

        self.value = []
        for e in enum_list:
            self.value.append("'%s'" % e)

        self.maxlen = max([len(val) for val in self.enum_list])

        super(EnumField, self).__init__(*args, **kwargs)

    def clone_base(self, **kwargs):
        # Add the extra parameter so the field is cloned properly
        return super(EnumField, self).clone_base(enum_list=self.enum_list, **kwargs)

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
            return str(val or "")
        else:
            raise TypeError("Value %s not in ENUM(%s)" % str(self.value))


class base_model(pw.Model):
    """Fallback base class for all peewee models."""

    class Meta(object):
        database = database_proxy

        # TODO: consider whether to use only_save_dirty = True here
