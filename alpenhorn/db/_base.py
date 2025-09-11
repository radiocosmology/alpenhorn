"""``alpenhorn.db._base``: Alpenhorn Database Base Implementation.

Do not access symbols from this module directly.  Access them
via `alpenhorn.db`.
"""

from __future__ import annotations

import logging
from typing import Any

import click
import peewee as pw
from playhouse import db_url

# All peewee-generated logs are logged to this namespace.
log = logging.getLogger(__name__)

# We store the database extension here
_db_ext = None

# Initialised by connect()
database_proxy = pw.Proxy()


def _capability(key: str) -> Any:
    """Access function for database capabilities.

    This function returns the value of the capability called
    `key` from the dictionary returned by the registered
    database extension.

    If `key` is missing from the database extension dictionary,
    or if there is no database extension registered, then a
    suitable default is returned instead.

    Parameters
    ----------
    key : {"connect", "close", "reentrant"}
        The name of the capability being accessed.

    Raises
    -------
    KeyError
        If `key` is not one of the values listed above.
    RuntimeError
        If this function is used before `connect()` has been called.
    """
    # capability defaults (these implement the fallback database
    # module).
    default_cap = {
        "connect": None,
        "close": None,
        "reentrant": True,
    }

    try:
        return _db_ext[key]
    except TypeError:
        # happens when _db_ext is None because connect() wasn't called
        raise RuntimeError("database not initialised")
    except KeyError:
        try:
            return default_cap[key]
        except KeyError:
            raise KeyError(f"unknown database capability: {key}")


def threadsafe() -> bool:
    """Report whether the database is threadsafe.

    Returns
    -------
    bool
        ``True`` if the database is threadsafe.  ``False`` otherwise.
    """
    return _capability("reentrant")


def connect() -> None:
    """Connect to the database.

    This must be called once, after extensions are loaded, before
    threads are created.
    """
    from ..common import config, extensions

    # attempt to load a database extension
    global _db_ext
    _db_ext = extensions.database_extension()
    if _db_ext is None:
        # The fallback gets implemented via the default_cap
        # dict defined in _capability()
        log.debug("Using internal database module.")
        _db_ext = {}

    # If fetch the database config, if present
    database_config = config.get("database", default={}, as_type=dict)

    # Call the connect function from the database extension (or fallback)
    func = _capability("connect")
    if func is None:
        func = _connect

    # On connection error, raise click.ClickException
    try:
        db = func(config=database_config)
    except (pw.OperationalError, pw.ProgrammingError, pw.ImproperlyConfigured) as e:
        raise click.ClickException(
            f"Unable to connect to the database: {e}.\n"
            "See --help-config for more details."
        ) from e

    database_proxy.initialize(db)

    if isinstance(db, pw.MySQLDatabase | pw.PostgresqlDatabase):
        db.field_types["enum"] = "enum"
        EnumField.native = True
    else:
        EnumField.native = False


def _connect(config: dict) -> pw.Database:
    """Set up the fallback database connection from an explicit peewee url.

    This function should never be called directly use the db.connect() function
    instead.

    Parameters
    ----------
    config : dict
        A dict of configuration data.  If the key "url" is present in the dict,
        its value passed as the URL to `playhouse.db_url`.  If no such key
        exists, a in-memory SQLite database is created.
    """

    try:
        db = db_url.connect(config["url"])
    except KeyError as e:
        raise pw.OperationalError("No database configured") from e
    except RuntimeError as e:
        raise pw.OperationalError("Database connect failed") from e

    # dynamically make the database instance also inherit from
    # `RetryOperationalError`, so that it retries operations in case of
    # transient database failures
    db.__class__ = type("RetryableDatabase", (RetryOperationalError, type(db)), {})

    return db


def close() -> None:
    """Close a database connection if it is open."""

    func = _capability("close")
    if func is None:
        if database_proxy.obj is not None:
            database_proxy.close()
    else:
        func()


# Helper classes for the peewee ORM
# =================================


class RetryOperationalError:
    """DB mixin to retry failed queries.

    See: https://github.com/coleifer/peewee/issues/1472
    """

    def execute_sql(self, sql, params=None, commit=None):
        """Extend default execute_sql to retry.

        Retries once on pw.OperationalError, but only if not
        in a transaction, and only if the database is set to
        autoreconnect.

        Parameters
        ----------
        sql, params, commit : Any
            Parameters per `peewee`.

        Returns
        -------
        peewee.cursor
            The `peewee.cursor`.
        """
        try:
            cursor = super().execute_sql(sql, params, commit)
        except pw.OperationalError:
            # If we're in a transaction or the database isn't
            # set to autoconnect, there's not much we can do,
            # so just continue to crash
            if not self.autoconnect or self.in_transaction():
                raise

            # Close the broken connector, if it's still open
            if not self.is_closed():
                self.close()

            # And then retry.  This will re-open the DB because
            # we've just closed it and autoconnect is True.
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
    *args : tuple
        Passed to `peewee.Field`.
    **kwargs : dict
        Passed to `peewee.Field`.

    Attributes
    ----------
    native : bool
        Attempt to use the native database `ENUM` type. Should be set at the
        *class* level. Only supported for MySQL or PostgreSQL, and will throw
        SQL syntax errors if used for other databases.
    """

    native = True

    @property
    def field_type(self):  # numpydoc ignore=GL08
        if self.native:
            return "enum"

        return "string"

    def __init__(self, enum_list, *args, **kwargs):
        self.enum_list = enum_list

        self.value = []
        for e in enum_list:
            self.value.append(f"'{e}'")

        self.maxlen = max([len(val) for val in self.enum_list])

        super().__init__(*args, **kwargs)

    def clone_base(self, **kwargs):  # numpydoc ignore=GL08
        # Add the extra parameter so the field is cloned properly
        return super().clone_base(enum_list=self.enum_list, **kwargs)

    def get_modifiers(self) -> list | None:  # numpydoc ignore=GL08
        # This routine seems to be for setting the arguments for creating the
        # column.
        if self.native:
            return self.value or None

        return [self.maxlen]

    def db_value(self, val: str | None) -> str | None:
        """Verify supplied value before handing off to DB.

        Parameters
        ----------
        val : str or None
            The value to verify.

        Returns
        -------
        str or None
            `val`.

        Raises
        ------
        ValueError
            Validation failed.
        """

        # If we're using a native Enum field, just let the DBMS decide what to do
        # Otherwise, allow values in the enum_list and the null value (which may
        # be rejected by the database, but that's not our problem.)
        if self.native or val in self.enum_list or val is None:
            return val

        raise ValueError(f'invalid value "{val}" for EnumField')


class base_model(pw.Model):
    """Base class for all models."""

    class Meta:  # numpydoc ignore=GL08
        database = database_proxy

        # TODO: consider whether to use only_save_dirty = True here
