"""Alpenhorn Database Base Implementation."""

from __future__ import annotations

import logging
from collections import namedtuple
from typing import TYPE_CHECKING

import click
import peewee as pw
from playhouse import db_url

if TYPE_CHECKING:
    from ..extensions.database import DatabaseExtension
del TYPE_CHECKING

# All peewee-generated logs are logged to this namespace.
log = logging.getLogger(__name__)

# We store the database extension here
_db_ext = None

# Initialised by connect()
database_proxy = pw.Proxy()

# Internal database pseduo-extension
InternalDB = namedtuple("InternalDB", ["full_name", "connect", "close", "reentrant"])


def set_extension(ext: DatabaseExtension) -> str | None:
    """Set the DatabaseExtension in use.

    This is called by the extension loader to set
    the in-use database extension.  If an extension is
    already in use, the new extension is _not_ used.

    Parameters
    ----------
    ext : DatabaseExtension
        The DatabaseExtension to use

    Returns
    -------
    str or None:
        If a database extension is already in-use, probably
        due to a prior call to this function, this returns
        the name of the in-use extension.  Otherwise, it
        returns None.
    """
    global _db_ext
    if _db_ext:
        return _db_ext.full_name

    _db_ext = ext
    return None


def _extension() -> DatabaseExtension | namedtuple:
    """Return the DatabaseExtension in use.

    If there is no extension in use, this returns instead
    a namedtuple emulating a DatabaseExtension for the internal
    database module.
    """

    # If a database extension was set, return that.
    if _db_ext:
        return _db_ext

    # Otherwise, we return a pseudo-extension representing the internall
    # fallback DB code
    return InternalDB(__name__, _connect, None, True)


def threadsafe() -> bool:
    """Returns bool indicating whether the database is threadsafe."""
    return _extension().reentrant


def connect() -> None:
    """Connect to the database.

    This must be called once, after extensions are loaded, before
    threads are created.
    """
    from ..common import config

    # attempt to load a database extension
    ext = _extension()

    if ext.full_name == __name__:
        # This is the fallback database connection
        log.debug("Using internal database module.")
    else:
        log.info(f"Using database extension {ext.full_name}.")

    # If fetch the database config, if present
    database_config = config.get("database", default={}, as_type=dict)

    # Call the connect function from the database extension (or fallback)
    # On connection error, raise click.ClickException
    try:
        db = ext.connect(config=database_config)
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

    func = _extension().close
    if not func:
        if database_proxy.obj is not None:
            database_proxy.close()
    else:
        func()


# Helper classes for the peewee ORM
# =================================


class RetryOperationalError:
    """Updated rewrite of the former `peewee.shortcuts.RetryOperationalError` mixin

    See: https://github.com/coleifer/peewee/issues/1472
    """

    def execute_sql(self, sql, params=None, commit=None):
        """Extend default execute_sql to retry.

        Retries once on pw.OperationalError, but only if not
        in a transaction, and only if the database is set to
        autoreconnect.
        """
        try:
            cursor = super().execute_sql(sql, params)
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
            cursor = super().execute_sql(sql, params)
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

        return "string"

    def __init__(self, enum_list, *args, **kwargs):
        self.enum_list = enum_list

        self.value = []
        for e in enum_list:
            self.value.append(f"'{e}'")

        self.maxlen = max([len(val) for val in self.enum_list])

        super().__init__(*args, **kwargs)

    def clone_base(self, **kwargs):
        # Add the extra parameter so the field is cloned properly
        return super().clone_base(enum_list=self.enum_list, **kwargs)

    def get_modifiers(self):
        # This routine seems to be for setting the arguments for creating the
        # column.
        if self.native:
            return self.value or None

        return [self.maxlen]

    def db_value(self, val):
        """Verify supplied value before handing off to DB."""

        # If we're using a native Enum field, just let the DBMS decide what to do
        # Otherwise, allow values in the enum_list and the null value (which may
        # be rejected by the database, but that's not our problem.)
        if self.native or val in self.enum_list or val is None:
            return val

        raise ValueError(f'invalid value "{val}" for EnumField')


class base_model(pw.Model):
    """Base class for all models."""

    class Meta:
        database = database_proxy

        # TODO: consider whether to use only_save_dirty = True here
