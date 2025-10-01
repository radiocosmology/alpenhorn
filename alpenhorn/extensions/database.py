"""The Alpenhorn Database Extension."""

from collections.abc import Callable
from typing import Any

import peewee as pw

from .base import Extension

# Type alias for the database connection function
DatabaseConnect = Callable[[Any], pw.Database]


class DatabaseExtension(Extension):
    """A Database extension.

    A Database Extension can be used to override alpenhorn's standard method
    of connection to the Data Index.

    Only one database extension can be loaded by alpenhorn at a time.

    Attributes
    ----------
    name : str
        The name of this Extension.  The name of an Extension should
        be unique within the extension module defining it, but does
        not need to be globally unique.
    version : str
        The version of the Extension.  The verison must be parsable by
        `packaging.version`.
    connect : Callable
        A function called to create the database connection.  The function
        will be called after the alpenhorn config is read and will be passed
        the contents of the "database" section of the config. On successful
        connection a `peewee.Database` should be returned.  On failure, this
        function should raise one of `peewee.OperationalError`,
        `peewee.ProgrammingError`, or `peewee.ImproperlyConfigured` as
        appropriate.
    close : Callable, optional
        A function called to close the database connection.  If not provided,
        the `close` method of the database returned by `connect` is called instead.
        If given, the function is called with no arguments and the returned value
        is ignored.
    reentrant : bool, optional
        Should be True if the database connection is threadsafe (re-entrant).
        Defaults to False
    min_version : str, optional
        If given, the minimum Alpenhorn version supported by
        this Extension.  Note: it may make more sense for an extension
        module to check `alpenhorn.__version__` directly instead of using
        this parameter.
    max_version : str, optional
        If given, the maximum Alpenhorn version supported by
        this Extension.  Note: it may make more sense for an extension
        module to check `alpenhorn.__version__` directly instead of using
        this parameter.
    """

    # Initialised immediately after config is loaded.
    stage = 1

    def __init__(
        self,
        name: str,
        version: str,
        connect: DatabaseConnect,
        close: Callable | None = None,
        reentrant: bool = False,
        min_version: str | None = None,
        max_version: str | None = None,
    ) -> None:
        super().__init__(
            name, version, min_version=min_version, max_version=max_version
        )
        if not callable(connect):
            raise ValueError("connect not callable.")
        self.connect = connect

        if close is not None and not callable(close):
            raise ValueError("close not callable.")

        self.close = close
        self.reentrant = bool(reentrant)
