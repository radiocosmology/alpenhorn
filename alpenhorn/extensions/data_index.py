"""The Alpenhorn Data Index Extension."""

from __future__ import annotations

from collections.abc import Callable, Iterable

from .base import Extension


class DataIndexExtension(Extension):
    """A Data Index Schema Extension.

    A Data Index Schema Extension provides additional table models used to
    extend the Alpenhorn data index database.

    Attributes
    ----------
    name : str
        The name of this Extension.  The name of an Extension should
        be unique within the extension module defining it, but does
        not need to be globally unique.
    version : str
        The version of the Extension.  The verison must be parsable by
        `packaging.version`.
    component : str
        The name of this data index component (in the DataIndexVersion table).
    schema_version : int
        The version of the data index component (in the DataIndexVersion table).
        Must be positive.
    tables : Iterable
        A list or other iterable of table models.  These should be subclasses of
        `alpenhorn.db.base_model`.
    post_init : Callable, optional
        A hook to call after creating the tables in the Data Index for the first
        time.  This can be used to populate the newly-created tables or perform
        other initialisation for the component.  This hook is executed in a
        database transaction.  If an exception is thrown, the transaction is
        rolled back and the component is not initialised.  Any value returned by
        this hook is ignored.
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
    require_schema : dict, optional
        If given, a dict of Data Index schema components and the required
        component schema version needed by this extension.  See
        `alpenhorn.db.schema_version` for the specification of the requirements.
        All DataIndex Extensions have an implicit requirement on their
        own schema `component`, which must match `schema_version`.
    """

    # Initialised after database connection
    stage = 2

    def __init__(
        self,
        name: str,
        version: str,
        component: str,
        schema_version: int,
        tables: Iterable,
        post_init: Callable | None = None,
        min_version: str | None = None,
        max_version: str | None = None,
        require_schema: dict[str, int | str] | None = None,
    ) -> None:
        from ..db import base_model

        # Add an implicit requirement for our own schema
        if require_schema is None:
            require_schema = {component: schema_version}
        else:
            require_schema[component] = schema_version

        super().__init__(
            name,
            version,
            min_version=min_version,
            max_version=max_version,
            require_schema=require_schema,
        )

        if not isinstance(component, str):
            raise TypeError("component name must be a string")

        component = component.strip()
        if not component:
            raise ValueError("empty component name")
        if component == "alpenhorn":
            raise ValueError("Invalid component name")
        self.component = component

        # Co-erce to int
        try:
            self.schema_version = int(schema_version)
        except (ValueError, TypeError) as e:
            raise TypeError("schema_version must be an int") from e

        # Reject non-integer numbers
        if self.schema_version != float(schema_version):
            raise TypeError("schema_version must be an int")

        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")

        # Check that a single table hasn't been passed as "tables"
        if isinstance(tables, type):
            raise TypeError("Parameter 'tables' must be iterable")

        self.tables = set()
        for t in tables:
            try:
                if not issubclass(t, base_model):
                    raise ValueError(f"invalid table model: {t}")
            except TypeError as e:
                # t wasn't a class
                raise ValueError(f"invalid table model: {t}") from e
            self.tables.add(t)

        if not self.tables:
            raise ValueError("No tables defined")

        self.post_init = post_init
