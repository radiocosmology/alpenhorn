"""Data Index metadata

This module provides access to metadata about the Data Index
itself.
"""

from __future__ import annotations

import logging

import click
import peewee as pw

from ._base import base_model, database_proxy

log = logging.getLogger(__name__)

# This is the alpenhorn Data Index schema version implemented by this
# version of the alpenhorn code-base.  In general, only a single schema
# version (the one specified here) is supported by a particular version
# of alpenhorn.
current_version = 2


class DataIndexVersion(base_model):
    """Schema version table for the Data Index.

    Attributes
    ----------
    component : str
        The component of the Data Index being versioned.
        The primary alpenhorn tables have the component
        name "alpenhorn".  Extensions may also use this
        table to track their database schema versions by
        setting this to the name of the extension.
    version : int
        The schema version of the Data Index for the component.
        Schema versions should start at one and increment by
        one each time the schema for the specified `component`
        is changed.
    """

    component = pw.CharField(max_length=256, unique=True)
    version = pw.IntegerField()


def schema_version(
    *,
    check: bool = False,
    component: str | None = None,
    component_version: int | None = None,
) -> int:
    """Report and optionally check Data Index schema version

    By default, this function returns the schema version of the
    Data Index in the database.  This function can also be used
    to check for a particular version and raise an exception if
    a version mismatch is encountered.

    All parameters must be specified by keyword.

    Parameters
    ----------
    check:
        If True, check that the alpenhorn Data Index schema version
        is equal to `alpenhorn.db.current_version` and raise an
        exception if it isn't.  If this is True, the other parameters
        to this function are ignored.  Setting this to True is equivalent
        to setting `component` to "alpenhorn" and `component_version` to
        `alpenhorn.db.current_version`.
    component:
        The name of the schema component to report/check.  Setting this
        to `None` (the default) is equivalent to setting it to "alpenhorn".
    component_version:
        If not `None`, raise an exception if the schema version of the
        component specified by `component` is not equal to this value.
        If `component` is not present in the DataIndexVersion table, this
        check always fails.

    Returns
    -------
    version : int
        The schema version of the specified component, or of the alpenhorn
        Data Index itself, if no other component was specified.  If this
        function was requested to check the version (because `check` or
        `component_version` was set), this will always equal the target
        version (because any other version will result in an exception, instead).
        If the `component` was not listed in the DataIndexVersion table, zero
        is returned.

    Raises
    ------
    click.ClickException:
        A check was requested and the check failed.
    """
    # Special case for check
    if check:
        component = "alpenhorn"
        component_version = current_version
    elif not component:
        component = "alpenhorn"

    # Fetch version for component
    try:
        schema = DataIndexVersion.get_or_none(component=component)
    except (pw.OperationalError, pw.ProgrammingError):
        # This may be because the table doesn't exist.  Look for it.
        if DataIndexVersion._meta.table_name in database_proxy.get_tables():
            # Table exists, but be some sort of other error
            raise

        # Otherwise, no table
        schema = None

    if schema:
        schema_vers = schema.version
    else:
        schema_vers = 0

    # Check, if requested
    if component_version is not None:
        # Importantly here: if `schema` is None, this check always fails
        # (i.e. even if the caller were to have passed component_version=0)
        if not schema or schema_vers != component_version:
            # There's a special message for alpenhorn itself
            if component == "alpenhorn":
                lead = "Data Index version mismatch"
            else:
                lead = f'Schema version mismatch for Data Index component "{component}"'

            wanted = f"wanted version {component_version}"

            if schema:
                found = f"found version {schema_vers}"
            else:
                found = "no schema found"

            raise click.ClickException(f"{lead}:  {wanted}; {found}")

    # Check, if performed, succeeded.  Return found version
    return schema_vers
