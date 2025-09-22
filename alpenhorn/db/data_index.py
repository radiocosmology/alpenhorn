"""Data Index metadata

This module provides access to metadata about the Data Index
itself.
"""

from __future__ import annotations

import logging
import operator

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


def _op_and_vers(comp: str):
    """Decompose a string of the form <op><int>.

    Raises ValueError on failure.
    """

    # We could probably do this parameterically, but there's only four of them.
    if comp.startswith("<="):
        return operator.le, int(comp[2:])
    if comp.startswith(">="):
        return operator.ge, int(comp[2:])
    if comp.startswith("<"):
        return operator.lt, int(comp[1:])
    if comp.startswith(">"):
        return operator.gt, int(comp[1:])
    raise ValueError("unknown operator")


def schema_version(
    *,
    check: bool = False,
    component: str | None = None,
    component_version: str | int | None = None,
    return_check: bool = False,
) -> int | bool:
    """Report and optionally check Data Index schema version

    By default, this function returns the schema version of the
    Data Index in the database.  This function can also be used
    to check for a particular version and return the result or
    raise an exception if a version mismatch is encountered.

    All parameters are optional and must be specified by keyword.

    Parameters
    ----------
    check : bool, optional
        If True, check that the alpenhorn Data Index schema version
        is equal to `alpenhorn.db.current_version` and raise an
        exception if it isn't.  If this is True, the other parameters
        to this function are ignored.  Setting this to True is equivalent
        to setting `component` to "alpenhorn" and `component_version` to
        `alpenhorn.db.current_version`.
    component : str or None, optional
        The name of the schema component to report/check.  Setting this
        to `None` (the default) is equivalent to setting it to "alpenhorn".
    component_version : str or int or None, optional
        If not `None`, check that the the schema version of the component
        specified by `component` is not equal to this value.  If `component`
        is not present in the DataIndexVersion table, the check always fails,
        the schema version used for the check is implicitly zero.  If this
        is an `int`, that exact schema version is required.  Otherwise, it may
        be a string of the form: "<OP><int>" or "<OP><int>,<OP><int>" specifying
        a schema range to check.  The "<OP>" should be one of "=", ">", ">=",
        "<", or "<=".  In the two operator case, one operator must indicate
        a minimum version and the other must indicate a maximum version, e.g.
        e.g.  "<8,>=2" or ">2,<5"
    return_check : bool, optional
        If True, and a check is requested, return the boolean result of the
        check, instead of the version of the indicated component.  If this
        is True but no check has been requested, `ValueError` is raised.
        The defalt is False.

    Returns
    -------
    int or bool
        If `return_check` is True, this is the boolean result of the requested
        check.  Otherwise, this is the schema version of the specified component,
        or of the alpenhorn Data Index itself, if no other component was specified.
        If the `component` was not listed in the DataIndexVersion table, zero
        is returned.

    Raises
    ------
    ValueError:
        No check was requested, but `return_check` was True, or "component_version"
        couldn't be parsed.
    click.ClickException:
        A check was requested with `return_check` False, and the check failed,
        or there was an error trying to read from the database.
    """
    # Special case for check
    if check:
        component = "alpenhorn"
        component_version = current_version
    elif not component:
        component = "alpenhorn"

    # Check return_check makes sense
    if return_check and not component_version:
        raise ValueError("return_check is True, but no check has been requested")

    # Fetch version for component
    try:
        schema = DataIndexVersion.get_or_none(component=component)
    except (pw.OperationalError, pw.ProgrammingError, pw.ImproperlyConfigured) as e1:
        # This may be because the table doesn't exist.  Look for it.
        try:
            tables = database_proxy.get_tables()
        except (
            pw.OperationalError,
            pw.ProgrammingError,
            pw.ImproperlyConfigured,
        ) as e2:
            # Database read error
            raise click.ClickException(f"Database read error: {e2}") from e2

        if DataIndexVersion._meta.table_name in tables:
            # Table exists, must be some sort of other error
            raise click.ClickException(
                f"Unable to determine schema version: {e1}"
            ) from e1

        # Otherwise, no table
        schema = None

    if schema:
        schema_vers = schema.version
    else:
        schema_vers = 0

    # If no check, we're done
    if component_version is None:
        return schema_vers

    # Otherwise, start checking
    check_failed = False

    # Is component_version a simple int?
    try:
        version1 = int(component_version)
        op = operator.eq
    except ValueError:
        version1 = None

    # If component_version wasn't an int, we'll need to parse it
    if version1 is None:
        # Split on commas
        comparisons = component_version.split(",")

        if len(comparisons) > 2:
            raise ValueError(f"bad schema version: {component_version}")

        # Decode the first (and maybe only) comparison
        try:
            op, version1 = _op_and_vers(comparisons[0])

            # Decode the second comparison, if present
            if len(comparisons) > 1:
                op2, version2 = _op_and_vers(comparisons[1])
            else:
                op2 = None
        except ValueError as e:
            raise ValueError(f"bad schema version: {component_version}") from e

        # If we have two checks, make sure they're opposite
        if op2:
            if op == operator.lt or op == operator.le:
                if op2 != operator.ge and op2 != operator.gt:
                    raise ValueError(f"bad schema version: {component_version}")
            if op == operator.gt or op == operator.ge:
                if op2 != operator.le and op2 != operator.lt:
                    raise ValueError(f"bad schema version: {component_version}")

            # Do the second check, first (with "not" to ensure check_failed is a bool)
            check_failed = not op2(schema_vers, version2)

    # Do the first check, if the second one didn't fail
    if not check_failed:
        check_failed = not op(schema_vers, version1)

    # If we were requested to return the result of the check, do that now
    if return_check:
        return not check_failed

    # Otherwise, compose the clickException on error
    if check_failed:
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

    # Finally, return the schema version
    return schema_vers
