"""``alpenhorn.cli.cli``: Alpenhorn CLI core.

This module is a library of core functionality
used to implement the Alpenhorn CLI.
"""

from __future__ import annotations

from collections.abc import Callable

import click

from ..common.logger import echo


def check_then_update(
    do_check: bool, do_update: bool, func: Callable, ctx, *, args: tuple | list = ()
) -> None:
    """Boilerplate for the check-confirm-update pattern.

    Calls `func` up to twice, and maybe asks for confirmation in between.

    Flow is like this:

    1. If `do_check` is False, skips to step 6
    2. Calls `func` in "check" mode.
    3. If `do_update` is False, program exits
    4. Asks user for confirmation.
    5. If user declines confirmation, program exits
    6. If `do_update` is True, calls `func` in "update" mode.

    When calling `func` the following arguments are passed, in order:

    1. update : bool
        ``False`` in "check" mode (step 2) and ``True`` in "update" mode (step 6)
    2. ctx: The click context object
    3. any positional parameters given in in `args`
    4. `first_time` : bool, a keyword-only argument
        ``True`` the first time `func` is called, and ``False`` the second time
        (if called twice).

    Anything returned by `func` is ignored.

    Parameters
    ----------
    do_check : bool
        ``True`` if we should run the check phase.
    do_update : bool
        ``True`` if we should run the update phase.
        .. note::
        If both `do_check` and `do_update` are ``False``, this function will
        successfully do nothing.
    func : Callable
        The function to call.
    ctx : click Context
        The `click` context.
    args : tuple or list
        Positional arguments to pass `func`.
    """

    if do_check:
        func(False, ctx, *args, first_time=True)

        # If we're not doing the update, we're done
        if not do_update:
            ctx.exit()

        # Ask for confirmation
        echo()
        if not click.confirm("Continue?"):
            echo("\nCancelled.")
            ctx.exit()
        echo()

    # If, both do_check and do_update are False, I guess we do nothing...
    if do_update:
        # `first_time` is `not do_check` here because if do_check is True, this is the
        # second call, and if do_check is False, this is the first call
        func(True, ctx, *args, first_time=not do_check)


def update_or_remove(field: str, new: str | None, old: str | None) -> dict:
    """Helper for metadata updates.

    A helper function to determine whether a field needs updating.
    Only works on string fields.  (Numeric fields need some other
    mechanism to let the user specify None/null when necessary.)

    Parameters
    ----------
    field : str
        Name of the field (i.e. table column) to update.
    new : str or None
        The new value of the field, maybe.  From the user.  If this
        is ``None``, there is no update.  If this is the empty string,
        the field should be set to ``None`` (i.e. null) if not that already.
    old : str or None
        The current value of the field, which might be ``None`` (i.e. null).
        From the database.

    Returns
    -------
    dict
        A dict containing the fields to update.  Has at most one key, `field`,
        which is present only if this function determines an update is required.
        It should be merged with the dict of updates by the caller.
    """

    # This implies the user didn't specify anything for this field
    if new is None:
        return {}

    # Check for empty string, which means: set to None
    if new == "":
        if old is not None:
            return {field: None}
        return {}

    # Otherwise, new is an actual value. Set it, if different than old
    if new != old:
        return {field: new}

    # Otherwise, no change.  Return no update.
    return {}


def pretty_time(time: datetime | None) -> str:
    """Print a human-readable time.

    Parameters
    ----------
    time : naive datetime or None
        The time to format.  Assumed to be in UTC (which is true
        for any time read from the data index), or ``None``,
        indicating no value.

    Returns
    -------
    str
        The time formatted as a string.  If `time` was ``None``,
        this will be simply "-".
    """

    if time:
        return time.ctime() + " UTC"
    return "-"
