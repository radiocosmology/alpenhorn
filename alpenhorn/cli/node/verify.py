"""alpenhorn node verify command"""

from __future__ import annotations

import click
import peewee as pw

from ...common.util import pretty_bytes
from ...db import StorageNode, ArchiveAcq, ArchiveFile, ArchiveFileCopy, database_proxy
from ..options import cli_option, not_both, resolve_acqs
from ..cli import check_then_update, echo


def _run_query(
    update: bool,
    ctx,
    name: str,
    acq: list,
    file_selection: pw.Expression,
    verify_goal: str,
    first_time: bool,
):
    """Run the veroify query, either in check mode or update mode.

    Must be done twice because the DB contents can change while we wait
    for the user's confirmation.

    Parameters:
    -----------
    update:
        True if in "update" mode.  False in "count" mode.
    ctx:
        click context object
    name:
        node name
    acq:
        list of --acq arguments, if any
    file_selection:
        a peewee.Expression encapsulating the --corrupt, --missing, and
        --healthy optons.  This is a constraint which will be applied to
        the `where` clause of the `ArchiveFileCopy` query.
    verify_goal:
        The value we want to set has_file to.  This is 'M' except in
        --cancel mode.
    """

    with database_proxy.atomic():
        # Check name
        try:
            node = StorageNode.get(name=name)
        except pw.DoesNotExist:
            raise click.ClickException("no such node: " + name)

        # Resolve acqs
        acqs = resolve_acqs(acq)

        # Find all candidate file copies
        query = ArchiveFileCopy.select(ArchiveFileCopy.id).where(
            ArchiveFileCopy.node == node, file_selection
        )

        # Limit to acquisitions, if any
        if acqs:
            query = query.join(ArchiveFile).where(ArchiveFile.acq << acqs)

        # Perform the query
        copies = list(query.execute())

        # Did we match?
        if not copies:
            echo("No matching files found.")
            ctx.exit()

        # Tot everything up.
        total = (
            ArchiveFileCopy.select(
                pw.fn.COUNT(ArchiveFileCopy.id).alias("count"),
                pw.fn.Sum(ArchiveFile.size_b).alias("size"),
            )
            .join(ArchiveFile)
            .where(ArchiveFileCopy.id << copies)
            .execute()
        )[0]

        # Grammar
        files = "file" if total.count == 1 else "files"
        if verify_goal != "M":  # i.e. cancelling
            if update:
                verb = "Forcing"
            else:
                verb = "Would force"
            what = "status"

            if verify_goal == "Y":
                goal = " to healthy"
            elif verify_goal == "N":
                goal = " to missing"
            else:
                goal = " to corrupt"
        else:
            if update:
                verb = "Requesting"
            else:
                verb = "Would request"
            what = "verification"
            goal = ""

        size = pretty_bytes(total.size) if total.size is not None else "size unknown"
        echo(f"{verb} {what} of {total.count} {files} ({size}){goal}.")

        if update:
            # Do the update
            count = (
                ArchiveFileCopy.update(has_file=verify_goal)
                .where(ArchiveFileCopy.id << copies)
                .execute()
            )
            files = "file" if count == 1 else "files"
            echo(f"Updated {count} {files}.")


@click.command()
@click.argument("name", metavar="NODE")
@cli_option("acq")
@cli_option(
    "all_",
    help="Ignored if used with --cancel.  Otherwise, verify all files "
    '(equivalent to "--corrupt --healthy --missing").',
)
@click.option(
    "--cancel",
    is_flag=True,
    help="Cancel existing verifcation requests by explicitly setting their status",
)
@click.option(
    "--count",
    is_flag=True,
    help="Don't actually perform the operation, just print how many files "
    "would be affected",
)
@click.option(
    "--corrupt",
    help="With --cancel, set file status to corrupt.  Without --cancel, "
    "verify known corrupt files",
    is_flag=True,
)
@click.option(
    "--force",
    help="Force update (skips confirmation).  Incompatible with --count",
    is_flag=True,
)
@click.option(
    "--healthy",
    help="With --cancel, set file status to healthy.  Without --cancel, "
    "verify known good files",
    is_flag=True,
)
@click.option(
    "--missing",
    help="With --cancel, set file status to missing.  Without --cancel, "
    "verify known missing files",
    is_flag=True,
)
@click.pass_context
def verify(ctx, name, acq, all_, cancel, count, corrupt, force, healthy, missing):
    """Verify files on a Storage Node.

    Use this command to request verification of files on NODE, or to
    explicitly set the status of suspect files (thus, cancelling verificaiton).

    \b
    VERIFYING FILES
    ---------------

    Without the "--cancel" flag, this command requests that the daemon re-verify
    the integrity of files on NODE.  This is done by changing the status of the
    files being re-verified to "suspect".

    By default, only files known to be corrupt or missing are verified,
    but the types of files to re-verify can be explicitly chosen with the
    --all, --corrupt, --healthy, and --missing flags.  If any of these flags
    are used, the default selection is ignored.

    Files to verify may be further restricted by specifying one or
    more acqusitions to limit the operation to (via the --acq option).
    Files which have been released for immediate removal (via, say, the
    "node clean" command) are always skipped.

    NOTE: Be careful when you request re-verification.  After this command
    sets an affected file to "suspect", that file will not be available for
    other operations (e.g. transfer) until the daemon has re-verified the
    it.  Re-verification, therefore, is best done during times of low
    activity on the NODE.

    \b
    CANCELLING VERIFICATION
    -----------------------

    With the "--cancel" flag, this command instead, changes the status
    of matching suspect files to the new status specified by one of the
    flags "--corrupt", "--missing", or "--healthy".  (If none are given,
    "--corrupt" is the default.)

    Which files are updated may be further restricted with the "--acq"
    option.

    NOTE: If you cancel verification of a file which the daemon happens to
    be in the process of re-verifying, the result of the daemon's check
    will override whatever status you set manually.
    """
    # usage checks
    not_both(count, "count", force, "force")

    # Cancel and non-cancel modes are fairly different about what most
    # of the flags mean
    if cancel:
        # All is explicitly ignored in cancel mode: we effectively cancel
        # all matching verification requests
        not_both(corrupt, "corrupt", healthy, "healthy")
        not_both(corrupt, "corrupt", missing, "missing")
        not_both(healthy, "healthy", missing, "missing")

        # We match against suspect files in this case
        file_selection = (ArchiveFileCopy.has_file == "M") & (
            ArchiveFileCopy.wants_file != "N"
        )

        # Our goal is determined by flag.  This is the
        # value we'll be setting has_file to in the update
        if healthy:
            verify_goal = "Y"
        elif missing:
            verify_goal = "N"
        else:
            # --corrupt is the default, if no flag is given
            verify_goal = "X"
    else:
        # Verify mode; normalise corrupt/missing/healthy flags
        if all_:
            corrupt = True
            missing = True
            healthy = True
        elif not corrupt and not healthy and not missing:
            corrupt = True
            missing = True

        # In verify mode, we're always going to set has_file to 'M'
        verify_goal = "M"

        # Build up a peewee.Expression for the types of files we want to find
        if corrupt:
            file_selection = (ArchiveFileCopy.has_file == "X") & (
                ArchiveFileCopy.wants_file != "N"
            )
        else:
            file_selection = None

        if healthy:
            healthy_expr = (ArchiveFileCopy.has_file == "Y") & (
                ArchiveFileCopy.wants_file != "N"
            )
            if file_selection is None:
                file_selection = healthy_expr
            else:
                file_selection = file_selection | healthy_expr

        if missing:
            missing_expr = (ArchiveFileCopy.has_file == "N") & (
                ArchiveFileCopy.wants_file == "Y"
            )
            if file_selection is None:
                file_selection = missing_expr
            else:
                file_selection = file_selection | missing_expr

        # Sanity check
        if file_selection is None:
            raise RuntimeError("Something went wrong!  Selection expression is empty.")

    # Do a check-then-update with the `_run_query` function.
    check_then_update(
        not force,
        not count,
        _run_query,
        ctx,
        args=[name, acq, file_selection, verify_goal],
    )