"""alpenhorn node clean"""

from __future__ import annotations

import click
import datetime
import peewee as pw

from ...db import (
    StorageGroup,
    StorageNode,
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    database_proxy,
    utcnow,
)
from ...common.util import pretty_bytes
from ..options import not_both
from ..cli import echo


def _run_query(
    update,
    ctx,
    name,
    acq,
    archive_ok,
    days: datetime.datetime,
    has_file: pw.Expression,
    size: int,
    target,
    clean_goal: str,
    warn: bool = True,
) -> None:
    """Actually do the clean query, either in check mode or update mode.

    We separate this from check() because we typically have to do the
    whole thing twice, once at the start to get the results to present
    to the user, and then again to actually do the update.

    We can't do it all at once because we don't want to be in the
    middle of a transaction wile waiting for the user to confirm the clean.

    Parameters
    ----------
    Most parameters are simply the CLI options passed in by click to `clean`.
    Differing ones are:

    update:
        True if we're performing the update; False otherwise.
    clean_goal:
        One of 'Y', 'M', 'N'. Indicating what we're setting wants_file to.
        Replaces the `now` and `cancel` parameters.
    days:
        Converted to datetime
    has_file:
        peewee.Expression instance for the ArchiveFileCopy.has_file constraint.
        That is: an object which is the result of evaluating, say,
        (ArchiveFileCopy.has_file == 'Y'), which we'll pass on to the where()
        clause in the query.
    size:
        Converted to bytes
    warn:
        True the first time this function is called (and we should print
        warnings).
    """

    with database_proxy.atomic():
        # Check name
        try:
            node = StorageNode.get(name=name)
        except pw.DoesNotExist:
            raise click.ClickException("no such node: " + name)

        # Check to see if we are on an archive node and whether
        # --archive-ok was given
        if node.archive:
            if archive_ok:
                if warn:
                    echo(f'DANGER: "{name}" is an archive node. Forcing clean.')
            else:
                raise click.ClickException(f'Cannot clean archive node "{name}".')

        # Resolve targets
        target_files = None
        for gname in target:
            # Resolve group name
            try:
                group = StorageGroup.get(name=gname)
            except pw.DoesNotExist:
                raise click.ClickException("No such target group: " + gname)

            # Get files in target
            query = (
                ArchiveFile.select(ArchiveFile.id)
                .join(ArchiveFileCopy)
                .join(StorageNode)
                .where(
                    StorageNode.group == group,
                    ArchiveFileCopy.has_file == "Y",
                    ArchiveFileCopy.wants_file == "Y",
                )
            )

            # If we already have a list of files on the target, intersect that
            # list with this node
            if target_files:
                query = query.where(ArchiveFile.id << target_files)

            # Execute the query and record the result
            target_files = set(query.scalars())

            # Are there any target files left?
            if not target_files:
                echo("Nothing to do: no matching files in target.")
                ctx.exit()

        # Resolve acqs
        acqs = []
        for acqname in acq:
            try:
                acqs.append(ArchiveAcq.get(name=acqname))
            except pw.DoesNotExist:
                raise click.ClickException("No such acquisition: " + acqname)

        # Find all candidate file copies
        query = ArchiveFileCopy.select().where(ArchiveFileCopy.node == node, has_file)

        # Join to ArchiveFile, if necessary
        if acqs or days or size:
            query = query.join(ArchiveFile)

        # Add a wants_file constraint, if appropriate (with --size)
        # we need to skip this: we need to scan through all the files
        # so the totals are correct.  In that case, this constraint
        # is handled when looping through copies.
        if not size:
            # Without a size, we filter out everything that's not a
            # candidate for update
            if clean_goal == "M":
                # deferred: only mark as "M" want_files that are
                # currently "Y", but leave "N" unchanged
                query = query.where(ArchiveFileCopy.wants_file == "Y")
            else:
                # Otherwise, we're setting everything that's different
                # from our goal to our goal (Y or N)
                query = query.where(ArchiveFileCopy.wants_file != clean_goal)

        # Limit to acquisitions, if any
        if acqs:
            query = query.where(ArchiveFile.acq << acqs)

        # Limit to registration time, if requested.  This will implicitly
        # drop any file copies where the registration time is unknown.
        if days:
            query = query.where(ArchiveFile.registered > days)

        # This will contain stats of what we're going, so we can summarise
        # later.  The outer key is the current value of `wants_file` for a
        # file, or "S" for files already cleaned in --size mode
        results = {
            "Y": {"count": 0, "size": 0},
            "M": {"count": 0, "size": 0},
            "N": {"count": 0, "size": 0},
            "S": {"count": 0, "size": 0},
        }
        update_ids = []

        # For --size check
        total = 0

        # Now loop over files, gathering candidates and statisticts
        for copy in query.order_by(ArchiveFileCopy.id).execute():
            # Skip if we have --targets and this isn't one of them
            if target_files and copy.file.id not in target_files:
                continue

            # Handle null file size
            file_size = 0 if copy.file.size_b is None else copy.file.size_b

            # If we have a size, we need to check whether the copy
            # already has the right wants_file.  If it does, we
            # add its size to the total, but then skip it.
            if size:
                total += file_size
                if copy.wants_file == clean_goal or (
                    clean_goal == "M" and copy.wants_file == "N"
                ):
                    results["S"]["count"] += 1
                    results["S"]["size"] += file_size

                    # If we're over size, we can stop
                    if size and total >= size:
                        break

                    # Otherwise, skip to the next file
                    continue

            # Add to results list
            results[copy.wants_file]["count"] += 1
            results[copy.wants_file]["size"] += file_size

            # Also save the copy id itself, if we're updating
            if update:
                update_ids.append(copy.id)

            # If we're over size, we can stop
            if size and total >= size:
                break

        # Sum everything
        count = results["Y"]["count"] + results["M"]["count"] + results["N"]["count"]
        size = results["Y"]["size"] + results["M"]["size"] + results["N"]["size"]

        # Is there anything to do?
        if not count:
            # Is it because a size constraint was already satisfied?
            if results["S"]["count"] > 0:
                echo("No files to clean (size constraint already satisfied).")
            else:
                echo("No files to clean.")
            ctx.exit()

        # Now report

        # Do we need to print a break-down?
        if clean_goal == "M":
            breakdown = False
        elif clean_goal == "Y":
            breakdown = (
                count != results["M"]["count"] and count != results["N"]["count"]
            )
        else:
            breakdown = (
                count != results["M"]["count"] and count != results["Y"]["count"]
            )

        # Grammar
        files = "files" if count != 1 else "file"
        stop = ":" if breakdown else "."

        if clean_goal == "Y":
            verb = "Cancelling" if update else "Would cancel"
            gerund = " for cleaning"
        elif clean_goal == "M":
            verb = "Marking" if update else "Would mark"
            gerund = " for deferred cleaning"
        else:  # clean_goal == 'N'
            verb = "Releasing" if update else "Would release"
            gerund = ""

        echo(f"{verb} {count} {files} ({pretty_bytes(size)}){gerund}{stop}")

        # Files already satisfying --size
        if size and results["S"]["count"]:
            files = "files" if results["S"]["count"] != 1 else "file"
            echo(
                f'{results["S"]["count"]} {files} ({pretty_bytes(results["S"]["size"])}) already contributing to size constraint.'
            )

        if breakdown:
            if results["Y"]["count"]:
                files = "files" if results["Y"]["count"] != 1 else "file"
                echo(
                    f'  {results["Y"]["count"]} present {files} ({pretty_bytes(results["Y"]["size"])})'
                )
            if results["M"]["count"]:
                files = "files" if results["M"]["count"] != 1 else "file"
                echo(
                    f'  {results["M"]["count"]} marked {files} ({pretty_bytes(results["M"]["size"])})'
                )
            if results["N"]["count"]:
                files = "files" if results["N"]["count"] != 1 else "file"
                echo(
                    f'  {results["N"]["count"]} released {files} ({pretty_bytes(results["N"]["size"])})'
                )
        echo("")

        # Now update, if needed
        if update:
            count = (
                ArchiveFileCopy.update(wants_file=clean_goal)
                .where(ArchiveFileCopy.id << update_ids)
                .execute()
            )
            files = "files" if count != 1 else "file"
            echo(f"Updated {count} {files}.")


@click.command()
@click.argument("name", metavar="NODE")
@click.option(
    "--acq",
    metavar="ACQ",
    default=None,
    multiple=True,
    help="Only clean files in acquisition ACQ.  May be specified multiple times.",
)
@click.option(
    "--archive-ok", help="Run the clean, even if NODE is an archive node.", is_flag=True
)
@click.option("--cancel", "-x", help="Cancel files marked for cleaning", is_flag=True)
@click.option(
    "--check",
    "-c",
    help="Only check (and print) what would be cleaned, don't actually do anything."
    "  Incompatible with --force",
    is_flag=True,
)
@click.option(
    "--days",
    "-d",
    metavar="COUNT",
    type=int,
    help="Only clean files registered more than COUNT days ago.",
)
@click.option(
    "--force",
    "-f",
    help="Force cleaning (skips confirmation).  Incompatible with --check",
    is_flag=True,
)
@click.option(
    "--include-bad",
    help="Include suspect and corrupt files in the operation.",
    is_flag=True,
)
@click.option("--now", "-n", help="Force immediate removal.", is_flag=True)
@click.option(
    "--size",
    "-s",
    metavar="SIZE",
    type=float,
    help="Stop cleaning files once the total size cleaned reaces SIZE GiB.",
)
@click.option(
    "--target",
    metavar="GROUP",
    multiple=True,
    help="Clean only files already in GROUP.  If specified multiple times, clean only"
    "files in all GROUPs.",
)
@click.pass_context
def clean(
    ctx,
    name,
    acq,
    archive_ok,
    cancel,
    check,
    days,
    force,
    include_bad,
    now,
    size,
    target,
):
    """Remove files from a Storage Node.

    There are two ways alpenhorn can schedule a file for cleaning:

    * Normally, files are cleaned by *marking* them for discretionary removal.
      This tells the alpenhorn daemon that it may remove the marked files to
      stay above the minimum free space set for NODE.  If NODE has no minimum
      free space set, then these marked files will never be deleted.  The
      minimum free space can be set using the "--min-avail" option to the
      "node modify" command.

    * The other option, which can be selected with the "--now" flag, is to
      *release* files for immediate removal.  Released files will be removed
      by the daemon as soon as possible.  In this case, files marked for
      discretionary removal will also be released for immediate removal if
      they satisfy the specified criteria.

    Either way, if scheduled for cleaning, files will not be removed from a
    Storage Node unless until they are available on at least two (other)
    archive nodes.

    By default, alpenhorn will refuse run this command on an archive node.
    This restriction may be overridden with the "--archive-ok" flag.  This
    restrition is also ignored when cancelling cleaning.

    In normal operation, this command will schedule *all* files on NODE for
    cleaning, but the operation may be limited with the "--acq", "--days",
    "--size", and "--target" options.

    Multiple --acq options may be given to provide a list of acquisitions to
    restrict cleaning to.  Files not in one of the specified acqusitions will
    not be considered by this command.

    If --days is specified, only files registered more than COUNT days ago will
    be considered for cleaning.

    If --target is specified, one or more times, the command will only affect
    files already available in the target GROUP(s). This is useful for cleaning
    out intermediate locations such as transport disks.  If multiple GROUPs are
    given, a file must be in _all_ GROUPs to be considered for cleaning.

    A SIZE given with --size indicates a target quantity of data to clean.
    This restriction is applied _after_ the limits imposed by --acq and --target.
    alpenhorn will ensure at least SIZE GiB of files are scheduled for cleaning.
    Files are considered in registration order (the order in which they were first
    added to the data index).  In this mode, alpenhorn takes in to consideration
    files previously scheduled for cleaning but not yet removed: if more than
    SIZE GiB of files are already scheduled for cleaning, no new files will be
    added.

    \b
    CANCELLING CLEANING
    -------------------

    You may unschedule files for cleaning using the "--cancel" flag, which can
    be similarly restricted with "--acq", "--days", and "--target", but not
    "--size".

    Both kinds of cleaning (discretionary and immediate) will be cancelled,
    but only for files not yet removed from the NODE by the daemon.
    """
    # usage checks.
    not_both(cancel, "cancel", now, "now")
    not_both(cancel, "cancel", size is not None, "size")
    not_both(check, "check", force, "force")
    if days is not None and days <= 0:
        raise click.UsageError("--days must be positive")
    if size is not None and size <= 0:
        raise click.UsageError("--size must be positive")

    # Convert days to a datetime
    if days:
        days = utcnow() - datetime.timedelta(days=-days)

    # Convert size to bytes
    if size:
        size = int(size * 2**30)

    # What's the user's goal?  i.e. what are we going to be setting
    # wants_file to?
    if now:
        clean_goal = "N"
    elif cancel:
        clean_goal = "Y"
    else:
        clean_goal = "M"

    # Determine the has_file constraint
    if include_bad:
        has_file = ArchiveFileCopy.has_file != "N"
    else:
        has_file = ArchiveFileCopy.has_file == "Y"

    # If we're forcing, skip the check phase
    if not force:
        # Doesn't return if there's nothing to clean
        _run_query(
            False,
            ctx,
            name,
            acq,
            archive_ok,
            days,
            has_file,
            size,
            target,
            clean_goal,
            warn=True,
        )

        # With --check, we're done
        if check:
            ctx.exit()

        # Ask for confirmation
        if not click.confirm("Continue?"):
            echo("\nCancelled.")
            ctx.exit()
        echo()

    # Execute the query, if not in --check mode
    if not check:
        _run_query(
            True,
            ctx,
            name,
            acq,
            archive_ok,
            days,
            has_file,
            size,
            target,
            clean_goal,
            warn=force,
        )