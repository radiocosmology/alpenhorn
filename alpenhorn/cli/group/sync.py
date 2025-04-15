"""alpenhorn group sync command"""

from __future__ import annotations

from collections import defaultdict

import click
import peewee as pw

from ...common.util import pretty_bytes
from ...db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
    utcnow,
)
from ..cli import check_then_update, echo
from ..options import (
    check_if_from_stdin,
    cli_option,
    files_from_file,
    files_in_groups,
    not_both,
    requires_other,
    resolve_acq,
    resolve_group,
)


def _run_cancel(
    update,
    ctx,
    group: StorageGroup,
    node: StorageNode,
    acqs: set[ArchiveAcq],
    listed_files: set[ArchiveFile],
    show_acqs: bool,
    show_files: bool,
    first_time: bool,
):
    """Run a sync cancel.

    This is somewhat easier than a sync, I think.

    Parameters
    ----------
    update:
        False during "check" mode.  True when actually updating.
    ctx:
        click context object
    group:
        StorageGroup destination, or None if not given
    node:
        StorageNode soruce, or None if not given
    acqs:
        list of ArchiveAcqs to limit to.  May be the empty list for no limit
    listed_files:
        set of ArchiveFiles listed in a --file-list file.
    show_acqs:
        List acqusitions affected by the command
    show_files:
        List files affected by the command
    first_time:
        True if this is the first time this function was called.
    """

    # Find all the files currently being synced
    query = ArchiveFileCopyRequest.select(ArchiveFileCopyRequest.id).where(
        ArchiveFileCopyRequest.cancelled == 0, ArchiveFileCopyRequest.completed == 0
    )

    # This takes care of both kinds of --all
    if node is not None:
        query = query.where(ArchiveFileCopyRequest.node_from == node)
    if group is not None:
        query = query.where(ArchiveFileCopyRequest.group_to == group)

    # Limit to listed files, if given
    if listed_files:
        query = query.where(ArchiveFileCopyRequest.file << listed_files)

    # Apply acq constraint, if any
    if acqs:
        query = query.join(ArchiveFile).where(ArchiveFile.acq << acqs)

    afcrs = list(query.scalars())

    # Nothing to do?
    if not afcrs:
        echo("No requests to cancel.")
        ctx.exit()

    # Grammar
    count = len(afcrs)
    requests = "request" if count == 1 else "requests"
    if update:
        verb = "Cancelling"
    else:
        verb = "Would cancel"

    if first_time and (show_acqs or show_files):
        stop = ":\n"
    else:
        stop = "."

    echo(f"{verb} {count} pending transfer {requests}{stop}")

    # Show details if requested, but only once
    if first_time:
        if show_acqs or show_files:
            # Need to fetch the full request in this case
            reqs = ArchiveFileCopyRequest.select().where(
                ArchiveFileCopyRequest.id << afcrs
            )

        if show_acqs:
            acq_counts = {}
            acq_files = defaultdict(list)
            for req in reqs:
                acq = req.file.acq
                acq_counts[acq] = acq_counts.get(acq, 0) + 1
                if show_files:
                    acq_files[acq].append("    " + str(req.file.path))

            for acq in sorted(acq_counts, key=lambda x: x.name):
                requests = "request" if acq_counts[acq] == 1 else "requests"
                echo(f"{acq.name} [{acq_counts[acq]} {requests}]")
                if show_files:
                    echo("\n".join(sorted(acq_files[acq])))
        elif show_files:
            paths = [str(req.file.path) for req in reqs]
            echo("\n".join(sorted(paths)))

    # Do update
    if update:
        count = (
            ArchiveFileCopyRequest.update(cancelled=1)
            .where(ArchiveFileCopyRequest.id << afcrs)
            .execute()
        )

        requests = "request" if count == 1 else "requests"
        echo(f"\nCancelled {count} transfer {requests}.")


def _run_sync(
    update: bool,
    ctx,
    group: StorageGroup,
    node: StorageNode,
    acqs: set[ArchiveAcq],
    listed_files: set[ArchiveFile],
    show_acqs: bool,
    show_files: bool,
    target: list[str],
    first_time: bool,
) -> None:
    """Run a sync (instead of a sync-cancel)

    Parameters
    ----------
    update:
        False during "check" mode.  True when actually updating.
    ctx:
        click context object
    group:
        StorageGroup destination
    node:
        StorageNode soruce
    acqs:
        list of ArchiveAcqs to limit to.  May be the empty list for no limit
    listed_files:
        set of ArchiveFiles listed in a --file-list file.
    show_acqs:
        List acqusitions affected by the command
    show_files:
        List files affected by the command
    target:
        The --target from command line
    first_time:
        True if this is the first time this function was called.
    """

    # Get list of target files
    skipped_files = files_in_groups(resolve_group(target), in_any=True)

    # If there are no target files, set it to the empty list
    if skipped_files is None:
        skipped_files = set()

    # also skip all the files in the dest
    skipped_files |= set(
        ArchiveFileCopy.select(ArchiveFile.id)
        .join(ArchiveFile)
        .switch(ArchiveFileCopy)
        .join(StorageNode)
        .where(StorageNode.group == group, ArchiveFileCopy.has_file == "Y")
        .scalars()
    )

    # Now select all files on the source but not in the skip list
    query = (
        ArchiveFile.select()
        .join(ArchiveFileCopy)
        .where(
            ArchiveFileCopy.node == node,
            ArchiveFileCopy.has_file == "Y",
            ~(ArchiveFileCopy.file_id << skipped_files),
        )
    )

    # Limit to listed files, if given
    if listed_files:
        query = query.where(ArchiveFile.id << listed_files)

    # Limit to acqs, if needed
    if acqs:
        query = query.where(ArchiveFile.acq << acqs)

    # Execute query
    all_files = list(query.order_by(ArchiveFile.id).execute())

    # Nothing to do?
    if not all_files:
        echo("No files to sync.")
        ctx.exit()

    # Find all existing pending transfer between node and group
    active_transfers = set(
        ArchiveFileCopyRequest.select(ArchiveFileCopyRequest.file_id)
        .where(
            ArchiveFileCopyRequest.node_from == node,
            ArchiveFileCopyRequest.group_to == group,
            ArchiveFileCopyRequest.cancelled == 0,
            ArchiveFileCopyRequest.completed == 0,
        )
        .scalars()
    )

    # Find files which already have a pending transfer.  Also compute summary data
    satisfied = []
    sync = []
    acq_counts = {}
    acq_files = defaultdict(list)
    totals = {True: 0, False: 0}
    for file_ in all_files:
        # Separate into files already being synced and those needing a new request
        new_req = file_.id not in active_transfers

        if new_req:
            sync.append(file_)
        else:
            satisfied.append(file_)

        if first_time and show_acqs:
            acq = file_.acq
            acq_counts[acq] = acq_counts.get(acq, 0) + 1
            if show_files:
                acq_files[acq].append("    " + str(file_.path))

        # Once total is None, it stays None
        if totals[new_req] is not None:
            size = file_.size_b
            if size is None:
                totals[new_req] = None  # i.e. we don't know the size
            else:
                totals[new_req] += size

    # All done already?
    if not sync:
        count = len(satisfied)
        files = "file" if count == 1 else "files"
        size = "unknown size" if totals[False] is None else pretty_bytes(totals[False])
        echo(
            f"{count} {files} ({size}) already scheduled for sync "
            f'from Node "{node.name}" to Group "{group.name}"'
        )
        echo("No additional files to sync.")
        ctx.exit()

    if satisfied:
        size = "unknown size" if totals[False] is None else pretty_bytes(totals[False])
        count = len(satisfied)
        files = "file" if count == 1 else "files"
        echo(
            f"{count} {files} ({size}) already scheduled for sync "
            f'from Node "{node.name}" to Group "{group.name}"'
        )

    # Grammar
    size = "unknown size" if totals[True] is None else pretty_bytes(totals[True])
    count = len(sync)
    files = "file" if count == 1 else "files"
    if update:
        verb = "Syncing"
    else:
        verb = "Would sync"

    if first_time and (show_acqs or show_files):
        stop = ":\n"
    else:
        stop = "."

    echo(
        f'{verb} {count} {files} ({size}) from Node "{node.name}" '
        f'to Group "{group.name}"{stop}'
    )

    # Show what's going to happen, but only once
    if first_time:
        if show_acqs:
            for acq in sorted(acq_counts, key=lambda x: x.name):
                files = "file" if acq_counts[acq] == 1 else "files"
                echo(f"{acq.name} [{acq_counts[acq]} {files}]")
                if show_files:
                    echo("\n".join(sorted(acq_files[acq])))
        elif show_files:
            paths = [str(file_.path) for file_ in sync]
            echo("\n".join(sorted(paths)))

    # Run the update, if we're in update mode
    if update:
        now = utcnow()

        new_requests = [
            {
                "file": file_.id,
                "node_from": node,
                "group_to": group,
                "completed": 0,
                "cancelled": 0,
                "timestamp": now,
            }
            for file_ in sync
        ]

        # Do a bulk insert of these new rows
        ArchiveFileCopyRequest.insert_many(new_requests).execute()

        requests = "request" if len(new_requests) == 1 else "requests"
        echo(f"\nAdded {len(new_requests)} new copy {requests}.")


def run_query(
    update: bool,
    ctx,
    group_name: str | None,
    node_name: str | None,
    acq: list,
    cancel: bool,
    listed_files: set[ArchiveFile],
    show_acqs: bool,
    show_files: bool,
    target: list,
    first_time: bool,
):
    """Run the sync query.

    This is also invoked by the "node sync" command.

    Parameters
    ----------
    update:
        False during "check" mode.  True when actually updating.
    ctx:
        click context object
    group_name:
        group name from command-line.  May be None if `--all` was used.
    node_name:
        node name from command-line.  May be None if `--all` was used.
    acq:
        list of --acq from command line
    cancel:
        True if we're cancelling transfers.
    listed_files:
        set of ArchiveFiles listed in a --file-list file.
    show_acqs:
        List acqusitions affected by the command
    show_files:
        List files affected by the command
    target:
        The --target from command line
    first_time:
        True if this is the first time this function was called.
    """

    if node_name is None:
        node = None
    else:
        try:
            node = StorageNode.get(name=node_name)
        except pw.DoesNotExist:
            raise click.ClickException("No such node: " + node_name)

    if group_name is None:
        group = None
    else:
        try:
            group = StorageGroup.get(name=group_name)
        except pw.DoesNotExist:
            raise click.ClickException("No such group: " + group_name)

    # Resolve acqs
    acqs = resolve_acq(acq)

    # Cancel and sync are different enough that they've been broken up
    if cancel:
        _run_cancel(
            update,
            ctx,
            group,
            node,
            acqs,
            listed_files,
            show_acqs,
            show_files,
            first_time,
        )
    else:
        _run_sync(
            update,
            ctx,
            group,
            node,
            acqs,
            listed_files,
            show_acqs,
            show_files,
            target,
            first_time,
        )


@click.command()
@click.argument("group_name", metavar="GROUP")
@click.argument("node_name", metavar="NODE", required=False)
@cli_option("acq")
@cli_option(
    "all_",
    help="Must be used with --cancel.  Cancel _all_ inbound "
    "transfers into GROUP.  NODE must not be specified if this flag "
    "is used.",
)
@cli_option("cancel", help="Cancel pending transfers into GROUP.")
@click.option(
    "--check",
    "-c",
    help="Only check (and print) what would be synced, don't actually do anything."
    "  Incompatible with --force",
    is_flag=True,
)
@cli_option("file_list")
@click.option(
    "--force",
    help="Force update (skips confirmation).  Incompatible with --check",
    is_flag=True,
)
@click.option("--show-acqs", help="List acquisitions to be copied.", is_flag=True)
@click.option("--show-files", help="List files to be copied.", is_flag=True)
@cli_option(
    "target",
    help="May be specified multiple times.  Skip files which already exist in "
    "any specified target groups GROUP.  Cannot be used with --cancel.",
)
@click.pass_context
def sync(
    ctx,
    group_name,
    node_name,
    acq,
    all_,
    cancel,
    check,
    force,
    file_list,
    show_acqs,
    show_files,
    target,
):
    """Copy files into a Storage Group.

    The sync command requests files from the Storage Node NODE be copied
    into the Storage Group GROUP.  The copying will be handled by the daemon
    running on the desination.  Only files which are not already in GROUP will
    be copied.

    Which files are copied may be further limited with the --acq, --file-list and
    --target options.

    \b
    CANCELLING TRANSFERS
    --------------------

    Using the --cancel flag, this command can also be used to cancel pending
    transfers into GROUP.  With a NODE, transfers from NODE to GROUP are cancelled,
    but you can instead of NODE use the special flag --all to cancel all inbound
    transfers into GROUP.  Cancelling can still be further limited by --acq and
    --file-list, but not --target.
    """

    # Usage checks
    not_both(check, "check", force, "force")
    requires_other(all_, "all", cancel, "cancel")
    not_both(cancel, "cancel", target, "target")

    # Must supply NODE or --all, but not both
    if not all_ and node_name is None:
        raise click.UsageError("NODE or --all must be provided.")
    if all_ and node_name is not None:
        raise click.UsageError("Can't use --all with NODE.")

    # Set check mode if --file-list=- was used to redirect stdin and no --force
    check = check_if_from_stdin(file_list, check, force)

    # Load file list, if any
    listed_files = files_from_file(file_list, node_name)

    # Run the check-confirm-update loop
    check_then_update(
        not force,
        not check,
        run_query,
        ctx,
        args=[
            group_name,
            node_name,
            acq,
            cancel,
            listed_files,
            show_acqs,
            show_files,
            target,
        ],
    )
