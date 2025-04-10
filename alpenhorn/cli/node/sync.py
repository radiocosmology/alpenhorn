"""alpenhorn node sync command

This is just an alias of alpenhorn.group.sync (q.v.)
"""

import click

from ..cli import check_then_update
from ..group.sync import run_query
from ..options import (
    check_if_from_stdin,
    cli_option,
    files_from_file,
    not_both,
    requires_other,
)


@click.command()
@click.argument("node_name", metavar="NODE")
@click.argument("group_name", metavar="GROUP", required=False)
@cli_option("acq")
@cli_option(
    "all_",
    help="Must be used with --cancel.  Cancel _all_ outbound "
    "transfers from NODE.  GROUP must not be specified if this flag "
    "is used.",
)
@cli_option("cancel", help="Cancel pending transfers from NODE.")
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
    node_name,
    group_name,
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
    """Copy files off a Storage Node.

    The sync command requests files from the Storage Node NODE be copied
    into the Storage Group GROUP.  The copying will be handled by the daemon
    running on the desination.  Only files which are not already in GROUP will
    be copied.

    Which files are copied may be further limited with the --acq, --file-list, and
    --target options.

    \b
    CANCELLING TRANSFERS
    --------------------

    Using the --cancel flag, this command can also be used to cancel pending
    transfers out of NODE.  With a GROUP, transfers from NODE to GROUP are cancelled,
    but you can instead of GROUP use the special flag --all to cancel all outbound
    transfers from NODE.  Cancelling can still be further limited by --acq and
    --file-list, but not --target.
    """

    # Usage checks
    not_both(check, "check", force, "force")
    requires_other(all_, "all", cancel, "cancel")
    not_both(cancel, "cancel", target, "target")

    # Must supply GROUP or --all, but not both
    if not all_ and group_name is None:
        raise click.UsageError("GROUP or --all must be provided.")
    if all_ and group_name is not None:
        raise click.UsageError("Can't use --all with GROUP.")

    # Set check mode if --file-list=- was used to redirect stdin and no --force
    check = check_if_from_stdin(file_list, check, force)

    # Load file list, if any
    listed_files = files_from_file(file_list, node_name)

    # We're using the "group sync" run_query function here, which is why
    # group_name and node_name are backwards
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
