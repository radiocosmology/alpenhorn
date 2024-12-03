"""alpenhorn file sync command."""

import click

from ...db import ArchiveFileCopyRequest, database_proxy
from ..cli import echo
from ..options import cli_option, file_from_path, resolve_group, resolve_node


@click.command()
@click.argument("path", metavar="FILE")
@click.option("--cancel", is_flag=True, help="Cancel existing transfer requests.")
@click.option(
    "--force",
    is_flag=True,
    help="Create the transfer request even if the source Node does "
    "not have a copy of the file.  Ignored if used with --cancel.",
)
@cli_option(
    "from_", help="Source Node for the transfer request.  Required if not cancelling."
)
@cli_option(
    "to",
    help="Destination Group for the transfer request.  Required if not cancelling.",
)
@click.pass_context
def sync(ctx, path, cancel, force, from_, to):
    """Create or cancel a File transfer.

    This command allows you to either create a new request to transfer
    a file from a Node to a Group, or else lets you cancel existing
    pending transfer requests for FILE.  The FILE should be specified as
    "<acq_name>/<filename>".

    \b
    New Requests
    ------------

    Wihtout "--cancel", this command creates a request for the daemon to
    transfer the file named FILE from the Node SORUCE_NODE into the
    destination Group DEST_GROUP.  In this case, both "--from" and "--to"
    are required.  The File must not already be present in the destination
    Group.

    By default, the command will only create the request if the file exists
    on the source.  To skip this check, and make the request anyways use the
    "--force" flag.  In this case, the daemon may be unable to complete such
    a request, and may cancel it, if it can't find the source file when it
    tries to handle the request.

    \b
    Cancelling Requests
    -------------------

    To cancel pending transfer requests for FILE use the "--cancel" flag.
    Which requests are cancelled may be limited with the "--from" and "--to"
    flags.  By default, all exisingt pending requests for FILE are cancelled.
    """

    # Usage checks: must use --cancel or else both --from AND --to
    if not cancel:
        if not from_:
            raise click.UsageError("missing --from")
        if not to:
            raise click.UsageError("missing --to")

    if cancel:
        # Cancel mode.  Find matching requests and cancel them.
        with database_proxy.atomic():
            file_ = file_from_path(path)
            query = ArchiveFileCopyRequest.update(cancelled=True).where(
                ArchiveFileCopyRequest.file == file_,
                ArchiveFileCopyRequest.cancelled == 0,
                ArchiveFileCopyRequest.completed == 0,
            )

            if from_:
                query = query.where(
                    ArchiveFileCopyRequest.node_from == resolve_node(from_)
                )
            if to:
                query = query.where(
                    ArchiveFileCopyRequest.group_to == resolve_group(to)
                )

            count = query.execute()

        if count == 1:
            echo("Cancelled 1 request.")
        elif count:
            echo(f"Cancelled {count} requests.")
        else:
            echo("No requests to cancel.")
        ctx.exit()

    # Create mode.
    with database_proxy.atomic():
        file_ = file_from_path(path)
        src = resolve_node(from_)
        dest = resolve_group(to)

        # Is the file already in the dest?
        state, node = dest.state_on_node(file_)
        if state == "Y":
            echo(
                f'Not done: {path} already present on Node "{node.name}" '
                f'in destination Group "{to}".'
            )
            ctx.exit()

        # Does the source file exist?
        if src.filecopy_state(file_) != "Y":
            if force:
                echo("File missing from source: forcing request.")
            else:
                echo("Not done: File missing from source.")
                ctx.exit()

        # Create request
        ArchiveFileCopyRequest.create(
            file=file_, node_from=src, group_to=dest, completed=0, cancelled=0
        )
        echo("Request submitted.")
