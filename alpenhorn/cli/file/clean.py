"""alpenhorn file clean command."""

import click

from ...db import ArchiveFileCopy, database_proxy
from ..cli import echo
from ..options import at_least_one, cli_option, file_from_path, not_both, resolve_node


@click.command()
@click.argument("path", metavar="FILE")
@cli_option("archive_ok")
@cli_option("cancel", help="Cancel existing cleaning requests.")
@cli_option("node", help="Update the file on Node NODE.")
@click.option("--now", "-n", help="Force immediate removal.", is_flag=True)
@click.pass_context
def clean(ctx, path, archive_ok, cancel, node, now):
    """Remove a File from a Node.

    This command will schedule the File called FILE for cleaning, or cancel
    an existing cleaning, if the --cancel flag is used.  FILE should be
    specified as "<acq_name>/<filename>".

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

    By default, alpenhorn will refuse to clean files from an archive node.
    This restriction may be overridden with the "--archive-ok" flag.

    When using --cancel to cancel cleaning, both kinds of cleaning
    (discretionary and immediate) will be cancelled, but only if FILE has not
    yet been removed by the daemon.  When cancelling, --node may be omitted,
    in which case all pending cleaning requests are cancelled for FILE.
    """

    # Usage checks: must use --node or --cancel (not exclusively)
    at_least_one(cancel, "cancel", node, "node")
    not_both(cancel, "cancel", now, "now")

    # --cancel and --now set the target value of wants_file:
    if cancel:
        wants = "Y"
    elif now:
        wants = "N"
    else:
        wants = "M"

    with database_proxy.atomic():
        if node:
            # Check node
            node = resolve_node(node)

            if node.archive and not cancel:
                if archive_ok:
                    echo(f'Node "{node.name}" is an archive node: forcing clean.')
                else:
                    echo(f'Not done: Node "{node.name}" is an archive node.')
                    ctx.exit()

        # Update
        query = ArchiveFileCopy.update(wants_file=wants).where(
            ArchiveFileCopy.file == file_from_path(path)
        )

        # Add node, which may be omitted in cancel mode
        if node:
            query = query.where(ArchiveFileCopy.node == node)

        count = query.execute()

    if count:
        if cancel:
            if node:
                echo(f'Cancelled cleaning for "{path}" on Node "{node.name}".')
            else:
                requests = "request" if count == 1 else "requests"
                echo(f'Cancelled {count} cleaning {requests} for "{path}".')
        elif now:
            echo(f'Released "{path}" for immediate removal on Node "{node.name}".')
        else:
            echo(f'Marked "{path}" for discretionary cleaning on Node "{node.name}".')
    else:
        echo("No change.")
