"""alpenhorn file state command."""

import click
import peewee as pw

from ...db import ArchiveFileCopy, database_proxy
from ..cli import echo
from ..options import file_from_path, not_both, resolve_node


def _update_state(ctx, path, node_name, set_, ready, unready):
    """Update ArchiveFileCopy record.

    This is completely different than the non-update part
    of the command, so let's separate it.
    """
    # Usage
    not_both(ready, "ready", unready, "unready")

    # States present, suspect, and corrupt can't have wants_file='N'
    # (though keeping wants_file='M' is okay).  We record that situation here
    now_present = False

    # Convert --set=STATE into an update dict
    updates = {}
    if set_:
        new_state = set_.lower()
        if new_state == "healthy":
            updates["has_file"] = "Y"
            now_present = True
        elif new_state == "suspect":
            updates["has_file"] = "M"
            now_present = True
        elif new_state == "corrupt":
            updates["has_file"] = "X"
            now_present = True
        elif new_state == "absent":
            updates["has_file"] = "N"
            updates["wants_file"] = "N"
        elif new_state == "missing":
            updates["has_file"] = "N"
            # This is always 'Y'.  A wants_file=='M' file isn't missing:
            # it's been removed.
            updates["wants_file"] = "Y"
        else:
            raise click.UsageError(f"invalid state for --set: {set_}")

        # Can't ready if file isn't present
        if ready:
            if new_state != "healthy":
                raise click.ClickException("can't set ready bit: file not present.")

            updates["ready"] = True
            # So we don't try to set it again
            ready = False

    with database_proxy.atomic():
        file = file_from_path(path)
        node = resolve_node(node_name)

        # Find the existing record, if any
        try:
            copy = ArchiveFileCopy.get(file=file, node=node)
        except pw.DoesNotExist:
            copy = None

        # Set wants_file if the file is now present
        if (not copy or copy.wants_file == "N") and now_present:
            updates["wants_file"] = "Y"

        if ready:
            # We can only ready a file if it's present
            if not copy or copy.has_file != "Y":
                raise click.ClickException("can't set ready bit: file not present.")
            updates["ready"] = True
        elif unready:
            updates["ready"] = False

        # Need to create new record
        if not copy:
            # Only create a new record if needed:
            if "has_file" not in updates or (
                updates["has_file"] == "N" and updates["wants_file"] == "N"
            ):
                echo("No change.")
                ctx.exit()

            # Create new
            ArchiveFileCopy.create(
                file=file,
                node=node,
                has_file=updates["has_file"],
                wants_file=updates["wants_file"],
                ready=updates.get("ready", False),
            )
            echo("State updated.")
        else:
            # Otherwise, update
            if (
                ArchiveFileCopy.update(**updates)
                .where(ArchiveFileCopy.id == copy.id)
                .execute()
            ):
                echo("State updated.")
            else:
                echo("No change.")

    # Done
    ctx.exit()


@click.command()
@click.argument("path", metavar="FILE")
@click.argument("node_name", metavar="NODE")
@click.option("set_", "--set", metavar="STATE", help="Set the state of FILE on NODE")
@click.option("--ready", is_flag=True, help="Set the ready bit for the FILE on NODE")
@click.option(
    "--unready", is_flag=True, help="Clear the ready bit for the FILE on NODE"
)
@click.pass_context
def state(ctx, path, node_name, set_, ready, unready):
    """Show or update the state of a File.

    Without other arguments, this command shows the state of the File FILE on
    the Node NODE.  FILE should be specified as "<acq_name>/<file_name>".
    This will print one or more words indicating the state of the file.

    The first word indicates the file's state, which will be one of these
    state words:

    \b
    * Healthy:   there is a good copy of FILE on NODE.
    * Suspect:   there is a copy of FILE on NODE which needs to be
                 reverified by the daemon to determine whether it is
                 healthy or corrupt.
    * Corrupt:   there is a corrupt copy of FILE on NODE.
    * Missing:   there used to be a copy of FILE on NODE, but it's now
                 absent even though it was never marked for cleaning.
    * Absent:    there is no copy of FILE on NODE.

    This state may be followed by zero or more extra words, which provide
    additional information:

    \b
    * Removable: the FILE on NODE has been marked for discretionary cleaning
    * Released:  the FILE on NODE has been released for immediate removal.
    * Ready:     the FILE's "ready" bit is set on NODE.  See the ready bit
                 section below for an important caveat on whether this is useful
                 information or not.

    \b
    Changing the state
    ------------------

    You can change the recorded state of FILE on NODE by using the "--set" option.
    Pass to "--set" one of the five state words above (Healthy, Suspect, Corrupt,
    Missing, Absent), disregarding case.

    To modify the "Ready" bit, you can also use the --ready or --unready options,
    though, again, see the section below on the potential usefulness of this.

    To change the cleaning state of a file (i.e. to set or clear "Removable" or
    "Released"), use the "file clean" command.

    Note: Changing the state of a File with this command will not affect an
    actual copy of the File on a Node at all.  This command is only useful for
    correcting the recorded state of a File on a Node in the Data Index in cases
    where it is incorrect and cannot be automatically corrected by the daemon.

    \b
    The Ready Bit
    -------------

    For every copy of a File on a Node, alpenhorn stores in the Data Index a bit
    called the "ready" bit.  For each File, one bit is stored per Node.

    *Some* Node I/O classes use this bit to determine if a file is ready for I/O
    operations (reading or copying), but *most* Node I/O classes do not, including
    the "Default" I/O class, the one used for regular filesystems.  For such Nodes,
    whether the ready bit is set or not is not indicative of anything.
    """

    # If any of the options are used, then we're in update mode
    if set_ or ready or unready:
        # Does not return
        _update_state(ctx, path, node_name, set_, ready, unready)

    # Find the ArchiveFileCopy
    file = file_from_path(path)
    node = resolve_node(node_name)

    try:
        copy = ArchiveFileCopy.get(file=file, node=node)
        state = copy.state

        # We list has_file state separately
        if state == "Released":
            if copy.has_file == "Y":
                state = "Healthy"
            elif copy.has_file == "M":
                state = "Suspect"
            else:
                state = "Corrupt"
        elif state == "Removable":
            state = "Healthy"
        # "Removed" is not reported by this function.
        elif state == "Removed":
            state = "Absent"

        # Add extra words
        if copy.has_file != "N":
            if copy.wants_file == "N":
                state += " Released"
            elif copy.wants_file == "M":
                state += " Removable"

            if copy.ready:
                state += " Ready"
    except pw.DoesNotExist:
        copy = None
        state = "Absent"

    echo(state)
