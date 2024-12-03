"""alpenhorn file show command."""

import click

from ...db import ArchiveFile, ArchiveFileCopy, database_proxy
from ..cli import echo, update_or_remove
from ..options import cli_option, file_from_path, validate_md5


@click.command()
@click.argument("path", metavar="FILE")
@cli_option("md5")
@click.option("--no-reverify", is_flag=True, help="Don't reverify File copies on Nodes")
@cli_option("size")
@click.pass_context
def modify(ctx, path, md5, no_reverify, size):
    """Change File details.

    This command can be used to change the MD5 hash
    and/or recorded size of the File given by the path FILE
    in the data index.  FILE should be specified as
    "acq-name/filename".

    By default, after updating a File record, all extant
    copies of the File will be marked as needing reverification.
    This is to ensure that all copies of the File marked as
    healthy have the correct new MD5/size.

    You can skip this re-verification step if necessary by
    using the -no-reverify flag.
    """

    # It's a usage error to not provide anything to modify
    if md5 is None and size is None:
        raise click.UsageError("at least of --md5 or --size must be used.")

    # Size must be non-negative
    if size is not None and size < 0:
        raise click.ClickException("negative file size.")

    # Check MD5
    validate_md5(md5)

    with database_proxy.atomic():
        file_ = file_from_path(path)

        updates = {}
        if md5 is not None:
            # The "remove" part can't happen because an empty string will fail
            # the validate_md5 check.
            updates |= update_or_remove("md5sum", md5, file_.md5sum)
        if size != file_.size_b:
            updates["size_b"] = size

        if not updates:
            echo("No change.")
            ctx.exit()

        # Do the update
        ArchiveFile.update(**updates).where(ArchiveFile.id == file_.id).execute()

        # Re-verfiy if, not prohibited
        if not no_reverify:
            # This re-verifies both good ('Y') copies and corrupt ('X') ones.
            count = (
                ArchiveFileCopy.update(has_file="M")
                .where(
                    ArchiveFileCopy.file == file_,
                    ArchiveFileCopy.has_file != "N",
                    ArchiveFileCopy.wants_file != "N",
                )
                .execute()
            )

            if count:
                copies = "copy" if count == 1 else "copies"
                echo(f"File updated.  {count} {copies} will be re-verified.")
            else:
                echo("File updated.  No additional copies need re-verification.")
        else:
            echo("File updated.  Re-verification skipped.")
