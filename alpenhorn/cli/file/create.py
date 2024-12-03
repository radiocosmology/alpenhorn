"""alpenhorn file create command."""

import pathlib

import click
import peewee as pw

from ...common.util import invalid_import_path, md5sum_file
from ...db import ArchiveFile, database_proxy
from ..cli import echo
from ..options import (
    both_or_neither,
    cli_option,
    not_both,
    requires_other,
    resolve_acq,
    validate_md5,
)


@click.command()
@click.argument("name", metavar="NAME")
@click.argument("acq_name", metavar="ACQ")
@click.option(
    "--from-file",
    is_flag=True,
    help="Scan a local copy of the file to compute MD5 and size.",
)
@cli_option("md5")
@click.option(
    "--prefix",
    metavar="PREFIX",
    help="Use with --from-file to specify the location of the file to scan.",
)
@cli_option("size")
def create(name, acq_name, from_file, md5, prefix, size):
    """Create a new File record.

    This command adds a File named NAME to the Acquisition named ACQ.
    ACQ must not already contain a File named NAME.

    When creating the record, you must provide both the File's size in bytes
    and its 128-bit MD5 hash value.  There are two ways of doing this:

    The first way is to manually set them with the "--md5" and "--size"
    options.

    The second way is to use the "--from-file" flag to ask the CLI to
    compute them by scanning a local copy of the file.  In this case you may,
    optionally, use the "--prefix=PREFIX" option to tell alpenhorn where to
    look for the file:

    If "--prefix" is used, alpenhorn will assume the file is at the path
    "PREFIX/ACQ/NAME".  PREFIX may be relative or absolute.  If relative,
    it's taken to be relative to the current working directory.  If
    PREFIX is not given, it's assumed to be "." (the current working directory).

    If "--from-file" is used, but an error occurs trying to scan the file, no file
    record will be created.

    Note: this command does _not_ associate the new file with an Storage Node,
    even if "--from-file" and "--prefix" were used to point alpenhorn to a copy of
    the file inside a local Storage Node tree.

    If the file _is_ already present in a Storage Node, it's generally easier to
    request the daemon import the file, rather than manually creating the file entry
    via command line, by runnning:

    \b
    file import ACQ/NAME NODE --register-new

    instead of using this command.  Importing files is the recommended way to create
    new File records in almost all cases.
    """

    # Usage checks

    # Must have either --from-file or (--md5 AND --size), but not both
    both_or_neither(md5, "md5", size, "size")
    not_both(from_file, "from-file", md5, "md5")
    if md5 is None and not from_file:
        raise click.UsageError("missing either --from-file or both --md5 and --size")

    # Can't use --prefix without --from-file
    requires_other(prefix, "prefix", from_file, "from_file")

    # Size must be non-negative
    if size is not None and size < 0:
        raise click.ClickException("negative file size.")

    # Validate
    rejection_reason = invalid_import_path(name)
    if rejection_reason:
        raise click.ClickException(f"invalid name: {rejection_reason}")

    validate_md5(md5)

    # Scan a file, if requested
    if from_file:
        # This is an early check to make sure we can create the file to save us from
        # MD5-ing the file and then failing to create the record because it already
        # exists.  We'll have to do this check again when creating the file (to ensure
        # database consistency).
        with database_proxy.atomic():
            # Resolve acq
            acq = resolve_acq(acq_name)

            # Check that "name" isn't already a file in acq
            try:
                ArchiveFile.get(name=name, acq=acq)
                raise click.ClickException(
                    f'the File "{acq_name}/{name}" already exists'
                )
            except pw.DoesNotExist:
                pass

        if prefix:
            local_path = pathlib.Path(prefix).joinpath(acq_name).joinpath(name)
        else:
            local_path = pathlib.Path(acq_name).joinpath(name)

        # Note: the CLI explicitly does NOT use the I/O framework.  The
        # CLI always assumes it's reading a "normal" file on a "normal" filesystem.

        # Reject weird stuff
        if not local_path.exists():
            raise click.ClickException(f"no such file: {local_path}")
        if not local_path.is_file():
            raise click.ClickException(f"{local_path} is not a regular file")

        # Stat it to get the size.
        try:
            size = local_path.stat().st_size
        except OSError as e:
            raise click.ClickException(f"unable to stat {local_path}: {e}") from e

        # Now MD5 it
        try:
            md5 = md5sum_file(str(local_path))
        except OSError as e:
            raise click.ClickException(f"failed to hash {local_path}: {e}") from e

        if md5 is None:
            raise click.ClickException(f"failed to hash {local_path}")

    # Create the ArchiveFile
    with database_proxy.atomic():
        # Resolve acq
        acq = resolve_acq(acq_name)

        # Check that "name" isn't already a file in acq.  We _may_ have already
        # done this once, but we need to do it again inside this transaction.
        try:
            ArchiveFile.get(name=name, acq=acq)
            raise click.ClickException(f'the File "{acq_name}/{name}" already exists')
        except pw.DoesNotExist:
            pass

        ArchiveFile.create(name=name, acq=acq, md5sum=md5, size_b=size)

        echo(f"Registered File {acq_name}/{name}.")
