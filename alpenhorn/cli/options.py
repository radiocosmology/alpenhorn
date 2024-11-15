"""Common CLI options and option processing code."""

from __future__ import annotations

import json
import click
import peewee as pw
from typing import TYPE_CHECKING

from ..db import ArchiveAcq, ArchiveFile, ArchiveFileCopy, StorageGroup, StorageNode

if TYPE_CHECKING:
    from typing import Any
del TYPE_CHECKING


def cli_option(option: str, **extra_kwargs):
    """Provide common CLI options.

    Returns a click.option decorator for the common CLI option called
    `option`.  Other keyword arguments are passed on to click.option.
    """

    # Set args for the click.option decorator
    if option == "acq":
        args = ("--acq",)
        kwargs = {
            "metavar": "ACQ",
            "default": None,
            "multiple": True,
            "help": "May be specified multiple times.  "
            "Limits operation to files in acquisition(s) named ACQ.",
        }
    elif option == "address":
        args = ("--address",)
        kwargs = {
            "metavar": "ADDR",
            "help": "Domain name or IP address to use for remote access to the "
            "node.",
        }
    elif option == "all_":
        # Has no help; must be provided when used
        args = ("all_", "--all", "-a")
        kwargs = {"is_flag": True}
    elif option == "archive":
        args = ("--archive",)
        kwargs = {
            "is_flag": True,
            "help": "Make node an archive node.  Incompatible with --field "
            "or --transport.",
        }
    elif option == "auto_verify":
        args = ("--auto-verify",)
        kwargs = {
            "metavar": "COUNT",
            "type": int,
            "help": "If COUNT is zero, turn off auto-verify.  If COUNT is "
            "non-zero, turn on auto-verify for the node, with COUNT as the "
            "maximum number of re-verified copies per iteration.",
        }
    elif option == "cancel":
        args = (
            "--cancel",
            "-x",
        )
        kwargs = {"is_flag": True, "help": "Cancel the operation"}
    elif option == "field":
        args = ("--field",)
        kwargs = {
            "is_flag": True,
            "help": "Make node a field node (i.e. neither an archive node "
            "nor a transport node).  Incompatible with --archive or --transport.",
        }
    elif option == "group":
        args = ("--group",)
        kwargs = {
            "metavar": "GROUP",
            "help": "Limit to files in Storage Group named GROUP.",
        }
    elif option == "host":
        args = ("--host",)
        kwargs = {"metavar": "HOST", "help": "The host managing this node."}
    elif option == "io_class":
        args = (
            "io_class",
            "-i",
            "--class",
        )
        kwargs = {
            "metavar": "IO_CLASS",
            "default": None,
            "help": "Set I/O class to IO_CLASS.",
        }
    elif option == "io_config":
        args = ("--io-config",)
        kwargs = {
            "metavar": "CONFIG",
            "default": None,
            "help": "Set I/O config to the JSON object literal CONFIG.  Any "
            "I/O config specified this way may be further modified by "
            "--io-var.  Setting this to nothing (--io-config=) empties the "
            "I/O config.",
        }
    elif option == "io_var":
        args = ("--io-var",)
        kwargs = {
            "metavar": "VAR=VALUE",
            "default": (),
            "multiple": True,
            "help": "Set I/O config variable VAR to the value VALUE.  May be "
            "specified multiple times.  Modifies any config specified by "
            "--io-config.  If VALUE is empty (--io-var VAR=), VAR is deleted "
            "if present.",
        }
    elif option == "max_total":
        args = ("--max-total",)
        kwargs = {
            "metavar": "SIZE",
            "type": float,
            "help": "The maximum allowed size of the node, in GiB",
        }
    elif option == "min_avail":
        args = ("--min-avail",)
        kwargs = {
            "metavar": "SIZE",
            "type": float,
            "help": "The minimum allowed free space on the node, in GiB, "
            "before auto-cleaning happens",
        }
    elif option == "node":
        args = ("--node",)
        kwargs = {
            "metavar": "NODE",
            "help": "Limit to files on Storage Node named NODE.",
        }
    elif option == "notes":
        args = ("--notes",)
        kwargs = {"metavar": "COMMENT", "help": "Set notes to COMMENT."}
    elif option == "root":
        args = ("--root",)
        kwargs = {"metavar": "ROOT", "help": "The node root or mount point."}
    elif option == "target":
        args = ("--target",)
        kwargs = {
            "metavar": "GROUP",
            "multiple": True,
            "help": "May be specified multiple times.  Restrict operation to "
            "files which exist in all specified target groups GROUP.",
        }
    elif option == "transport":
        args = ("--transport",)
        kwargs = {
            "is_flag": True,
            "help": "Make node a transport node.  Incompatible with --archive "
            "or --field.",
        }
    elif option == "username":
        args = ("--username",)
        kwargs = {
            "metavar": "USER",
            "help": "Username to use for remote access to the node.",
        }
    else:
        raise ValueError(f"Unknown option: {option}")

    # Update kwargs, if given
    kwargs.update(extra_kwargs)

    # Default help string for common options missing them
    if "help" not in kwargs:
        kwargs["help"] = (
            "SOMEONE FORGOT TO WRITE SOME HELP FOR THIS OPTION.  GOOD LUCK, I GUESS!"
        )

    def _decorator(func):
        nonlocal args, kwargs

        return click.option(*args, **kwargs)(func)

    return _decorator


def not_both(opt1_set: bool, opt1_name: str, opt2_set: bool, opt2_name: str) -> None:
    """Check whether two incompatible options were used.

    If they were, raise click.UsageError."""

    if opt1_set and opt2_set:
        raise click.UsageError(f"cannot use both --{opt1_name} and --{opt2_name}")


def exactly_one(opt1_set: bool, opt1_name: str, opt2_set: bool, opt2_name: str) -> None:
    """Check that exactly one of two incompatible options were used.

    If not, raise click.UsageError."""

    not_both(opt1_set, opt1_name, opt2_set, opt2_name)

    if not (opt1_set or opt2_set):
        raise click.UsageError(f"missing --{opt1_name} or --{opt2_name}")


def requires_other(
    opt1_set: bool, opt1_name: str, opt2_set: bool, opt2_name: str
) -> None:
    """If opt1 is set, check that opt2 was also set.

    If not, raise click.UsageError."""

    if opt1_set and not opt2_set:
        raise click.UsageError(f"--{opt1_name} may only be used with --{opt2_name}")


def resolve_acqs(acq: list[str]) -> list[ArchiveAcq]:
    """Convert --acq list to ArchiveAcq list.

    If the input list is empty, so is the output list.

    Raises `click.ClickException` if a non-existent acqusition was
    provided.
    """
    acqs = []
    for acqname in acq:
        try:
            acqs.append(ArchiveAcq.get(name=acqname))
        except pw.DoesNotExist:
            raise click.ClickException("No such acquisition: " + acqname)
    return acqs


def files_in_target(target: list[str], in_any: bool = False) -> list[int] | None:
    """Take a --target list and return a list of target files.

    Parameters
    ----------
    target:
        list of group names from --target options.
    in_any:
        if True, file needs to be only in one target.
        If False, file needs to be in all targets.

    Returns
    -------
    target_files:
        If the input list was empty, this is None.  Otherwise,
        it is a list of ArchiveFile.ids which are in any/all target
        groups.  The list may be empty, if no files satisfied the
        constraint.

    Raises
    ------
    click.ClickException:
        a non-exsitent group was found in the target list
    """

    target_files = None
    for name in target:
        # Resolve group name
        try:
            group = StorageGroup.get(name=name)
        except pw.DoesNotExist:
            raise click.ClickException("No such target group: " + name)

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

        if in_any:
            # In "any" mode, run the query and then compute the set union,
            # if we already have a target set
            if target_files:
                target_files |= set(query.scalars())
            else:
                target_files = set(query.scalars())
        else:
            # In "all" mode, intersect the current list with the new query
            # and replace the set with the result
            if target_files:
                query = query.where(ArchiveFile.id << target_files)

            # Execute the query and record the result
            target_files = set(query.scalars())

            # In this mode, there's no reason to continue if the set is empty
            if not target_files:
                return []

    return target_files


def set_storage_type(
    archive: bool, field: bool, transport: bool, none_ok: bool = False
) -> str | None:
    """Set node storage_type.

    Processes the --archive, --field, --transport options.

    Returns one of 'A', 'F', 'T', depending on which of the options is set.

    If none of the options are set, None is returned if `none_ok` is True, otherwise
    the default 'F' is returned.
    """

    # Usage checks
    not_both(archive, "archive", field, "field")
    not_both(archive, "archive", transport, "transport")
    not_both(field, "field", transport, "transport")

    if archive:
        return "A"
    if transport:
        return "T"
    if not field and none_ok:
        return None

    # Default
    return "F"


def set_io_config(
    io_config: str | None, io_var: list | tuple, default: str | dict = {}
) -> dict | None:
    """Set the I/O config from the command line.

    Processes the --io-config and --io-var options.

    Parameters
    ----------
    io_config:
        The --io-config parameter data from click
    io_var:
        The --io-var parameter data from click
    default:
        If --io-config is None, use this as the default value before applying
        io_var edits.  If a string, will be JSON decoded.

    Returns
    -------
    io_config : dict
        The composed I/O config dict, or None if the dict ended up empty.
    """

    # Attempt to compose the I/O config
    if io_config == "":
        # i.e. the user specified "--io-config=" to delete the I/O config
        new_config = {}
    elif io_config:
        try:
            new_config = json.loads(io_config)
        except json.JSONDecodeError:
            raise click.ClickException("Unable to parse --io-config value as JSON.")

        if not isinstance(new_config, dict):
            raise click.ClickException(
                "Argument to --io-config must be a JSON object literal."
            )
    else:
        # Decoding of default is only done if necessary
        if isinstance(default, str):
            try:
                default = json.loads(default)
            except json.JSONDecodeError:
                raise click.ClickException(
                    f"Invalid I/O config in database: {default}."
                )
        new_config = default

    # Add any --io-var data
    for var in io_var:
        split_data = var.split("=", 1)
        if len(split_data) <= 1:
            raise click.ClickException(f'Missing equals sign in "--io-var={var}"')

        if split_data[1] == "":
            # "--io-var VAR=" means delete VAR if present
            new_config.pop(split_data[0], None)
            continue

        # Otherwise, try coercing the value
        try:
            split_data[1] = int(split_data[1])
        except ValueError:
            try:
                split_data[1] = float(split_data[1])
            except ValueError:
                pass

        # Test jsonifibility.  I have not found a way to make click give this function
        # something it can't encode, but I don't think this hurts.
        try:
            json.dumps(dict([split_data]))
        except json.JSONEncodeError:
            raise click.ClickException(f'Cannot parse argument: "--io-var={var}"')

        # Now set
        new_config[split_data[0]] = split_data[1]

    # Return None instead of an empty I/O config
    return new_config if new_config else None
