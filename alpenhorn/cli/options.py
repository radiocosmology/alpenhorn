"""Common CLI options and option processing code."""

from __future__ import annotations

import json
import click
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
del TYPE_CHECKING


def cli_option(option: str, **extra_kwargs):
    """Provide common CLI options.

    Returns a click.option decorator for the common CLI option called
    `option`.  Other keyword arguments are passed on to click.option.
    """

    # Set args for the click.option decorator
    if option == "address":
        args = ("--address",)
        kwargs = {
            "metavar": "ADDR",
            "help": "Domain name or IP address to use for remote access to the "
            "node.",
        }
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
