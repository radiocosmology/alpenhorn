"""Alpenhorn CLI entry point."""

from __future__ import annotations

import click

from ..common import config
from ..common.util import help_config_option, start_alpenhorn, version_option
from . import acq, db, file, group, node
from .options import not_both


def _verbosity_from_cli(verbose: int, debug: bool, quiet: int) -> int:
    """Get CLI verbosity from command line.

    Processes the --verbose, --debug and --quiet flags to determine
    the requested verbosity."""

    not_both(quiet > 0, "quiet", verbose > 0, "verbose")
    not_both(quiet > 0, "quiet", debug, "debug")

    # Default verbosity is 3.  --quiet decreases it.  --verbose increases it.

    # Max verbosity
    if debug or verbose > 2:
        return 5
    # Min verbosity
    if quiet > 2:
        return 1

    return 3 + verbose - quiet


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@version_option
@click.option(
    "--conf",
    "-c",
    type=click.Path(exists=True),
    help="Configuration file to read.",
    default=None,
    metavar="FILE",
)
@click.option(
    "--quiet",
    "-q",
    help="Decrease verbosity.  May be specified mulitple times: "
    "once suppresses normal CLI output, leaving only warning "
    "and error message.  A second use also suppresses warnings.",
    count=True,
)
@click.option(
    "--test-isolation",
    is_flag=True,
    help=(
        "Enable test isolation.  Using this option prevents alpenhorn "
        "from reading config from the standard config paths."
    ),
)
@click.option(
    "--verbose",
    "-v",
    help="Increase verbosity.  May be specified mulitple times: "
    "once enables informational messages.  A second use also "
    "enables debugging messages.",
    count=True,
)
@click.option(
    "--debug",
    help="Maximum verbosity.",
    is_flag=True,
    show_default=False,
    default=False,
)
@help_config_option
def entry(conf, quiet, test_isolation, verbose, debug):
    """Alpenhorn data management system.

    This is the command-line interface to the alpenhorn data index.
    """

    # Turn on test isolation, if requested
    config.test_isolation(enable=test_isolation)

    # Initialise alpenhorn
    start_alpenhorn(
        conf, cli=True, verbosity=_verbosity_from_cli(verbose, debug, quiet)
    )


entry.add_command(acq.cli, "acq")
entry.add_command(db.cli, "db")
entry.add_command(file.cli, "file")
entry.add_command(group.cli, "group")
entry.add_command(node.cli, "node")
