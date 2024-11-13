"""Alpenhorn CLI entry point."""

from __future__ import annotations

import click

from .. import db
from ..common.util import start_alpenhorn, version_option

from . import acq, group, node, transport
from .options import not_both


def _verbosity_from_cli(verbose: int, debug: bool, quiet: int) -> int:
    """Get CLI verbosity from command line.

    Processes the --verbose, --debug and --quiet flags to determine
    the requested verbosity."""

    not_both(quiet, "quiet", verbose, "verbose")
    not_both(quiet, "quiet", debug, "debug")

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
def entry(conf, quiet, verbose, debug):
    """Alpenhorn data management system.

    This is the command-line interface to the alpenhorn data index.
    """

    # Initialise alpenhorn
    start_alpenhorn(
        conf, cli=True, verbosity=_verbosity_from_cli(verbose, debug, quiet)
    )


entry.add_command(acq.cli, "acq")
entry.add_command(group.cli, "group")
entry.add_command(node.cli, "node")
entry.add_command(transport.cli, "transport")
