"""alpenhorn db init command"""

import click

from ...db import (
    DataIndexVersion,
    current_version,
    database_proxy,
    gamut,
    schema_version,
)


@click.command()
@click.pass_context
def init(ctx):
    """Initialise the Data Index.

    This command will create all the database table required
    by the Alpenhorn Data Index.  Pre-existing tables will not
    be overwritten.

    The tables created here are required for most of alpenhorn's
    functionality.
    """

    # Check database schema version
    vers = schema_version()

    # Are we already good?
    if vers == current_version:
        click.echo("Data Index already initialised.")
        ctx.exit(0)

    # Version mismatch.  This needs to be solved with schema migration.
    if vers > 0:
        raise click.ClickException(f"Data Index version {vers} already present.")

    # No schema version, check the database for existing tables
    dbtables = database_proxy.get_tables()

    # "Data Index version 1" refers to CHIME's (former) data index.  No
    # one should encounter that anymore.
    for table in gamut:
        if table._meta.table_name in dbtables:
            raise click.ClickException(
                "Partially initialsed Data Index (or Data Index version 1) found.  "
                "Manual repair required."
            )

    # No data index tables exist.  We should be good to create the database.
    database_proxy.create_tables(gamut)

    # Set schema version
    DataIndexVersion.create(component="alpenhorn", version=current_version)
    click.echo(f"Data Index version {current_version} initialised.")
