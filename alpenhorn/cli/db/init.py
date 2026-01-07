"""alpenhorn db init command"""

import click

from ...db import data_index, database_proxy


@click.command()
@click.option(
    "-o",
    "--only",
    metavar="COMP",
    help="Only initialise Data Index component COMP.  For a list of "
    'supported components, use the "db version" command.  If COMP is '
    '"alpenhorn", the Data Index proper will be initialised, without '
    'initialising any third-party extensions.  If COMP is not "alpenhorn", '
    "the Data Index proper must already exist.",
)
def init(only):
    """Initialise the Data Index.

    This command will create all the database tables in the the Alpenhorn
    Data Index, plus any tables implemented by loaded Data Index Extensions.
    Pre-existing tables will not be overwritten.

    The tables created here are required for most of alpenhorn's
    functionality.
    """

    # Generate the dict of components
    if only:
        ext = data_index.extension_for_component(only)
        # Complain if asked to initialise an unsupported component
        if not ext:
            raise click.ClickException(f"Unsupported component: {only}")

        # If the data index proper has not been created, third-party
        # extension init is not allowed (because there will be no
        # DataIndexVersion table to record them).
        if only != "alpenhorn" and not data_index.schema_version(
            check=True, return_check=True
        ):
            raise click.ClickException(
                f'Unable to create component "{only}": Data Index missing'
            )

        components = {only: ext}
    else:
        components = data_index.all_components()

    # A flag to check whether something happened or not
    did_nothing = True

    # Was there an error?
    saw_error = False

    # This will hold the list of all tables in the Data Index database, if needed
    dbtables = None

    # Loop through components:
    for comp, ext in components.items():
        # Name this component (for user feedback later)
        name = "Data Index" if comp == "alpenhorn" else f'Component "{comp}"'

        # Get the current version for this component
        vers = data_index.schema_version(component=comp)

        # Check if already initialised
        if vers == ext.schema_version:
            continue

        # Version mismatch.  This needs to be solved with schema migration,
        # which we don't yet(?) support.
        if vers > 0:
            raise click.ClickException(f"{name} version {vers} already present.")

        # Otherwise, fetch tables, if needed
        if dbtables is None:
            dbtables = database_proxy.get_tables()

        # Loop through tables in this component to look for a partially-created
        # component
        for table in ext.tables:
            if table._meta.table_name in dbtables:
                raise click.ClickException(
                    f"Partially initialsed {name} found.  Manual repair required."
                )

        # This component doesn't exist, so try to create it:
        with database_proxy.atomic():
            database_proxy.create_tables(ext.tables)

            # Set schema version
            data_index.DataIndexVersion.create(
                component=comp, version=ext.schema_version
            )

            # Run the post-init hook, if any
            if ext.post_init:
                try:
                    ext.post_init()
                except RuntimeError as e:
                    click.echo(f"Error: post-init for {name} failed: {e}")
                    database_proxy.rollback()
                    saw_error = True
                    continue

        click.echo(f"{name} version {ext.schema_version} initialised.")
        did_nothing = False

    # Was there an error
    if saw_error:
        raise click.ClickException("One or more components failed to initialise.")

    # If we did nothing, let the user know that.
    if did_nothing:
        click.echo("Data Index already initialised.")
