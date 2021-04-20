"""Alpenhorn client database initialization functions"""

from alpenhorn import config, extensions, db


def config_connect():
    """Load the config, start the database and register extensions.
    """
    # Load the configuration and initialise the database connection
    config.load_config()
    extensions.load_extensions()
    db.config_connect()

    # Register the acq/file type extensions
    extensions.register_type_extensions()
