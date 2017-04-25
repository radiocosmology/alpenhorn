"""For configuring alpenhorn from the config file.

Configuration file search order:

- `/etc/alpenhorn/alpenhorn.conf`
- `/etc/xdg/alpenhorn/alpenhorn.conf`
- `~/.config/alpenhorn/alpenhorn.conf`
- `ALPENHORN_CONFIG_FILE` environment variable

This is in order of increasing precendence, with options in later files
overriding those in earlier entries.

Example config:

.. codeblock:: yaml

    # Configure the data base connection with a peewee db_url
    database:
        url:    peewee_url

    # Logging configuration
    logging:
        file:   alpenhorn.log
        level:  debug

    # Specify extensions as a list of fully qualified references to python packages or modules
    extensions:
        - alpenhorn.generic
        - alpenhorn_chime

    # Set any configuration for acquisition type extensions
    acq_types:
        generic:
            patterns:
                - ".*/.*"

    # Set any configuration for file type extensions
    file_types:
        generic:
            patterns:
                - ".*\.h5"
                - ".*\.log"

"""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

# Setup the logging
from . import logger
log = logger.get_log()

configdict = None

_default_config = {}


def load_config():
    """Find and load the configuration from a file.
    """

    global configdict

    import os
    import yaml

    # Initialise and merge in any default configuration
    configdict = {}
    configdict.update(_default_config)

    # Construct the configuration file path
    config_files = [
        '/etc/alpenhorn/alpenhorn.conf',
        '/etc/xdg/alpenhorn/alpenhorn.conf',
        '~/.config/alpenhorn/alpenhorn.conf',
    ]

    if 'ALPENHORN_CONFIG_FILE' in os.environ:
        config_files.append(os.environ['ALPENHORN_CONFIG_FILE'])

    any_exist = False

    for cfile in config_files:

        # Expand the configuration file path
        absfile = os.path.abspath(os.path.expanduser(os.path.expandvars(cfile)))

        if not os.path.exists(absfile):
            continue

        any_exist = True

        log.info('Loading config file %s', cfile)

        with open(absfile, 'r') as fh:
            conf = yaml.safe_load(fh)

        configdict.update(conf)

    if not any_exist:
        raise RuntimeError("No configuration files available.")


class ConfigClass(object):
    """A base for classes that can be configured from a dictionary.

    Note that this configures the class itself, not instances of the class.
    """

    @classmethod
    def set_config(cls, configdict):
        """Configure the class from the supplied `configdict`.
        """
        pass
