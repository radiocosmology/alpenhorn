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

    db: peewee_url

    logging:
        file:   alpenhorn.log
        level:  debug

    extensions:
        - alpenhorn.generic
        - alpenhorn_chime

    acq_types:
        generic:
            patterns:
                - ".*/.*"

    file_types:
        generic:
            patterns:
                - ".*\.h5"
                - ".*\.log"

"""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import


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
        cfile = os.path.abspath(os.path.expanduser(os.path.expandvars(cfile)))

        if not os.path.exists(cfile):
            continue

        any_exist = True

        with open(cfile, 'r') as fh:
            conf = yaml.safe_load(fh)

        configdict.update(conf)

    if not any_exist:
        raise RuntimeError("No configuration files available.")


class ConfigClass(object):
    """A base for classes that can be configured from a dictionary.

    Note that this configures the class itself, not instances of the class.
    """

    @classmethod
    def set_config(self, configdict):
        """Configure the class from the supplied `configdict`.
        """
        pass
