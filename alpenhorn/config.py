"""For configuring alpenhorn from the config file.

Example config:

.. codeblock:: yaml

    db: peewee_url

    logging:
        file:   alpenhorn.log
        level:  debug

    use_generic: Yes

    extensions:
        alpenhorn_chime

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


def load_config():
    raise NotImplementedError()


class ConfigClass(object):
    """A base for classes that can be configured from a dictionary.

    Note that this configures the class itself, not instances of the class.
    """

    @classmethod
    def set_config(self, configdict):
        """Configure the class from the supplied `configdict`.
        """
        pass
