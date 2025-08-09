Code Reference
==============

.. module:: alpenhorn

While Alpenhorn is primarily designed to be used via its daemon and CLI, it
is still possible, if perhaps somewhat convoluted, to use alpenhorn
interactively as if it were a normal Python package.

To use the alpehorn system interactively, first initialise the alpenhorn system
like this:

.. code:: python

    from alpenhorn.common.util import start_alpenhorn
    from alpenhorn.db import connect, schema_version

    # This call initialises alpenhorn.  It takes care of loading the alpenhorn
    # configuration file(s) and then importing any necessary alpenhorn extensions.
    #
    # If no extra config file is needed, the first parameter may be None.  Setting
    # cli to True here is necessary to stop your Python session from trying to log
    # to the alpenhorn daemon logger.
    #
    # The verbosity parameter controls which messages the alpenhorn system will
    # output.  Verbosity can range from 1 (least verbose) to 5 (most verbose).
    # The default is 3.  
    start_alpenhorn("/extra/config/file", cli=True, verbosity=3)

    # Once the configuration has been loaded and extensions have been imported,
    # then a database connection can be established.
    connect()

    # If you're planning on performing writes on the data index, you should now
    # check the data index's version, to ensure you're using the correct alpenhorn
    # version for the database you've connected to.  Even if you're only planning
    # read-only access, version checking is still a good idea, if any uncertainty
    # exists
    schema_version(check=True)

Module Reference
----------------

.. toctree::
   :maxdepth: 3

   cli
   common
   daemon
   db
