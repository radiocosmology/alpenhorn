.. Alpenhorn documentation master file, created by
   sphinx-quickstart on Sat Feb 27 14:18:05 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Alpenhorn
=========

Alpenhorn is a set of tools for managing an archive of scientific data across
multiple sites. It was designed for looking after the data from `CHIME
<http://chime.phas.ubc.ca/>`_.

Alpenhorn consists of a service (`alpenhornd`) that manipulates the archive held
at each location, a client (`alpenhorn`) which is used to control the system
(transfers, deletions etc.), and a database which holds the current state of the
system and is used for communication between the different components.

Configuration
-------------

The database holds the current state of the system, and must be manually set up
with the initial set of ``StorageGroup`` and ``StorageNode`` entries. Hopefully there
will be a description of how to do that here at somepoint.

There are currently two configuration parameters that can be set for any running
instance of ``alpenhornd``. They are both set by use of environment variables.

``ALPENHORN_LOG_PATH``
    The path to write out the log file to. If not set, use
    ``/var/log/alpenhorn/alpenhornd.log``. If set to ``""`` (empty string), then
    no log file is written.
``ALPENHORN_IMPORT_RECORD``
    File in which to cache the names of files already imported. Using this will
    save significant start up time with a large archive. If not set, attempt to
    use ``/etc/alpenhornd_import.dat``



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
