.. Alpenhorn documentation master file

Alpenhorn
=========

Alpenhorn is system for for managing an archive of scientific data files across
multiple, independent sites. Alpenhorn was originally designed to manage the
data produced by `The Canadian Hydrogen Intensity Mapping Experiment
<https://chime-experiment.ca/>`_.

Excluding the data archives themselves, the Alpenhorn system consists of three
parts:

* **The Data Index**, a SQL database containing both information tracking the
  data files being managed by alpenhorn, and also configuration information
  about the alpenhorn system itself.
* **The Alpenhorn Daemon**, an executable (`alpenhornd`) designed to be
  long-running which is responsible for manipulating the files in the data
  archive.  A separate instance of the daemon runs at each site containing data
  files.
* **The Alpenhorn CLI**, a command-line utility (`alpenhorn`) which allows
  querying and updating the Data Index, and indirectly, through the database,
  control operation of the daemons.


Contents
========
.. toctree::
   :maxdepth: 2

   demo
   coderef/index

* :ref:`genindex`
* :ref:`search`
