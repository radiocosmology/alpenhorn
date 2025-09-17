"""Alpenhorn Extension API

This module provides the API needed by third-parties to create Alpenhorn
Extensions.

Alpenhorn extensions are defined in a "extension modules", which are
listed by their full import path in the "extensions" sequence of the alpenhorn
config.  Each extension module must provide a function called
`register_extensions` which will be called by Alpenhorn on load and should
return a list of zero or more Extensions.

These Extensions should be instances of one of the Extension classes
provided by this module:

* `DatabaseExtension`: provides capabilities for accessing the database
        containing the Alpenhorn Data Index.
* `ImportDetectExtension`: provides capabilites for determining whether or
        not unregistered files found on a StorageNode should be imported
        (registered) or not.
* `IOClassExtension`: provides I/O framework for additional StorageNode
        or StorageGroup I/O classes.
"""

from .database import DatabaseExtension
from .import_detect import ImportDetectExtension
from .io_class import IOClassExtension
