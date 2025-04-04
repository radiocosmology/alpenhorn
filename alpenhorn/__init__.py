"""Alpenhorn

Submodules
==========

.. autosummary::
    :toctree: _autosummary

    cli
    common
    daemon
    db
    io
    scheduler
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("alpenhorn")
except PackageNotFoundError:
    # package is not installed
    pass

del version, PackageNotFoundError
