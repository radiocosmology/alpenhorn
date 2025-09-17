"""Test DatabaseExtension."""

import pytest

from alpenhorn.extensions import DatabaseExtension


def _func():
    """A dummy function to pass to constructors."""
    pass


def test_connect():
    """Connect must be callable."""

    with pytest.raises(ValueError):
        DatabaseExtension("Test", "1", connect=None)

    with pytest.raises(ValueError):
        DatabaseExtension("Test", "1", connect=1)

    # But this is fine
    DatabaseExtension("Test", "1", connect=_func)


def test_close():
    """If given, close must be callable."""

    with pytest.raises(ValueError):
        DatabaseExtension("Test", "1", connect=_func, close=1)

    with pytest.raises(ValueError):
        DatabaseExtension("Test", "1", connect=_func, close=False)

    # But these are fine.
    DatabaseExtension("Test", "1", connect=_func, close=None)
    DatabaseExtension("Test", "1", connect=_func, close=_func)
