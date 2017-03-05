import logging
import peewee as pw
import playhouse.db_url as db_url


# All peewee-generated logs are logged to this namespace.
logger = logging.getLogger("db")
logger.addHandler(logging.NullHandler())


# Global variables and constants.
# ================================

# _property = property  # Do this because we want a class named "property".
database_proxy = pw.Proxy()


def connect(url = None):
    # TODO: use connectdb?
    db = db_url.connect(url or 'sqlite:///:memory:')
    db.register_fields({'enum': 'enum'})
    database_proxy.initialize(db)


# Exceptions
# ==========

class NotFound(Exception):
    """Raise when a search fails."""


class NoSubgraph(Exception):
    """Raise when a subgraph specification is missing."""


class BadSubgraph(Exception):
    """Raise when an error in subgraph specification is made."""


class AlreadyExists(Exception):
    """The event already exists at the specified time."""


class DoesNotExist(Exception):
    """The event does not exist at the specified time."""


class UnknownUser(Exception):
    """The user requested is unknown."""


class NoPermission(Exception):
    """User does not have permission for a task."""


class LayoutIntegrity(Exception):
    """Action would harm the layout integrity."""


class PropertyType(Exception):
    """Bad property type."""


class PropertyUnchanged(Exception):
    """A property change was requested, but no change is needed."""


class ClosestDraw(Exception):
    """There is a draw for the shortest path to a given component type."""


# Helper classes for the peewee ORM
# =================================

class EnumField(pw.Field):
    """Implements an enum field for the ORM.

    Why doesn't peewee support enums? That's dumb. We should make one."""
    db_field = 'enum'

    def __init__(self, enum_list, *args, **kwargs):
        self.enum_list = enum_list
        self.value = []
        for e in enum_list:
            self.value.append("'" + str(e) + "'")
        super(EnumField, self).__init__(*args, **kwargs)

    def clone_base(self, **kwargs):
        return super(EnumField, self).clone_base(
            enum_list=self.enum_list, **kwargs)

    def get_modifiers(self):
        return self.value or None

    def coerce(self, val):
        return str(val or '')


class base_model(pw.Model):
    """Baseclass for all models."""

    class Meta:
        database = database_proxy

