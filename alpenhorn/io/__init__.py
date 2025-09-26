"""I/O modules and I/O utilites for alpenhorn."""

from .default import DefaultIO
from .polling import PollingIO
from .lustrequota import LustreQuotaIO
from .lustrehsm import LustreHSMIO
from .transport import TransportIO

# This is the registry of Alpenhorn's baked-in, internal I/O Classes
internal_io = {
    "Default": DefaultIO,
    "Polling": PollingIO,
    "LustreQuota": LustreQuotaIO,
    "LustreHSM": LustreHSMIO,
    "Transport": TransportIO,
}
