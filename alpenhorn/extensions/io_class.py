"""The Alpenhorn I/O Class Extension."""

from __future__ import annotations

from ..io.base import BaseGroupIO, BaseNodeIO
from .base import Extension


class IOClassExtension(Extension):
    """An I/O Class Extension.

    An I/O Class Extension provides support for additional StorageNode and/or
    StorageGroup I/O Classes not supported by the base Alpenhorn system.

    Attributes
    ----------
    name : str
        The name of this Extension.  The name of an Extension should
        be unique within the extension module defining it, but does
        not need to be globally unique.
    version : str
        The version of the Extension.  The verison must be parsable by
        `packaging.version`.
    io_class_name : str
        The I/O class name.  I/O class names are globally unique.
    node_class : BaseNodeIO class, optional
        A `BaseNodeIO` subclass implmeneting the Node I/O class framework,
        if any.  At least one of `node_class` or `group_class` must
        be provided.
    group_class : BaseGroupIO class, optional
        A `BaseGroupIO` subclass implmeneting the Group I/O class framework,
        if any.  At least one of `node_class` or `group_class` must
        be provided.
    min_version : str, optional
        If given, the minimum Alpenhorn version supported by
        this Extension.  Note: it may make more sense for an extension
        module to check `alpenhorn.__version__` directly instead of using
        this parameter.
    max_version : str, optional
        If given, the maximum Alpenhorn version supported by
        this Extension.  Note: it may make more sense for an extension
        module to check `alpenhorn.__version__` directly instead of using
        this parameter.
    require_schema : dict, optional
        If given, a dict of Data Index schema components and the required
        component schema version needed by this extension.  See
        `alpenhorn.db.schema_version` for the specification of the requirements.
    """

    # Initialised after everything else
    stage = 3

    def __init__(
        self,
        name: str,
        version: str,
        io_class_name: str,
        node_class: type[BaseNodeIO] | None = None,
        group_class: type[BaseGroupIO] | None = None,
        min_version: str | None = None,
        max_version: str | None = None,
        require_schema: dict[str, int | str] | None = None,
    ) -> None:
        super().__init__(
            name,
            version,
            min_version=min_version,
            max_version=max_version,
            require_schema=require_schema,
        )

        if not isinstance(io_class_name, str):
            raise TypeError("io_class_name must be a string")

        io_class_name = io_class_name.strip()
        if not io_class_name:
            raise ValueError("empty I/O class name")
        self.io_class_name = io_class_name

        if not node_class and not group_class:
            raise ValueError("neither node nor group class provided")

        # If node_class or group_class aren't classes at all, these with raise
        # a different kind of TypeError
        try:
            if node_class is not None and not issubclass(node_class, BaseNodeIO):
                raise TypeError("bad type for node_class")
        except TypeError as e:
            raise TypeError("node_class must be a subclass of BaseNodeIO") from e

        try:
            if group_class is not None and not issubclass(group_class, BaseGroupIO):
                raise TypeError("bad type for group_class")
        except TypeError as e:
            raise TypeError("group_class must be a subclass of BaseGroupIO") from e

        self.node_class = node_class
        self.group_class = group_class
