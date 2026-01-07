"""Alpenhorn Base Extension.

This module implements the base Alpenhorn `Extension` class which defines the
functionality common to all Alpenhorn extensions.

Third-parties shouldn't use this base class to define extensions, but should use
the `Extension` subclasses provided by `alpenhorn.extensions`.
"""

import logging

from packaging.version import Version

log = logging.getLogger(__name__)


class Extension:
    """Base Alpenhorn Extension class.

    This is the Alpenhorn Etension base.  This class
    defines the basic functionality of an Alpenhorn Extension.
    All other extension classes are subclassed from this base
    class.

    The base class should never be instantiated directly.

    Attributes
    ----------
    stage : int
        The initialisation stage for this extension.  The initialisation stage
        for an extension indicates when in the Alpenhorn start-up the extension
        is initialised.  An extension's stage is solely determined by the
        extension's type.  It is never necessary to explicitly specify this when
        instantiating Extensions.  There are three initialisation stages:
            * 1: initialised before the database connection occurs (used by
                DatabaseExtensions)
            * 2: initialised immediately after the database connection occurs
                (used by DataIndexExtensions)
            * 3: initialised after the data index extensions are available
                (used by all other extension types)
    name : str
        The name of this Extension.  The name of an Extension should
        be unique within the extension module defining it, but does
        not need to be globally unique.  The name may not contain a dot (".").
    full_name : str
        The Extension's name prepended with the import path of the module that
        provides it.  The full name of a module must be globally unique.
    version : str
        The version of the Extension.  The verison must be parsable by
        `packaging.version`.
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
        This is ignored by stage-1 Extensions.  DataIndexExtensions (q.v.) have
        an implicit requirement on their own schema.
    """

    # This is the initialisation stage.  Set by sublass.  Initialisation stage 0
    # never occurs, so this must be changed by subclasses.
    stage = 0

    def __init__(
        self,
        name: str,
        version: str,
        min_version: str | None = None,
        max_version: str | None = None,
        require_schema: dict[str, int | str] | None = None,
    ) -> None:
        # A (non-empty) name must be provided
        name = name.strip()
        if not name:
            raise ValueError("name is empty")
        if "." in name:
            raise ValueError(f"invalid name: {name}")
        self.name = name

        # This is set later by alpenhorn
        self.full_name = None

        # This raises an exception on parsing failure (or no version specified)
        self.version = Version(version)
        self.min_version = Version(min_version) if min_version else None
        self.max_version = Version(max_version) if max_version else None

        self.require_schema = require_schema

    def init_extension(self, check_schema: bool, schema_versions: dict | None) -> bool:
        """Initialise this extension.

        This method is called by alpenhorn when initialising the extension.
        When initialisation occurs for an extension is determined by its stage.

        Parameters
        ----------
        check_schema : bool
            If True, run checks on `required_schema`, if given.  Otherwise,
            these checks are skipped.
        schema_versions : dict | None
            If not None, this is a dict of schema components and
            versions which will be used to override their normal
            values during require_schema checks.

        Returns
        -------
        bool
            Whether or not the extension was initialised.

        Raises
        ------
        click.ClickException:
            A required schema check failed.
        """

        from .. import __version__

        # The alpenhorn version
        alpenversion = Version(__version__)

        # Check alpenhorn version
        if self.min_version and alpenversion < self.min_version:
            # Alpenhorn is too old, don't init
            log.debug(
                f"Skipping init of Extension {self.full_name}: "
                f"Alpenhorn too old: {alpenversion} < {self.min_version}"
            )
            return False

        if self.max_version and alpenversion > self.max_version:
            # Alpenhorn is too new; don't init
            log.debug(
                f"Skipping init of Extension {self.full_name}: "
                f"Alpenhorn too new: {alpenversion} > {self.max_version}"
            )
            return False

        # Check schema version(s) if requested.  This only happens with stage 2
        # and 3 extensions
        if check_schema and self.require_schema and self.stage > 1:
            # Raises click.ClickException on failure
            self.check_required_schema(version_overrides=schema_versions)

        # Extension initialised
        log.info(f"Initialised Extension {self.full_name} v{self.version}")
        return True

    def check_required_schema(self, version_overrides: dict[str, int]) -> None:
        """Perform the require_schema checks.

        Parameters
        ----------
        version_overrides : dict | None
            If not None, this is a dict of schema versions for all components
            which will be used to perform the checks.  If None, component
            schema versions will be read from the database.

        Raises
        ------
        click.ClickException
            The checks failed.
        """
        from ..db import schema_version

        for component, component_version in self.require_schema.items():
            # Raises click.ClickException on failure
            schema_version(
                component=component,
                component_version=component_version,
                check_for=self.full_name,
                version_overrides=version_overrides,
            )
