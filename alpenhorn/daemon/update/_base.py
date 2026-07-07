"""Base class for UpdateableNode and UpdatableGroup."""

from __future__ import annotations

import json
import logging
import time

from ...db import StorageGroup, StorageNode

log = logging.getLogger(__name__)


class UpdateableBase:
    """Abstract base class for UpdateableNode and UpdateableGroup.

    After instantiation, these subclasses provide access to the I/O instance
    via the `io` attribute and the underlying database object (StorageNode
    or StorageGroup) via the `db` attribute.
    """

    # Set to True or False in subclasses
    is_group = None

    def __init__(self) -> None:
        raise NotImplementedError("updateable_base cannot be instantiated directly.")

    def __del__(self) -> None:
        # Stop updating
        self.stop()

    @property
    def name(self) -> str:
        """The name of this instance."""
        return self.db.name

    def _check_io_reinit(self, new: StorageNode | StorageGroup) -> bool:
        """Do we need to re-initialise our I/O instance?

        Parameters
        ----------
        new : StorageNode or StorageGrou
            The new storage object for this update loop iteration

        Returns
        -------
        do_reinit : bool
            True if re-init needs to happen.
        """

        # db is None if this is a new instance
        if self.db is None:
            return True

        # io is None if the I/O class wasn't found
        if self.io is None:
            return True

        if self.db.id != new.id:
            return True

        if self.db.io_config != new.io_config:
            return True

        if self.db.io_class != new.io_class:
            return True

        return False

    def _parse_io_config(self, config_json: str | None) -> dict:
        """Parse and return the I/O config.

        Parameters
        ----------
        config_json : str or None
            The I/O config JSON string

        Returns
        -------
        io_config : dict
            The parsed I/O config, or an empty dict if `config_json`
            was `None.  This value is also assigned to `self._io_config`.

        Raises
        ------
        ValueError
            `config_json` did not evaluate to a dict.
        """
        if config_json is None:
            self._io_config = {}
        else:
            self._io_config = json.loads(config_json)

            if not isinstance(self._io_config, dict):
                raise ValueError(f'Invalid io_config: "{config_json}".')

        return self._io_config

    def _get_io_class(self):
        """Return the I/O class for our Storage object."""

        from ...common.extload import io_extension

        # If no io_class is specified, the Default I/O classes are used
        io_class = "Default" if self.db.io_class is None else self.db.io_class

        # We assume StorageNode if not StorageGroup
        if self.is_group:
            obj_type = "StorageGroup"
            key = "group_class"
        else:
            obj_type = "StorageNode"
            key = "node_class"

        # Get the I/O extension
        extension = io_extension(io_class)
        if extension is None:
            log.error(
                f'Unknown I/O class: "{io_class}".  Ignoring {obj_type} {self.name}.'
            )
            return None

        # Get the class from the extension
        class_ = getattr(extension, key, None)
        if not class_:
            log.error(
                f'No implementation of I/O class "{io_class}" in extension '
                f'"{extension.full_name}" for {obj_type} {self.name}.'
            )

        # return the class (or None)
        return class_

    def stop(self) -> None:
        """Stop updating.

        Called when the instance is deleted, and when reinit
        occurs.  Clears the associated queue FIFO.
        """

        if self._fifo is not None:
            pending_count, deferred_count = self._queue.clear_fifo(
                self._fifo, keep_clear=True
            )
            total = pending_count + deferred_count
            if total:
                name = (
                    f'Group "{self.name}"' if self.is_group else f'Node "{self.name}"'
                )
                tasks = "task" if total == 1 else "tasks"
                log.info(
                    f"Discarded {total} {tasks} ({pending_count} pending; "
                    f"{deferred_count} deferred) while stopping Storage{name}."
                )

    def reinit(self, storage: StorageNode | StorageGroup) -> bool:
        """Re-initialise the instance with a new database object.

        Called once per update loop.

        Parameters
        ----------
        storage : StorageNode or StorageGroup
            The newly-fetched database storage instance

        Returns
        -------
        did_reinit : bool
            True if the I/O object was re-initialised
        """
        # Does I/O instance need to be re-instantiated?
        if self._check_io_reinit(storage):
            # The check here ensures the message is only printed if self was previously
            # fully initialised
            if self.db and self.io:
                log.info(
                    "I/O config change detected for Storage"
                    + ("Group" if self.is_group else "Node")
                    + f' "{self.name}": attempting re-init.'
                )

            # This will do nothing, when reinit is called from __init__, because
            # self._fifo is None
            self.stop()

            self.db = storage
            self.io_class = self._get_io_class()

            if self.io_class is None:
                # Error locating I/O class
                self.io = None
                return False

            # Parse I/O config if present
            config = self._parse_io_config(storage.io_config)

            # Generate a new fifo key and label it
            self._fifo = (self.is_group, storage.name, time.monotonic())
            label = f"Group {storage.name}" if self.is_group else f"Node {storage.name}"
            self._queue.label_fifo(self._fifo, label=label)

            # Initialise I/O object
            self.io = self.io_class(storage, config, self._queue, self._fifo)

            return True

        # No re-init, update I/O instance's Storage object
        self.db = storage
        self.io.set_storage(storage)
        return False
