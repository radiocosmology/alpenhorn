"""Alpenhorn LustreQuota Node I/O class.

This I/O class extends the DefaultNodeIO.  The primary change is that it
uses the "lfs quota" command to determine free space on a quota-tracking
Lustre filesystem.

This module only queries group quota.  The group used must be provided in
the `io_config`.

The quota target can either be the one reported by "lfs quota" directly
or else set to a fixed value via the `io_config`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .default import DefaultNodeIO
from .lfs import LFS

if TYPE_CHECKING:
    from collections.abc import Hashable

    from ..queue import FairMultiFIFOQueue
    from ..storage import StorageNode
del TYPE_CHECKING

log = logging.getLogger(__name__)


class LustreQuotaNodeIO(DefaultNodeIO):
    """An extension to DefaultNodeIO which uses the "lfs quota" to determine
    free space, rather than stat.

    Required io_config keys:
        * quota_id: the id (username, uid, group name, gid, or project id) to query
            quota for.
        * quota_type: One of "user", "group" or "project" indicating how to
            interpret the value of quota_id.

    Optional io_config keys:
        * fixed_quota: a fixed number of kiB to use to override the max quota
            reported by the "lfs quota" command.
        * lfs: the lfs(1) executable.  Defaults to "lfs"; may be a full path.
        * lfs_timeout: the timeout, in seconds, for an lfs(1) call.  Calls
            that run longer than this will be abandonned.  Defaults to 60
            seconds if not given.

    Notes
    -----
    If the provided `quota_id` is using the default block quota on the filesystem,
    then block quota max cannot be determined from the lfs(1) output.  In this case,
    `fixed_quota` _must_ be used to specify the default quota for any meaningful free
    space value to be returned.
    """

    def __init__(
        self, node: StorageNode, config: dict, queue: FairMultiFIFOQueue, fifo: Hashable
    ) -> None:
        super().__init__(node, config, queue, fifo)

        quota_id = config.get("quota_id", None)
        quota_type = config.get("quota_type", None)

        # Make alpenhornd crash if the io_config is incomplete, but allow
        # "quota_group" legacy support
        if quota_id is None or quota_type is None:
            try:
                quota_id = config["quota_group"]
                quota_type = "group"
                log.warning(
                    "Using deprecated 'quota_group' in "
                    f"StorageNode {node.name} io_config"
                )
            except KeyError:
                pass

            if quota_id is None:
                raise KeyError(
                    f'"quota_id" missing from StorageNode {node.name} io_config'
                )
            if quota_type is None:
                raise KeyError(
                    f'"quota_type" missing from StorageNode {node.name} io_config'
                )

        # Initialise the lfs(1) wrapper
        self._lfs = LFS(
            quota_id=quota_id,
            quota_type=quota_type,
            fixed_quota=config.get("fixed_quota", None),
            lfs=config.get("lfs", "lfs"),
            timeout=config.get("lfs_timeout", None),
        )

    # I/O METHODS

    def bytes_avail(self, fast: bool = False) -> int | None:
        """Use "lfs quota" get the amount of free quota.

        Parameters
        ----------
        fast : bool
            If True, then this is a fast call and we simply return None.
            Otherwise (slow call) we do th "lfs quota" query.

        Returns
        -------
        bytes_avail : int or None
            None if `fast is True` otherwise, the available quota.
        """
        if fast:
            return None

        return self._lfs.quota_remaining(self.node.root)
