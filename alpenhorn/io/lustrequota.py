"""``alpenhorn.io.lustrequota``: LustreQuota I/O class.

This I/O module defines the `LustreQuotaNodeIO` class which extends `DefaultNodeIO`.
The primary change is that it uses the "lfs quota" command to determine free space
on a quota-tracking Lustre filesystem.

The quota to query must be specified in the `io_config`.
"""

from __future__ import annotations

import logging
from collections.abc import Hashable

from ..daemon.scheduler import FairMultiFIFOQueue
from ..db import StorageNode
from ._lfs import LFS
from .base import InternalIO
from .default import DefaultNodeIO

log = logging.getLogger(__name__)


class LustreQuotaNodeIO(DefaultNodeIO):
    """LustreQuota Node I/O.

    An extension to DefaultNodeIO which uses the "lfs quota" to determine
    free space, rather than stat.

    Required io_config keys:

        * quota_id: the id (username, uid, group name, gid, or project id) to query
            quota for.
        * quota_type: One of "user", "group" or "project" indicating how to
            interpret the value of quota_id.

            .. note::

                If the provided `quota_id` is using the default block quota on
                the filesystem, then block quota max cannot be determined from
                the lfs(1) output.  In this case, `fixed_quota` *must* be used to
                specify the default quota for any meaningful free space value to
                be returned.

    Optional io_config keys:

        * fixed_quota: a fixed number of kiB to use to override the max quota
            reported by the "lfs quota" command.
        * lfs: the lfs(1) executable.  Defaults to "lfs"; may be a full path.
        * lfs_timeout: the timeout, in seconds, for an lfs(1) call.  Calls
            that run longer than this will be abandonned.  Defaults to 60
            seconds if not given.

    Parameters
    ----------
    node : StorageNode
        The node we're performing I/O on.
    config : dict
        The I/O config.
    queue : FairMultiFIFIOQueue
        The task scheduler.
    fifo : Hashable
        The queue FIFO key to use.
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
            If ``True``, then this is a fast call and we simply return ``None``.
            Otherwise, it's a "slow call" and we do the "lfs quota" query.

        Returns
        -------
        int or None
            ``None`` if `fast` is ``True``. Otherwise, the available quota
            in bytes.
        """
        if fast:
            return None

        return self._lfs.quota_remaining(self.node.root)


LustreQuotaIO = InternalIO(__name__, LustreQuotaNodeIO, None)
