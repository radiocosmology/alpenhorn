"""Cedar nearline I/O.

These I/O classes are specific to the nearline tape archive at cedar.
"""

import logging
import pathlib
import peewee as pw

from alpenhorn.task import Task
from alpenhorn.archive import ArchiveFileCopy
from alpenhorn.io.base import BaseGroupIO, BaseNodeRemote
from alpenhorn.io.ioutil import pretty_bytes
from alpenhorn.querywalker import QueryWalker
from alpenhorn.io.LFSQuota import LFSQuotaNodeIO
from alpenhorn.io import _default_asyncs

log = logging.getLogger(__name__)


class NearlineNodeRemote(BaseNodeRemote):
    """NearlineNodeRemote: information about a Nearline remote StorageNode."""

    def pull_ready(self, file):
        """Returns True if the file copy is ready (not released)."""
        try:
            copy = ArchiveFileCopy.get(file=file, node=self.node)
        except pw.DoesNotExist:
            return False

        return copy.ready


class NearlineNodeIO(LFSQuotaNodeIO):
    """Nearline node I/O.

    Required io_config keys:
        * quota_group : string
            the user group to query quota for
        * fixed_quota : integer
            the quota, in kiB, on the Nearline disk

    Optional io_config keys:
        * lfs : string
            the lfs(1) executable.  Defaults to "lfs"; may be a full path.
        * headroom: integer
            the amount of space in bytes to keep empty on the disk.  Defaults
            to 25% of fixed_quota.
        * release_check_count : integer
            The number of files to check at a time when doing idle HSM status
            update (see idle_update()).  Default is 100.
    """

    remote_class = NearlineNodeRemote

    # QueryWalkers for the HSM state check.  Indexed by node id
    _release_qw = dict()

    def __init__(self, node):
        super().__init__(node)

        # 25% of 1 kiB == 2**8 bytes
        self._headroom = self.config.get(
            "headroom", self.config["fixed_quota"] * 2**8
        )

        # For idle-time HSM state updates
        self._nrelease = self.config.get("release_check_count", 100)
        if self._nrelease < 1:
            raise ValueError(
                f"io_config key 'release_check_count' non-positive (={self._nrelease})"
            )

    def release_files(self):
        """Release files from the nearline disk to keep the headroom clear."""

        headroom_needed = self._headroom - self._lfs.quota_remaining(self.node.root)

        # Nothing to do
        if headroom_needed <= 0:
            return

        def _async(task, node, lfs, headroom_needed):
            total_files = 0
            total_bytes = 0

            # loop through file copies until we've released enough
            # (or we run out of files)
            for copy in (
                ArchiveFileCopy.select()
                .where(
                    ArchiveFileCopy.node == node,
                    ArchiveFileCopy.has_file == "Y",
                    ArchiveFileCopy.ready == True,
                )
                .order_by(ArchiveFileCopy.last_update)
            ):
                # Skip unarchived files
                if not lfs.hsm_archived(copy.path):
                    continue

                log.debug(
                    f"releasing file copy {copy.path} "
                    f"[={pretty_bytes(copy.file.size_b)}] on node {self.node.name}"
                )
                lfs.hsm_release(copy.path)
                # Update copy record immediately
                ArchiveFileCopy.update(ready=False).where(
                    ArchiveFileCopy.id == copy.id
                ).execute()
                total_files += 1
                total_bytes += copy.file.size_b
                if total_bytes >= headroom_needed:
                    break
            log.info(
                f"released {pretty_bytes(total_bytes)} in "
                f"{total_files} files on node {self.node.name}"
            )
            return

        # Do the rest asynchronously
        Task(
            func=_async,
            queue=self._queue,
            key=self.node.name,
            args=(self.node, self._lfs, headroom_needed),
            name=f"Node {self.node.name}: HSM release {pretty_bytes(headroom_needed)}",
        )

    def before_update(self, idle):
        """Before node update check.

        If idle is True, call self.release_files to potentially free up space."""
        # If the update is going to happen, clear up headroom, if necessary
        if idle:
            self.release_files()

        # Continue with the update
        return True

    def check_active(self):
        """Returns True.

        There's no ALPENHORN_NODE file on nearline.
        """
        return True

    def fits(self, size_b):
        """Returns True.

        Everything fits on Nearline."""
        return True

    def filesize(self, path, actual=False):
        """Returns the size of the file given by path.

        This _always_ returns the apprent size, since the size on tape is
        not known (or important), and the size on disk is usually that of the
        the file stub.
        """
        path = pathlib.Path(path)
        if not path.is_absolute():
            path = pathlib.Path(self.node.root, path)
        return path.stat().st_size

    def reserve_bytes(self, size, check_only=False):
        """Returns True."""
        return True

    def release_bytes(self, size):
        """Does nothing."""
        pass

    def check(self, copy):
        """Make sure the file is restored before trying to check it."""

        # If the file is released, restore it and do nothing
        # further (assuming the update loop will re-call this
        # method next time through the loop).
        if self._lfs.hsm_released(copy.path):
            self._lfs.hsm_restore(copy.path)
            return

        # Otherwise, use the DefaultIO method to do the check
        super().check(copy)

    def auto_verify(self, copy):
        """Auto-verify a file in nearline.

        If the file happens to be restored, we simply run check() on
        it.  If it's released, instead we do the following:

        - trigger a restore the file
        - wait for the file to be restored
        - run the check()
        - release the file again

        The last step here is to keep auto-verifcation from replacing
        all the newly added data from nearline (which are presumably
        more interesting that some random files being auto-verified).
        """
        # The trivial case.
        if not pathlib.Path(copy.path).exists():
            # Only update if this is surprising (which it probably
            # is.)
            if copy.has_file != "N":
                log.warning(
                    "File copy missing during auto-verify: "
                    f"{copy.file.path}.  Updating database."
                )
                ArchiveFileCopy.update(has_file="N").where(
                    ArchiveFileCopy.id == copy.id
                ).execute()
            return

        # Are we restored?  Note: we're interested in the actual
        # state of the file, not the state as recorded in the database.
        if not self._lfs.hsm_released(copy.path):
            return super().check(copy)

        def _async(task, node, lfs, copy):
            """Asyncrhonously restore a file, verify it, and then
            release it again.
            """
            # Trigger restore
            lfs.hsm_restore(copy.path)

            # While the file is not restored, yield to wait for later
            while lfs.hsm_released(copy.path):
                yield 30

            # Do the check by inlining the Default-I/O function
            _default_asyncs.check_async(task, node, copy)

            # Before releasing the file, check whether the DB thinks
            # it's restored.  If it is don't bother releasing, since
            # it's better to be consistent with the DB
            if not ArchiveFileCopy.get(id=copy.id).ready:
                lfs.hsm_release(copy.path)

        Task(
            func=_async,
            queue=self._queue,
            key=self.node.name,
            args=(self.node, self._lfs, copy),
            name=(
                f"Auto-verify released file {copy.file.name} "
                f"on node {self.node.name}"
            ),
        )

    def ready(self, req):
        """Recall a file, if necessary, to prepare for a remote pull."""

        fullpath = pathlib.Path(self.node.root, req.file.path)
        state = self._lfs.hsm_state(fullpath)

        # Update DB based on HSM state
        ArchiveFileCopy.update(
            ready=(state == self._lfs.HSM_RESTORED or state == self._lfs.HSM_UNARCHIVED)
        ).where(
            ArchiveFileCopy.file == req.file, ArchiveFileCopy.node == req.node_from
        ).execute()

        # If it's restorable, restore it
        if state == self._lfs.HSM_RELEASED:
            self._lfs.hsm_restore(fullpath)

    def idle_update(self):
        """Update HSM state of copies when idle.

        If the node is idle after an update, double check the HSM state
        for a few files and update the DB if necessary.

        Any I/O on a nearline file will restore it.  It's important for the
        data index to reflect changes made in this way outside of alpenhorn so that
        the alpenhornd daemon can properly manage free space.
        """

        # Get the query walker.  Initialised if necessary.
        try:
            qw = self._release_qw[self.node.id]
        except KeyError:
            try:
                qw = QueryWalker(
                    ArchiveFileCopy,
                    ArchiveFileCopy.node == self.node,
                    ArchiveFileCopy.has_file == "Y",
                )
                self._release_qw[self.node.id] = qw
            except pw.DoesNotExist:
                return  # No files to check on the node

        # Try to get a bunch of copies to check
        try:
            copies = qw.get(self._nrelease)
        except pw.DoesNotExist:
            # Not sure why all the file copies have gone away, but given there's
            # nothing on the node now, can't hurt to re-init the QW in this case
            del self._release_qw[self.node.id]
            return

        def _async(task, node, lfs, copies):
            for copy in copies:
                state = lfs.hsm_state(copy.path)
                log.info(f"{copy.path} = {state}")
                if state == lfs.HSM_MISSING:
                    # File is unexpectedly gone.
                    log.warning(
                        f"File copy {copy.file.path} on node {node.name} is missing!"
                    )
                    ArchiveFileCopy.update(has_file="N", ready=False).where(
                        ArchiveFileCopy.id == copy.id
                    ).execute()
                elif state == lfs.HSM_RELEASED:
                    if copy.ready:
                        log.debug("Updating file copy {copy.file.path}: ready -> False")
                        ArchiveFileCopy.update(ready=False).where(
                            ArchiveFileCopy.id == copy.id
                        ).execute()
                else:  # i.e. RESTORED or UNARCHIVED
                    if not copy.ready:
                        log.debug("Updating file copy {copy.file.path}: ready -> True")
                        ArchiveFileCopy.update(ready=True).where(
                            ArchiveFileCopy.id == copy.id
                        ).execute()

        # Copies get checked in an async
        Task(
            func=_async,
            queue=self._queue,
            key=self.node.name,
            args=(self.node, self._lfs, copies),
            name=f"Node {self.node.name}: HSM state check for {len(copies)} copies",
        )


class NearlineGroupIO(BaseGroupIO):
    """Nearline Group I/O

    The Nearline Group contains two nodes:
     - a primary node pointing to the nearline storage itself.
     - a secondary node on a regular disk used for storing files too small for nearline.

    Optional io_config keys:
     - threshold : integer
            The smallfile threshold, in bytes.  Files above this size are sent to
            nearline and put on tape.  The small files are sent to the secondary
            node.  Default is 1000000000 bytes (1 GB).
    """

    def __init__(self, group):
        super().__init__(group)

        self._threshold = self.config.get("threshold", 1000000000)  # bytes

    @property
    def idle(self):
        """Returns the True if no node I/O is occurring."""
        return self._nearline.io.idle and self._smallfile.io.idle

    def before_update(self, nodes, idle):
        """Identify primary and secondary nodes.

        If both nodes aren't online, we cancel the update.
        """
        if len(nodes) != 2:
            log.error(
                f"need exactly two nodes in StorageGroup group {self.group.name} (have {len(nodes)})"
            )
            return False

        if nodes[0].io_class == "Nearline":
            self._nearline = nodes[0]
            self._smallfile = nodes[1]
        elif nodes[1].io_class == "Nearline":
            self._nearline = nodes[1]
            self._smallfile = nodes[0]
        else:
            log.error(f"no Nearline node in StorageGroup {self.group.name}")
            return False

        return True

    def exists(self, path):
        """Checks whether a file called path exists in this group.

        Returns the StorageNode containing the file, or None if no
        file was found.
        """
        if self._smallfile.io.exists(path):
            return self._smallfile
        if self._nearline.io.exists(path):
            return self._nearline
        return None

    def pull(self, req):
        """Pass ArchiveFileCopyRequest to the correct node."""
        if req.file.size_b <= self._threshold:
            self._smallfile.io.pull(req)
        else:
            self._nearline.io.pull(req)
