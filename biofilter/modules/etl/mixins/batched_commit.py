"""
Shared commit batching for the ETL query mixins.

The core DTPs create rows one at a time through `get_or_create_*`, and
each helper used to commit on its own. HGNC calls five of them per gene,
so a 46,854-gene load meant hundreds of thousands of commits.
"""

from __future__ import annotations


class BatchedCommitMixin:
    """
    Commit every COMMIT_BATCH_SIZE writes instead of every write.

    The flush that precedes each insert has already assigned the ids the
    caller reads back, so deferring the commit does not change what a
    caller sees — only when it reaches disk. A failure rolls back the
    pending batch, which is the outcome the ETL already produces: the
    step is marked failed and the source is re-run.

    Lives on its own rather than inside one of the mixins because both
    of them commit, and a mixin should not depend on being combined with
    another to work.
    """

    COMMIT_BATCH_SIZE = 1000

    def _commit_batched(self, force: bool = False) -> None:
        self._pending_writes = getattr(self, "_pending_writes", 0) + 1
        if force or self._pending_writes >= self.COMMIT_BATCH_SIZE:
            self.session.commit()
            self._pending_writes = 0

    def flush_pending_writes(self) -> None:
        """
        Commit whatever the last batch left behind.

        Called by the ETL manager after `load()`, so the tail of a run is
        never left uncommitted.
        """
        if getattr(self, "_pending_writes", 0):
            self.session.commit()
            self._pending_writes = 0
