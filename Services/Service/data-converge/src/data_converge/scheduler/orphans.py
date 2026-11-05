"""Hold rerun of interrupted windows until ClickHouse confirms the old query stopped.

Runs on the scheduler thread, once per round, before planning. The guard is the
only writer of ``orphan_queries`` releases; the executor consults it through its
``orphan_check`` hook so a held window stays BLOCKED with a precise reason.
"""

from __future__ import annotations

import logging
import time
from typing import Callable

logger = logging.getLogger(__name__)

HOLD_REASON = 'PREVIOUS_QUERY_STILL_RUNNING'


class OrphanQueryGuard:
    """Confirm previous-leader queries are gone before their windows rerun.

    ``stopper(query_ids)`` cancels synchronously and returns ids still running;
    it raises when ClickHouse cannot be asked. Unconfirmed windows remain held.
    """

    def __init__(self, store, stopper: Callable[[list[str]], set[str]], *,
                 retry_seconds: float = 5, clock=time.monotonic):
        self.store = store
        self.stopper = stopper
        self.retry_seconds = retry_seconds
        self.clock = clock
        self._next_attempt = 0.0
        self._warned: set[str] = set()

    def reason(self, instance, _now) -> str | None:
        query_id = self.store.held_orphans().get(instance.instance_id)
        return None if query_id is None else f'{HOLD_REASON}:{query_id}'

    def __call__(self) -> int:
        held = self.store.held_orphans()
        if not held or self.clock() < self._next_attempt:
            return 0
        self._next_attempt = self.clock() + self.retry_seconds
        try:
            running = self.stopper(list(dict.fromkeys(held.values())))
        except Exception as error:
            self._warn(held, type(error).__name__)
            return 0
        confirmed = [instance_id for instance_id, query_id in held.items() if query_id not in running]
        if confirmed:
            self.store.release_orphans(confirmed)
            logger.info('ORPHAN_QUERY_CONFIRMED_STOPPED: windows=%s', len(confirmed))
        if running:
            self._warn({k: v for k, v in held.items() if v in running}, 'STILL_RUNNING')
        return len(confirmed)

    def _warn(self, held: dict[str, str], cause: str) -> None:
        key = cause + ':' + ','.join(sorted(held.values()))
        if key in self._warned:
            return
        self._warned.add(key)
        logger.warning('ORPHAN_QUERY_UNCONFIRMED: windows=%s cause=%s query_ids=%s; '
                       'rerun held until ClickHouse confirms the previous query stopped',
                       len(held), cause, sorted(set(held.values())))
