import asyncio
import logging

from sqlalchemy import text

from app.config import settings
from app.db import engine, with_retry

log = logging.getLogger("worker")

CLAIM_SQL = text(
    """
    UPDATE transactions
       SET locked_until = now() + make_interval(secs => :lease),
           attempts = attempts + 1
     WHERE transaction_id IN (
           SELECT transaction_id
             FROM transactions
            WHERE status = 'PROCESSING'
              AND (locked_until IS NULL OR locked_until < now())
            ORDER BY created_at
            LIMIT :limit
              FOR UPDATE SKIP LOCKED
     )
    RETURNING transaction_id
    """
)

# Completes many transactions in one round-trip. Rows already PROCESSED are left untouched.
COMPLETE_SQL = text(
    """
    UPDATE transactions
       SET status = 'PROCESSED',
           processed_at = now(),
           locked_until = NULL
     WHERE transaction_id = ANY(CAST(:ids AS text[]))
       AND status = 'PROCESSING'
    """
)

MAX_COMPLETE_BATCH = 1000


class Worker:
    def __init__(self) -> None:
        self._wakeup = asyncio.Event()
        self._in_flight: set[asyncio.Task] = set()
        self._completed: asyncio.Queue[str] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []

    def notify(self) -> None:
        self._wakeup.set()

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._run(), name="worker-loop"),
            asyncio.create_task(self._complete_loop(), name="worker-completer"),
        ]

    async def stop(self) -> None:
        # Unfinished jobs (including ones not yet marked PROCESSED) keep their lease
        # and are picked up again after restart.
        tasks = [*self._tasks, *self._in_flight]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self) -> None:
        log.info("worker started")
        while True:
            try:
                claimed = await self._claim_batch()
                if claimed:
                    log.info("claimed %d transaction(s)", len(claimed))
                for txn_id in claimed:
                    task = asyncio.create_task(self._process(txn_id))
                    self._in_flight.add(task)
                    task.add_done_callback(self._in_flight.discard)
                if claimed:
                    continue  # there may be more pending; check again right away
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("worker loop error; retrying")

            self._wakeup.clear()
            try:
                await asyncio.wait_for(
                    self._wakeup.wait(), timeout=settings.worker_poll_interval_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def _claim_batch(self) -> list[str]:
        free = settings.worker_concurrency - len(self._in_flight)
        if free <= 0:
            return []

        async def claim(conn):
            result = await conn.execute(
                CLAIM_SQL, {"lease": settings.job_lease_seconds, "limit": free}
            )
            return [row.transaction_id for row in result]

        return await with_retry(engine, claim)

    async def _process(self, transaction_id: str) -> None:
        # Simulated external API call; the result is written by the completer in a batch
        await asyncio.sleep(settings.processing_delay_seconds)
        self._completed.put_nowait(transaction_id)

    async def _complete_loop(self) -> None:
        while True:
            ids = [await self._completed.get()]
            while len(ids) < MAX_COMPLETE_BATCH and not self._completed.empty():
                ids.append(self._completed.get_nowait())
            try:
                await with_retry(engine, lambda conn: conn.execute(COMPLETE_SQL, {"ids": ids}))
                log.info("processed %d transaction(s)", len(ids))
            except asyncio.CancelledError:
                raise
            except Exception:
                # Rows stay PROCESSING and are re-claimed once their lease expires
                log.exception("failed to mark %d transaction(s) processed; will retry after lease expiry", len(ids))


worker = Worker()
