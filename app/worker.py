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

COMPLETE_SQL = text(
    """
    UPDATE transactions
       SET status = 'PROCESSED',
           processed_at = now(),
           locked_until = NULL
     WHERE transaction_id = :transaction_id
       AND status = 'PROCESSING'
    """
)


class Worker:
    def __init__(self) -> None:
        self._wakeup = asyncio.Event()
        self._in_flight: set[asyncio.Task] = set()
        self._main_task: asyncio.Task | None = None

    def notify(self) -> None:
        """Called by the webhook so new jobs start immediately instead of waiting for the next poll."""
        self._wakeup.set()

    def start(self) -> None:
        self._main_task = asyncio.create_task(self._run(), name="worker-loop")

    async def stop(self) -> None:
        # Unfinished jobs keep their lease and are picked up again after restart.
        tasks = [t for t in (self._main_task, *self._in_flight) if t]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self) -> None:
        log.info("worker started")
        while True:
            try:
                claimed = await self._claim_batch()
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
        try:
            log.info("processing %s", transaction_id)
            await asyncio.sleep(settings.processing_delay_seconds)
            await with_retry(
                engine, lambda conn: conn.execute(COMPLETE_SQL, {"transaction_id": transaction_id})
            )
            log.info("processed %s", transaction_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("failed to process %s; will retry after lease expiry", transaction_id)


worker = Worker()
