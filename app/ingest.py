import asyncio
import logging

from sqlalchemy import text

from app.config import settings
from app.db import ingest_engine, with_retry

log = logging.getLogger("ingest")

INSERT_SQL = text(
    """
    INSERT INTO transactions
           (transaction_id, source_account, destination_account, amount, currency, status)
    SELECT t.id, t.src, t.dst, t.amount, t.cur, 'PROCESSING'
      FROM unnest(CAST(:ids AS text[]), CAST(:srcs AS text[]), CAST(:dsts AS text[]),
                  CAST(:amounts AS numeric[]), CAST(:curs AS text[]))
           AS t(id, src, dst, amount, cur)
    ON CONFLICT (transaction_id) DO NOTHING
    RETURNING transaction_id
    """
)

EMPTY_BATCH = {"ids": [], "srcs": [], "dsts": [], "amounts": [], "curs": []}
MAX_BATCH = 200


class IngestBatcher:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[tuple[dict, asyncio.Future]] = asyncio.Queue()
        # One concurrent batch per ingest connection
        self._pool_size = settings.db_pool_size_ingest
        self._slots = asyncio.Semaphore(self._pool_size)
        self._last_flush = 0.0
        self._tasks: list[asyncio.Task] = []

    async def warm_up(self) -> None:
        if failed := await self._touch_all():
            raise RuntimeError(f"could not open {failed} ingest DB connection(s)")

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._run(), name="ingest-batcher"),
            asyncio.create_task(self._keepalive(), name="ingest-keepalive"),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _touch_all(self) -> int:
        opened = await asyncio.gather(
            *(ingest_engine.connect().start() for _ in range(self._pool_size)), return_exceptions=True
        )
        conns = [c for c in opened if not isinstance(c, BaseException)]
        try:
            results = await asyncio.gather(
                *(conn.execute(INSERT_SQL, EMPTY_BATCH) for conn in conns), return_exceptions=True
            )
        finally:
            await asyncio.gather(*(conn.close() for conn in conns), return_exceptions=True)
        return (len(opened) - len(conns)) + sum(isinstance(r, BaseException) for r in results)

    async def _keepalive(self) -> None:
        loop = asyncio.get_running_loop()
        interval = settings.ingest_keepalive_seconds
        while True:
            await asyncio.sleep(interval)
            if loop.time() - self._last_flush < interval / 2:
                continue  
            for _ in range(self._pool_size):
                await self._slots.acquire()  
            try:
                if failed := await self._touch_all():
                    log.warning("keepalive: %d ingest connection(s) were dead, replacing", failed)
                    await self._touch_all()
            except Exception:
                log.exception("ingest keepalive failed")
            finally:
                for _ in range(self._pool_size):
                    self._slots.release()

    async def store(self, row: dict) -> bool:
        future = asyncio.get_running_loop().create_future()
        await self._queue.put((row, future))
        return await future

    async def _run(self) -> None:
        while True:
            first = await self._queue.get()
            await self._slots.acquire()
            batch = [first]
            while len(batch) < MAX_BATCH and not self._queue.empty():
                batch.append(self._queue.get_nowait())
            task = asyncio.create_task(self._flush(batch))
            task.add_done_callback(lambda _: self._slots.release())

    async def _flush(self, batch: list[tuple[dict, asyncio.Future]]) -> None:
        self._last_flush = asyncio.get_running_loop().time()
        first_seen: dict[str, dict] = {}
        for row, _ in batch:
            first_seen.setdefault(row["transaction_id"], row)
        rows = list(first_seen.values())

        try:
            inserted = await self._insert(rows)
        except Exception as exc:
            log.exception("failed to store batch of %d webhooks", len(batch))
            for _, future in batch:
                if not future.done():
                    future.set_exception(exc)
            return

        claimed: set[str] = set()
        for row, future in batch:
            txn_id = row["transaction_id"]
            is_new = txn_id in inserted and txn_id not in claimed
            if is_new:
                claimed.add(txn_id)
            if not future.done():  # the client may have disconnected
                future.set_result(is_new)

    async def _insert(self, rows: list[dict]) -> set[str]:
        params = {
            "ids": [r["transaction_id"] for r in rows],
            "srcs": [r["source_account"] for r in rows],
            "dsts": [r["destination_account"] for r in rows],
            "amounts": [r["amount"] for r in rows],
            "curs": [r["currency"] for r in rows],
        }

        async def run(conn):
            return set((await conn.execute(INSERT_SQL, params)).scalars())

        return await with_retry(ingest_engine, run)


ingest = IngestBatcher()
