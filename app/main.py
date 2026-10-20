import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse
from sqlalchemy import select

from app.db import dispose, engine, init_db, transactions, with_retry
from app.ingest import ingest
from app.schemas import HealthOut, TransactionOut, TransactionWebhook, WebhookAck, to_iso_z
from app.worker import worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("api")

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await ingest.warm_up()
    ingest.start()
    worker.start()
    yield
    await worker.stop()
    await ingest.stop()
    await dispose()


app = FastAPI(title="Transaction Webhook Service", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def server_timing(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["Server-Timing"] = f"app;dur={(time.perf_counter() - start) * 1000:.1f}"
    return response


@app.api_route("/", methods=["GET", "HEAD"], response_model=HealthOut)
async def health() -> HealthOut:
    return HealthOut(status="HEALTHY", current_time=to_iso_z(datetime.now(timezone.utc)))


@app.post(
    "/v1/webhooks/transactions",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WebhookAck,
)
async def receive_transaction_webhook(payload: TransactionWebhook) -> WebhookAck:
    try:
        is_new = await ingest.store(payload.model_dump())
    except Exception:
        # Not persisted -> don't acknowledge, so the payment processor retries.
        log.exception("failed to store webhook %s", payload.transaction_id)
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Temporarily unavailable, retry")

    duplicate = not is_new
    if not duplicate:
        worker.notify()
    return WebhookAck(
        message="Duplicate webhook ignored" if duplicate else "Webhook accepted",
        transaction_id=payload.transaction_id,
        duplicate=duplicate,
    )


@app.get("/v1/transactions/{transaction_id}", response_model=list[TransactionOut])
async def get_transaction(transaction_id: str) -> list[TransactionOut]:
    async def fetch(conn):
        query = select(transactions).where(transactions.c.transaction_id == transaction_id)
        return (await conn.execute(query)).mappings().first()

    row = await with_retry(engine, fetch)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Transaction not found")
    return [TransactionOut.model_validate(dict(row))]


@app.get("/demo", include_in_schema=False)
async def demo() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")
