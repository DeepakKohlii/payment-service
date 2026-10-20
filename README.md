# Transaction Webhook Service

Receives transaction webhooks from payment processors (e.g. Razorpay), acknowledges them immediately with `202 Accepted`, and processes them reliably in the background.

- **Live API:** https://payment-service-mqic.onrender.com
- **Demo UI:** https://payment-service-mqic.onrender.com/demo
- **Interactive docs:** https://payment-service-mqic.onrender.com/docs

> Hosted on Render's free tier (Singapore, 0.1 CPU), next to the Supabase database (Singapore). Free instances normally sleep after 15 min idle; the app pings its own public URL every 10 minutes to stay awake (see [app/keep_awake.py](app/keep_awake.py)). If it was asleep anyway, the first request can take ~1 min.

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Health check → `{"status": "HEALTHY", "current_time": "..."}` |
| `POST` | `/v1/webhooks/transactions` | Accept a webhook → `202 Accepted` (also for duplicates) |
| `GET` | `/v1/transactions/{transaction_id}` | Transaction status (`PROCESSING` / `PROCESSED`), `404` if unknown |
| `GET` | `/demo` | Small browser UI to exercise the API |

## Run locally

Requires Python 3.12 and a Postgres database (Supabase or local).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # set DATABASE_URL
uvicorn app.main:app --reload
```

The table is created automatically on startup. Open http://localhost:8000/demo.

## Test

```bash
# Automated check of all success criteria (health, duplicates, latency under load, ~30s processing)
python scripts/smoke_test.py http://localhost:8000      # or the deployed URL
```

Manual:

```bash
curl -X POST $URL/v1/webhooks/transactions -H 'Content-Type: application/json' \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'

curl $URL/v1/transactions/txn_abc123def456     # PROCESSING, then PROCESSED after ~30s
```

## Technical choices

**Stack:** FastAPI (async, typed validation, auto docs) · SQLAlchemy 2.0 async + asyncpg · Supabase Postgres · Render.

**Fast acknowledgement (<500ms).** The webhook handler does exactly one thing: it persists the transaction with status `PROCESSING`, then returns `202`. The 30s work never touches the request path. Getting this reliably under 500ms against a remote database took several measured optimisations. See [Performance: getting under 500ms](#performance-getting-under-500ms) and [optimisation.md](optimisation.md).

**Idempotency.** `transaction_id` is the table's primary key, and the insert uses `INSERT ... ON CONFLICT DO NOTHING`. Duplicates (even concurrent ones) are rejected by the database itself, return `202` with `"duplicate": true`, and are never queued again. This is safe across multiple server instances, unlike an in-memory check.

**Durable background processing: Postgres as the job queue.** I deliberately did *not* use FastAPI `BackgroundTasks` or a plain `asyncio.create_task`: those live in memory, so a restart or deploy (frequent on free hosting) would silently lose transactions. Instead:

1. The `transactions` table *is* the queue. A worker loop running inside the app claims pending rows with `SELECT ... FOR UPDATE SKIP LOCKED` and sets a **lease** (`locked_until = now() + 120s`).
2. Each claimed transaction is processed concurrently (30s simulated external call), then marked `PROCESSED` with `processed_at`.
3. If the process crashes mid-processing, the row is still `PROCESSING` in the DB. Once its lease expires, it is claimed again. *(Verified by `kill -9`-ing the server mid-processing: after restart the transaction was picked up again and completed, `attempts = 2`.)*
4. `SKIP LOCKED` + leases mean several instances can run safely side by side without double-processing.
5. The webhook wakes the worker immediately, so processing starts right away rather than on the next poll.

This gives at-least-once processing with no extra infrastructure. The final `UPDATE ... WHERE status = 'PROCESSING'` makes completion idempotent.

**Reliability on the request path.** If the DB write fails, the API returns `503` instead of `202`. A webhook is never acknowledged unless it has been persisted, so the payment processor will retry it.

**Supabase connection.** Use the **session pooler** (port 5432). The direct connection is IPv6-only, which Render can't reach. The transaction pooler (6543) can't be used with asyncpg's prepared statements outside an explicit transaction, and that would add round-trips. The free session pooler allows **15 clients**, so each instance keeps a fixed pool of 7 connections with no overflow (`DB_POOL_SIZE`). That leaves room for a second instance during deploys. When all connections are busy, extra requests wait briefly for one instead of failing.

**Why Supabase over Neon:** Supabase's free Postgres is always on. Neon scales to zero after ~5 min idle, which adds cold-start latency to the webhook (risking the 500ms budget) and conflicts with an always-polling worker.

**Schema:** `transaction_id` (PK), accounts, `amount NUMERIC(18,2)` (no float rounding for money), `currency`, `status`, `attempts`, `locked_until`, `created_at`, `processed_at`, plus a partial index on pending rows for the worker query.

## Performance: getting under 500ms

The brief requires the webhook to respond within 500ms "regardless of processing complexity". Keeping the 30s work off the request path was the obvious part. The less obvious part was that, against a real cloud database, **the response time is almost entirely database round-trips**. Every optimisation below came from measuring, finding where the time went, and removing round-trips.

**Test setup:** the app running locally, the Supabase database in Seoul (`ap-northeast-2`), **one round-trip ≈ 150ms**. (The database has since moved to Singapore, next to the Render deployment: ~79ms from a local machine, so the numbers below are pessimistic. See [optimisation.md §7](optimisation.md#7-database-moved-next-to-the-app-server).) Bursts were sent as N concurrent requests with `httpx`. Numbers are end-to-end response times seen by the client. In production, deploy the app in the region closest to the database, which shortens every round-trip.

### Live results (Render Singapore + Supabase Singapore)

| Live check (from a laptop, against the deployed URL) | Result |
|---|---|
| Single webhook, server time | **~6–9ms** |
| Single webhook, round-trip from the laptop | ~120ms (mostly internet travel to Singapore) |
| Burst of 20, server time | max **79ms** |
| Burst of 50, server time | p50 97ms, max **506ms** (0.1 CPU becomes the limit) |
| Smoke test: 5 concurrent duplicates | 1 accepted, 4 ignored |
| Smoke test: single transaction | **PROCESSED after 30s** |
| ~340 webhooks sent from the demo page | all accepted and processed |

With the app and the database in the same region, the database part of a webhook takes a few milliseconds. On the free tier, large bursts are limited by the 0.1 CPU rather than by the database. The local measurements below show how each optimisation got here.

### Results at a glance (development measurements)

| Stage | Single webhook | Burst of 20 | Burst of 50 | Errors |
|---|---|---|---|---|
| 0. Local Postgres (baseline, no network) | ~50ms | — | p95 63ms | 0 |
| 1. First run on Supabase | — | — | some requests **failed (500)** | 5 / 50 |
| 2. Pool capped to Supabase's limit | ~750–1050ms | — | p50 ~3.3s, p95 ~5.7s | 0 |
| 3. Pre-ping removed, retry on stale connection, pool opened at startup | **~160ms** | max 746ms | p50 ~800ms, p95 ~1.3s | 0 |
| 4. Concurrent webhooks batched into one INSERT | ~160ms (first request 1.1s) | **max ~370ms** (first burst 2.1s) | max ~370ms | 0 |
| 5. Insert prepared on every connection at startup | **~160ms (first request 230ms)** | **max ~320–350ms** | **max ~350–400ms** | 0 |
| 6. Separate pool for webhooks + keepalive (tested *while* the status endpoint and worker are busy) | ~160ms | **max ~330–420ms** | — | 0 |

Overall: a single webhook went from ~1000ms to **~160ms**, and a burst of 50 from seconds (with failures) to **under 400ms**. That holds right after startup, after long idle periods, and while status polling and background processing compete for the database.

**The full story is in [optimisation.md](optimisation.md):** every optimisation since the start of the project, including the diagnosis behind each stage, the approach that was rejected (Supabase's transaction pooler), the browser-vs-server timing issue, and the remaining limits.

### Reproduce

```bash
python scripts/smoke_test.py $URL   # includes 50 concurrent webhooks and reports p50 / p95 / max
```
Or use the **Burst of traffic** scenario on `/demo`, which shows the slowest response against the 500ms target.

Every response carries a standard **`Server-Timing: app;dur=<ms>`** header with the time the API itself spent. The demo page reports that number, not the browser's round-trip time. Browsers open only ~6 connections per site, so a burst of 20 fetches (plus the page's own status polling) mostly measures time spent *queued in the browser*: we saw 4–9s round-trips while the server answered every request in under 400ms. Tools like `curl` or `httpx`, which is how a payment processor calls a webhook, don't have that limit.

## Scaling

The brief gives no traffic numbers, so nothing here is tuned to a specific load. [scaling.md](scaling.md) covers:
- **What the current setup handles, measured:** all 1000 of 1000 simultaneous webhooks were accepted with no errors. Responses stay fast up to ~100 at once on a single process; beyond that, requests queue on CPU. 100 transactions process in parallel per instance.
- **Where it breaks and why:** single-process CPU, Supabase's 15-connection limit, and the worker concurrency setting.
- **How to scale to 10,000 transactions/second:** ~300k transactions in flight at once, log-first ingest (Kafka/Kinesis), a real queue, a sharded or NoSQL transaction store, autoscaled workers, plus a step-by-step migration path.

## What I'd add for production

- Verify the processor's webhook signature (e.g. Razorpay `X-Razorpay-Signature` HMAC).
- For much higher volume: a larger DB connection limit (a paid pooler or a direct connection), or a durable buffer such as SQS/Kafka in front of the database.
- A dedicated queue (SQS / Celery + Redis) and separate worker service once volume grows; max-attempts + dead-letter status for poison messages.
- Structured logging/metrics and alerting on stuck `PROCESSING` rows.
- Alembic migrations instead of `create_all` on startup.
