# Optimisations

This file records every optimisation made in this project, in the order they were made: what the problem was, how it was found, what changed, and the measured result. It also covers the approaches that were tried and rejected.

**Contents**

1. [Design-level decisions](#1-design-level-decisions): fast acknowledgement, idempotency, durability
2. [Background worker efficiency](#2-background-worker-efficiency)
3. [Webhook latency: from ~1000ms to ~160ms](#3-webhook-latency-from-1000ms-to-160ms): six measured stages
4. [Measuring correctly](#4-measuring-correctly): browser queueing vs server time
5. [Demo page](#5-demo-page)
6. [Limits and what's next](#6-limits-and-whats-next)
7. [Database moved next to the app server](#7-database-moved-next-to-the-app-server)
8. [No cold starts: keep-awake on Render's free tier](#8-no-cold-starts-keep-awake-on-renders-free-tier)

**Test setup for sections 1–6:** app running on a local machine, Supabase Postgres in Seoul (`ap-northeast-2`), **one database round-trip ≈ 150ms**. Bursts are N concurrent requests sent with `httpx`. Deployed next to the database, every round-trip is shorter, so these numbers are a pessimistic case.

---

## Summary

| | Before | After |
|---|---|---|
| Single webhook | ~1000ms | **~160ms** |
| First webhook after startup | ~1100ms | **~230ms** |
| Burst of 20 | 746ms | **~200–370ms** |
| Burst of 50 | p95 ~5.7s, some failed with `500` | **max ~350–400ms, 0 errors** |
| Burst of 20 while status polling + worker are busy | ~650ms–6.8s | **max ~330–420ms** |
| Burst of 20 after 5+ min idle | slow (connections reopened) | **max 205ms** |
| Crash mid-processing | (in-memory tasks would be lost) | **recovered and completed** |
| Single transaction while a backlog of >100 is processing | **~60s** (waited for a free worker slot) | **~30s** |
| 1000 transactions received at once | ~10 waves of 30s; completions spread over 30–39s | **all processed in 30.2–30.3s** |
| Database round-trip (after moving to Singapore, see 7) | ~150ms | **~79ms** from a local machine; a few ms from Render Singapore |

The single biggest lesson: **against a remote database, response time is almost entirely database round-trips.** CPU was never the bottleneck. Almost every optimisation below removes round-trips or takes them off the request path.

---

## 1. Design-level decisions

These were decided before any code was written. They shape everything else.

### 1.1 Acknowledge first, process later
**Problem:** the brief requires `202` within 500ms, but processing takes 30s.
**Decision:** the webhook handler only *persists* the transaction (status `PROCESSING`) and returns `202`. The 30s work runs in a background worker. The request path does one thing: one database write.

### 1.2 Postgres as the job queue (durability)
**Problem:** the obvious approaches, FastAPI `BackgroundTasks` or `asyncio.create_task`, keep work in memory. A restart or deploy (frequent on free hosting) would silently lose transactions, and the brief says "doesn't lose transactions".
**Decision:** the `transactions` table *is* the queue. The worker claims pending rows from the database, so a job only counts as done when the database says `PROCESSED`.
**Considered and rejected:** Celery + Redis. That's extra infrastructure and a paid worker on Render, overkill at this scale. It's the documented scale-up path.
**Verified:** the server was killed with `kill -9` while a transaction was mid-processing. After restart, the row was still `PROCESSING`, its lease expired, and the worker re-claimed it (`attempts = 2`) and completed it.

### 1.3 Idempotency enforced by the database
**Problem:** payment processors resend webhooks. Two identical webhooks can even arrive at the same instant.
**Decision:** `transaction_id` is the **primary key**, and every insert uses `ON CONFLICT DO NOTHING`. The database itself makes a second row impossible, even for concurrent requests and across multiple server instances.
**Rejected:** "check if it exists, then insert" in application code. Two concurrent requests can both see "doesn't exist", which is a race condition.
**Bonus:** because the insert is idempotent, *retrying it is always safe*. Several later optimisations (3.3, 3.6) rely on this.

### 1.4 Never acknowledge what isn't stored
If the database write fails, the API returns `503`, not `202`. A `202` is a promise ("I have it"). A `503` makes the processor retry. A webhook is never acknowledged unless it was persisted.

### 1.5 Database choice: Supabase over Neon
Neon's free tier scales to zero after ~5 minutes idle. Waking it adds latency to the webhook (risking the 500ms budget), and an always-polling worker would keep it awake and burn free compute hours. Supabase's free Postgres is always on.

### 1.6 Money stored as `NUMERIC(18,2)`
Not a float, so there's no rounding error on amounts. It's validated on input (`amount > 0`, max 2 decimals, 3-letter currency).

---

## 2. Background worker efficiency

| Optimisation | Why |
|---|---|
| **Batch claiming with `FOR UPDATE SKIP LOCKED`** | One query claims up to N pending rows. Concurrent workers or instances skip rows already locked instead of waiting, so they never block each other or double-process. |
| **Leases (`locked_until`)** instead of a permanent "in progress" flag | If a worker dies, its lease expires (120s) and the row becomes claimable again. That's crash recovery with no coordinator. |
| **Concurrent processing** (up to 5000 in flight per instance, see 2.1) | 20 transactions take ~30s total, not 20 × 30s. Each is an `asyncio` task, so waiting costs no threads. |
| **Batched completions** (see 2.1) | Transactions that finish together are marked `PROCESSED` with one `UPDATE … WHERE transaction_id = ANY(…)`, not one write each. |
| **Wake-up on new webhook** | The webhook handler signals the worker directly, so processing starts immediately instead of on the next poll. Polling (every 2s) is only a fallback. |
| **Partial index on pending rows** (`WHERE status = 'PROCESSING'`) | The worker's "find pending jobs" query stays fast as processed rows pile up, because the index only contains rows still to be processed. |
| **Idempotent completion** (`UPDATE ... WHERE status = 'PROCESSING'`) | Completing a transaction twice, after a lease expiry for example, is harmless. |
| **Single-statement autocommit queries** | Claim and complete are each one statement, so there are no `BEGIN`/`COMMIT` round-trips (see 3.3). |

### 2.1 Worker limit and batched completions
**Symptom:** the smoke test's single transaction took **63s** instead of ~30s. That breaks the brief's first success criterion ("processed after ~30 seconds").
**Cause:** 116 transactions had been sent just before it, and the worker processed at most **100 at a time** (`WORKER_CONCURRENCY`). The database showed exactly that: 100 finished at 30s+, the remaining 67 at 60s+. A reviewer who runs a load test and *then* a single-transaction test would hit this.
**Second cause, found earlier with 200 at once:** every finished transaction needed its own `UPDATE`, through only 3 connections. When many finished at the same moment, the writes queued and completions spread over **30–39s**.
**Fix:**
- `WORKER_CONCURRENCY` default raised to **5000**. Each in-flight transaction is an async task that mostly sleeps, so this costs memory, not threads. The limit now has to exceed any realistic backlog rather than act as a throttle.
- **Batched completions:** a finished transaction goes into an in-memory queue, and a completer marks everything that has finished with **one** `UPDATE … WHERE transaction_id = ANY(…) AND status = 'PROCESSING'`. As with ingest batching, there's no artificial delay; batches form from whatever finished while the previous write was in flight.
- **Durability is unchanged:** if the process dies before the batch is written, those rows are still `PROCESSING` with a lease, and are re-claimed and completed after the lease expires.

**Result:**
- **1000 transactions received at once were all processed in 30.2–30.3s,** using 12 completion writes in total instead of 1000.
- The smoke test's single transaction, sent while those 1000 were processing: **~30s**.
- Webhooks sent while the 1000 were processing still answered in **max 255ms** (50 at once). That's the brief's "responds quickly even under processing load".
- **Crash recovery re-tested** with batched completions: killed 3s into processing, re-claimed after the lease expired (`attempts = 2`), and completed. It happened to be finished by a *second* running instance, which again showed that multiple instances share work safely.

---

## 3. Webhook latency: from ~1000ms to ~160ms

All of this was found by measuring against the real cloud database. On a local Postgres (stage 0) everything looked fast, which hid every problem below.

| Stage | Single webhook | Burst of 20 | Burst of 50 | Errors |
|---|---|---|---|---|
| 0. Local Postgres (no network) | ~50ms | — | p95 63ms | 0 |
| 1. First run on Supabase | — | — | some requests failed | 5 / 50 |
| 2. Pool capped to Supabase's limit | ~750–1050ms | — | p50 ~3.3s, p95 ~5.7s | 0 |
| 3. No pre-ping, retry on dead connection, pool opened at startup | **~160ms** | max 746ms | p95 ~1.3s | 0 |
| 4. Concurrent webhooks batched into one INSERT | ~160ms (first: 1.1s) | max ~370ms (first: 2.1s) | max ~370ms | 0 |
| 5. Insert prepared on every connection at startup | ~160ms (first: 230ms) | max ~320–350ms | max ~350–400ms | 0 |
| 6. Dedicated webhook pool + keepalive | ~160ms | **max ~330–420ms under load** | — | 0 |

### 3.1 Stage 1: connection limit
**Symptom:** 5 of 50 concurrent webhooks returned `500`.
**Cause:** Supabase's free session pooler allows **15 clients**. The pool could open 20 (10 + 10 overflow): `EMAXCONNSESSION max clients reached`.
**Second bug found:** asyncpg raised this error unwrapped, so the handler returned `500` instead of `503`.
**Fix:** a fixed pool with **no overflow**, so requests wait briefly for a connection instead of failing, and **all** storage errors map to `503`.

### 3.2 Stage 2: diagnosing where the time went
**Symptom:** even one request on an idle server took ~750–1050ms, about 5–7 round-trips.
**Diagnosis:** each phase was timed separately. **Checking a connection out of the pool took ~520ms**, before the insert even ran. The insert itself was ~150ms (one round-trip).
**Cause:** SQLAlchemy's `pool_pre_ping`. With asyncpg it runs `BEGIN` + ping + `ROLLBACK` on *every* checkout, which is 3 round-trips.

### 3.3 Stage 3: remove hidden round-trips
- **Pre-ping disabled.** A dead connection is handled instead by **retrying once on a fresh connection** (`with_retry` in `app/db.py`). That's safe because every statement on these paths is idempotent (see 1.3).
- **Autocommit** for single-statement writes, so there's no separate `BEGIN`/`COMMIT`.
- **Pool opened at startup.** A new connection costs ~1s (TCP + TLS + auth), and that shouldn't land on a webhook.

**Result: ~1000ms → ~160ms** for a single webhook, essentially one round-trip.

### 3.4 Rejected: Supabase's transaction pooler
**Idea:** the transaction pooler (port 6543) allows ~200 clients instead of 15, so a burst could use 50 connections in parallel.
**Result:** under load it failed with `prepared statement "__asyncpg_…__" does not exist`. The pooler can route asyncpg's *prepare* and *execute* to different server connections. Making it safe needs an explicit transaction around each insert, which adds back the 2 round-trips just removed.
**Decision:** stay on the session pooler (port 5432).

### 3.5 Stage 4: batched inserts for bursts
**Symptom:** single webhooks were fast, but a burst of 20 took **746ms**. With few connections, 20 requests are written in waves, and each wave costs a round-trip plus waiting.
**Fix:** `app/ingest.py`. Concurrent webhooks go into a queue, and a writer stores them together with **one multi-row `INSERT … ON CONFLICT DO NOTHING RETURNING transaction_id`**.
- **Each request still waits for its own row to be committed** before getting `202`. The durability guarantee is unchanged; only the number of round-trips drops.
- **No artificial delay** (no "wait 10ms to collect a batch"). A lone webhook is written immediately. Batches only form from webhooks that pile up while a write is in flight, so batching scales with load automatically.
- `RETURNING` tells each request whether *its* row was new. If the same `transaction_id` appears twice *within one batch*, the first is new and the rest are duplicates. **Tested:** 5 IDs × 4 copies + 10 unique in one burst gave exactly 15 new and 15 duplicates; resending the 5 IDs later gave 5 duplicates.
- The SQL uses `unnest()` arrays, so the statement text is identical for every batch size and asyncpg reuses one prepared statement. A `VALUES (…), (…)` list would be a new statement per batch size.

**Result:** burst of 20 and burst of 50 both at **max ~370ms**. But the *first* requests after startup were slow: 1.1s single, 2.1s first burst.

### 3.6 Stage 5: prepare the insert at startup
**Cause:** the first time each connection runs the insert, asyncpg prepares it and looks up the array/numeric types. That's several round-trips, paid separately on every connection.
**Fix:** at startup, open every ingest connection and run the insert **with empty arrays** on each. It writes nothing, but prepares the statement and caches the types.
**Result:** first request **230ms**, first burst of 20 **~320ms**. Cold start is no longer visible.

### 3.7 Stage 6: real-world conditions (contention and idle time)
**Symptom:** stages 1–5 were measured on a freshly started server with nothing else happening. On a server that had been running the demo for a while, a burst of 20 took **~650ms, and once 6.8s**.
**Causes:**
1. **Contention.** Webhooks shared 7 connections with the status endpoint (the demo polls every in-flight transaction every 2s) and with the worker's writes. Webhooks queued behind reads.
2. **Cold connections.** Connections older than 4 minutes were reopened on next use, so after a quiet period a burst paid reconnect (~1s) and re-prepare again.

**Fix:**
- **Two connection pools:** 4 connections reserved for webhook writes, and 3 for the worker and status reads. A status read can never delay a `202`. The total of 7 still fits Supabase's limit of 15 with room for a second instance during deploys.
- **Webhook connections are never recycled by age.** A **keepalive** task touches them every 60s while idle (the same empty insert as at startup). That keeps them open and prepared, and replaces any dead connection in the background instead of on a webhook.
- The worker and status endpoint also use single-statement autocommit queries with `with_retry`.

**Result:**
- Burst of 20 while 20 statuses are polled every 2s **and** the worker completes 20 transactions at the same moment: **max ~330–420ms**.
- After **5+ minutes of complete idle:** burst of 20 **max 205ms**, single 152ms.
- **Trade-off, by design:** under heavy polling, status reads can take ~1–3s because they get only 3 connections. The brief's latency requirement applies to the webhook, so webhooks get priority.

---

## 4. Measuring correctly

### 4.1 Browser queueing vs server time
**Symptom:** the demo page showed a burst of 20 taking **1025ms**, while scripted tests of the same burst showed ~350ms.
**Diagnosis:** the browser's own timing data (Resource Timing API) showed requests **queued inside the browser for up to 454ms** before being sent. Browsers open only ~6 connections per site, and the page's status polling uses the same 6. In one test the page measured **9.5s** round-trips while the server had answered every request in **under 400ms**.
**Why it matters:** a payment processor calls the webhook server-to-server, without that limit, so the browser number was misleading.
**Fix:**
- Every response carries a standard **`Server-Timing: app;dur=<ms>`** header with the time the API itself spent. It's also visible in browser DevTools.
- The demo reports that server time ("Slowest API response · server time").
- The demo **pauses status polling while a scenario is sending**, so the page doesn't compete with itself.

**Verified in the browser:** single ~150ms, duplicates ~300ms, burst of 20 at 176–368ms of server time, including while 40+ transactions were being polled.

### 4.2 Testing what matters
- `scripts/smoke_test.py` checks the brief's success criteria end to end: health, 5 concurrent duplicates, 50 concurrent webhooks (p50/p95/max), and processing after ~30s.
- Every stage above was re-measured after each change, including the "boring" conditions: first request after startup, after long idle, and under competing load.

---

## 5. Demo page

| Optimisation | Why |
|---|---|
| Progress bars and timers update **in place** every second; the list is only rebuilt when something changes (a webhook is sent or a status changes) | Rebuilding the whole list every second reset animations and could swallow a click that landed mid-render |
| Status polling skips transactions already `PROCESSED` | No wasted requests |
| Polling pauses while a scenario is sending | Stops the page from competing with its own webhooks (see 4.1) |
| Server-reported timing | Shows what the API did, not the browser's queue |

---

## 6. Limits and what's next

- **Distance to the database dominates.** Every number above is ~1–2 round-trips of ~150ms. Deploying the API in the region nearest the database is the biggest remaining win.
- **Capacity is bounded by Supabase's free connection limit (15).** 4 webhook connections × batches of up to 200 rows is ample here. For much higher volume: a larger connection limit (paid tier or direct connection), or a durable buffer such as SQS/Kafka in front of the database.
- **Pre-ping is off,** so a dead connection costs one failed attempt. It's retried automatically, and if the retry fails too the client gets `503` and the processor retries later, so nothing is lost.
- **Unexplained one-off:** once, right after the development server auto-reloaded (`--reload`), it reported ~1–1.4s of server time for a couple of bursts. It couldn't be reproduced afterwards on that instance or a fresh one. The likely cause is a brief connection squeeze while the old and new processes overlapped, but it's unconfirmed. It's a development-only scenario.

## 7. Database moved next to the app server
**Problem:** Render (where the app runs) has no Seoul region. With the database in Seoul, every round-trip from a Render server would cross between countries, and every webhook costs at least one round-trip.
**Fix:** a new Supabase project in **Singapore (`ap-southeast-1`)**, and the app deployed on **Render Singapore**, so both sit in the same region. (Supabase can't move an existing project; the app creates its table on first start, so switching was just a new connection string.) The new project was also created with **"Automatically expose new tables" turned off**, so the `transactions` table isn't reachable through Supabase's public Data API.

**Measured from a local machine** (before deploying):

| | Seoul | Singapore |
|---|---|---|
| Database round-trip | ~150ms | **~79ms** |
| Single webhook | ~160ms | **~90ms** |
| Burst of 20 | ~320–370ms | **~170–260ms** |
| Burst of 50 | ~350–400ms | **~216–232ms** |

**Confirmed on the live deployment** (Render Singapore + Supabase Singapore), measured from a laptop:

| Live check (from a laptop, against the deployed URL) | Result |
|---|---|
| Single webhook, server time | **~6–9ms** |
| Single webhook, round-trip from the laptop | ~120ms (mostly internet travel to Singapore) |
| Burst of 20, server time | max **79ms** |
| Burst of 50, server time | p50 97ms, max **506ms** (0.1 CPU becomes the limit) |
| Smoke test: 5 concurrent duplicates | 1 accepted, 4 ignored |
| Smoke test: single transaction | **PROCESSED after 30s** |
| ~340 webhooks sent from the demo page | all accepted and processed |

The database part of a webhook dropped to **~6–9ms**. On the free tier, the remaining limit under large bursts is Render's **0.1 CPU**: a burst of 50 reached 506ms of server time. That's a hosting limit, not a code one; a paid instance with more CPU removes it.

## 8. No cold starts: keep-awake on Render's free tier
**Problem:** Render spins free web services down after **15 minutes without inbound traffic**. The next request then waits **~1 minute** while the instance boots. A reviewer's first webhook would blow the 500ms budget by two orders of magnitude, and while asleep the background worker isn't running either.
**Fix:** `app/keep_awake.py`. Render sets `RENDER_EXTERNAL_URL` to the service's public URL. The app sends a `HEAD /` to that URL **every 10 minutes**. The request goes out through Render's proxy, so it counts as inbound traffic and the instance never reaches the 15-minute idle limit.
- **Off locally:** the variable only exists on Render, so development is unaffected.
- **Switchable:** set `KEEP_AWAKE=false` to disable it; the interval is `KEEP_AWAKE_INTERVAL_SECONDS`.
- **Cost to be aware of:** a service that never sleeps runs ~720–744 hours a month. Render's free tier has a monthly instance-hour allowance shared by all free services in the workspace, so with other free services awake as well, the allowance can run out. Turn it off once the service no longer needs to be always-on.
- **Why not an external pinger** (UptimeRobot, cron-job.org, a GitHub Actions schedule)? Those work too, but need an extra account or have loose timing (GitHub's scheduled workflows can be delayed by many minutes, enough to miss the 15-minute window). The in-app version needs no setup.
- **Limitation:** it keeps an *awake* instance awake. If Render stops the instance for another reason (e.g. the hour allowance runs out), the pinger stops with it, and the next real request wakes it as usual.
