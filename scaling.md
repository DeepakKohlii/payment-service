# Scaling

The brief gives no traffic numbers. It only says the webhook must respond quickly "even under processing load". So this project doesn't optimise for any particular number. Instead, this document answers two questions:

1. **What can the current setup actually handle, and where does it break?** (measured)
2. **What would it take to handle a very large scale, e.g. 10,000 transactions per second?** (design)

The test sizes used elsewhere in this repo (20 in the demo's burst button, 50 in the smoke test) are just examples, not requirements.

---

## Part 1: What the current setup can handle

### The setup
| Component | Current choice | Hard limit that matters |
|---|---|---|
| API + worker | **One** Python process (FastAPI/uvicorn), on Render's free tier | **0.1 CPU**, 512MB RAM, sleeps after 15 min idle, can't run multiple instances |
| Database | Supabase free (Postgres), Singapore (measurements below were taken against the earlier Seoul project) | **15 connections** via the session pooler; 500MB storage |
| Connections per instance | 4 for webhook writes + 3 for worker and status reads | 7 of the 15 |
| Worker | Up to **5000** transactions processed at once per instance (`WORKER_CONCURRENCY`) | each takes 30s |

### Measured: receiving webhooks
Measured from a local machine with the test client on the same machine, against Supabase in Seoul (~150ms per round-trip):

| Webhooks sent at once | Accepted | Server time p50 / max | Client sees p50 / max |
|---|---|---|---|
| 1 | 1/1 | ~160ms | ~160ms |
| 20 | 20/20 | ~300 / ~370ms | ~300 / ~400ms |
| 100 | 100/100 | 370 / 376ms | 459 / **539ms** |
| 200 | 200/200 | 330 / 368ms | 594 / **990ms** |
| 500 | 500/500 | 263 / 382ms | 2.0s / **5.5s** |
| 1000 | 1000/1000 | 293 / 577ms | 5.8s / **6.2s** |

**What this shows:**
- **Correctness held at every size:** every webhook was accepted, with no errors and no duplicates.
- **The database is not the bottleneck.** Thanks to batched inserts (see [optimisation.md](optimisation.md)), the time spent inside the handler stays ~300–600ms even at 1000 at once.
- **The bottleneck is CPU in a single Python process.** Above ~100 at once, requests queue *before* the handler runs, while the one process accepts connections and parses HTTP and JSON. Here that topped out at **~160 webhooks/second**, with the test client competing for the same CPU. The `Server-Timing` header doesn't include this queueing, so at this scale the client-side number is the honest one.
- **On Render's free tier (0.1 CPU), expect roughly a tenth of that.** That's an estimate to verify against the deployed URL.

### Measured: processing
- **Originally 100 transactions in parallel per instance**, which turned out to break the "~30 seconds" criterion: with a backlog over 100, a new transaction waited a full extra cycle (~60s). Completions were also one write each, so 200 finishing together were spread over 30–39s. Both are fixed (see [optimisation.md §2.1](optimisation.md#21-worker-limit-and-batched-completions)).
- **Now: up to 5000 in parallel per instance, with batched completions.** Measured: **1000 transactions received at once were all processed in 30.2–30.3s**, and a single transaction sent during that backlog still took ~30s.
- **Sustained throughput ≈ 5000 / 30s ≈ 160 transactions/second per instance** on the processing side. That's about the same as what one process can *receive* (~160/s), so the two sides are now balanced.
- **Multiple instances already work.** By accident, two servers were connected to the same database during the 200 test. Each claimed about half the work, and none was processed twice (`SKIP LOCKED` + leases).

### Safe numbers for the demo deployment

| | Comfortable | Degrades | Why |
|---|---|---|---|
| Webhooks arriving at once | up to ~100 (local); fewer on 0.1 CPU | beyond that, responses slow down (queueing), but **none are lost** | CPU of one process |
| Transactions processing in parallel | up to 5000 | more simply wait their turn; processed after 60s, 90s… | `WORKER_CONCURRENCY` |
| Sustained rate | limited by receiving (~160/s locally, less on 0.1 CPU) | backlog grows; everything is still processed eventually | CPU of one process |
| Instances | 2 | a 3rd would exceed Supabase's 15 connections | free DB tier |

**Quick wins without changing the architecture:**
1. ✅ *Done:* `WORKER_CONCURRENCY` raised to 5000.
2. ✅ *Done:* "mark as processed" writes batched into one `UPDATE … WHERE transaction_id = ANY(…)`.
3. ✅ *Done:* database and app in the same region (Singapore).
4. Still open: run several uvicorn worker processes per instance (`--workers N`) on a plan with more than 0.1 CPU. This is the next ceiling, since receiving is CPU-bound.

---

## Part 2: Designing for 10,000 transactions per second

### First: what "10k" means changes everything
Because each transaction takes 30s, the number in flight at once is **rate × 30s** (Little's law):

| Load | Per day | In flight at once | Current design? |
|---|---|---|---|
| 10k per day | 10k | ~4 | ✅ easily |
| 10k per hour | 240k | ~85 | ✅ |
| 10k per minute (~170/s) | 14M | **~5,000** | ⚠️ needs Part 1's quick wins + paid tiers |
| **10k per second** | **864M** | **~300,000** | ❌ needs a different architecture (below) |

At 10k/second, the challenge isn't any single request. It's that **300,000 transactions are in flight at once**, and **10k new rows/second plus 10k updates/second** hit storage around the clock.

### Target architecture

```
            Razorpay / Stripe / …
                    │  webhooks (signed)
                    ▼
        ┌───────────────────────┐
        │ Load balancer         │  multi-AZ, TLS
        └───────────┬───────────┘
                    ▼
        ┌───────────────────────┐  stateless, autoscaled on CPU / request rate
        │ Ingest API (N pods)   │  verify signature → validate → append to log → 202
        └───────────┬───────────┘
                    ▼
        ┌───────────────────────┐  durable, replicated, partitioned by transaction_id
        │ Kafka / Kinesis       │  (the "has it been received?" guarantee lives here)
        └───────┬───────────┬───┘
                ▼           ▼
   ┌────────────────┐  ┌───────────────────────────┐
   │ Writer workers │  │ Processing workers (M pods)│  autoscaled on consumer lag
   │ batch-insert   │  │ call external APIs with    │  rate limits, retries + backoff,
   │ into the DB    │  │ concurrency control        │  circuit breakers, dead-letter queue
   └───────┬────────┘  └─────────────┬─────────────┘
           ▼                         ▼
   ┌─────────────────────────────────────────────┐
   │ Transactions DB: partitioned / sharded      │  + read replicas or a cache for status reads
   └─────────────────────────────────────────────┘
```

### What changes, component by component

**1. Ingest API: stateless and horizontally scaled**
- Many instances behind a load balancer, autoscaled on CPU and request rate. One Python process handles roughly hundreds of requests/second, so 10k/s needs tens of processes plus headroom, across availability zones.
- The request path stays minimal: verify the gateway's **signature**, validate, persist, return `202`. "Persist" becomes an **append to a durable log** (Kafka/Kinesis with replication and `acks=all`) instead of a database insert. Appending is cheaper and absorbs spikes; the database is written asynchronously behind it.
- Keep the principle from this project: **never return `202` until the event is durably stored.**

**2. Queue: replace "Postgres as the queue" with a real log or queue**
- Postgres as a queue (our current design) is excellent up to roughly hundreds to low thousands of jobs/second. Beyond that, constant claim and update queries, row locks and dead-row cleanup (vacuum) on a hot table become the bottleneck.
- **Kafka/Kinesis:** partitioned by `transaction_id`, so all events for one transaction go to the same consumer in order. Consumers scale out to the partition count. Events can be replayed after bugs.
- **Or SQS:** simpler to operate. Its "visibility timeout" is the same idea as our lease. Add a dead-letter queue for poison messages.

**3. Idempotency at scale**
- The same guarantee is still needed: one transaction is processed once, even if the webhook arrives 5 times.
- **Fast dedupe on ingest:** a conditional write such as Redis `SET NX` with a TTL, or a DynamoDB conditional put keyed by `transaction_id`, so duplicates are dropped before the queue.
- **Final guarantee in the database:** keep the unique key on `transaction_id`, so even if dedupe misses one, the write is a no-op. Consumers stay idempotent, because at-least-once delivery means they *will* sometimes see a message twice.
- When calling external APIs (banks, ledgers), pass an **idempotency key** too, so a retried call can't move money twice.

**4. Database**
- **Connections:** a proper pooler (PgBouncer / RDS Proxy) and far more than 15 connections, or a database designed for very high write rates.
- **Write volume:** ~10k inserts/second + ~10k updates/second sustained. Options, roughly in order of effort:
  - A single large Postgres (Aurora / RDS) with **batched writes** (as we already do) can reach the low tens of thousands of simple writes/second when tuned, but with little headroom.
  - **Sharded Postgres** (e.g. Citus, or application-level sharding by `transaction_id` hash) spreads writes across nodes.
  - **DynamoDB / Cassandra** for the transaction-state table: they're built for this write pattern and have conditional writes for idempotency. Keep relational Postgres for reporting and reconciliation.
- **Partition the table by time** (e.g. daily) so indexes stay small, and old data can be archived or dropped cheaply. 864M rows/day can't live in one unpartitioned table.
- **Separate reads from writes:** status queries go to read replicas or a cache (Redis), never to the write primary.

**5. Processing workers**
- A separate service from the API, autoscaled on **queue lag** (how far behind it is), not CPU.
- 300k transactions in flight is fine *if* workers are async. Waiting on I/O costs memory, not threads: thousands of concurrent tasks per process, times tens of processes.
- **The external API becomes the real limit.** Banks and fraud services have rate limits. That needs per-dependency concurrency limits (bulkheads), **retries with exponential backoff and jitter**, **circuit breakers** so a failing dependency doesn't take everything down, and a **dead-letter queue** after N attempts, plus alerting.
- Batch the state updates, as this project already does for inserts.

**6. Status and notifications**
- At this scale clients can't poll per transaction. Push results instead: send an outgoing webhook to the merchant, or use WebSockets/SSE for UIs. Serve any remaining status reads from a cache.

**7. Reliability and operations**
- Multi-AZ for every component. Clear recovery targets: how much data can be lost (ideally zero once `202` is sent), and how fast to recover.
- **Observability:** p99 acknowledgement latency, queue lag, processing time, error and retry rates, dead-letter queue size, and an alert on transactions stuck in `PROCESSING`. Distributed tracing from webhook to completion.
- **Reconciliation:** a periodic job that compares with the gateway's records (e.g. Razorpay's payments API) to catch anything missed, because at 864M/day "rare" problems happen daily.
- **Security:** signature verification and replay protection (reject old timestamps), secrets in a manager, and rate limiting per source.
- **Load testing** before launch (k6 / Locust / Gatling), with explicit SLOs, e.g. "p99 acknowledgement < 300ms at 15k/s".

### Migration path: grow in steps, not one leap

| Step | Handles roughly | Changes |
|---|---|---|
| **Now** | up to ~160 transactions/second on one process (less on 0.1 CPU); ~100 webhooks at once comfortably | — |
| **1. Quick wins** | hundreds/second | multiple processes per instance, paid tiers with real CPU *(higher `WORKER_CONCURRENCY`, batched completions and co-located regions are already done)* |
| **2. Split services** | hundreds/second | API and workers deployed and scaled separately; PgBouncer/RDS Proxy; read replica for status; table partitioning |
| **3. Real queue** | thousands/second | SQS or Kafka instead of Postgres-as-queue; Redis dedupe; dead-letter queue; autoscale workers on lag |
| **4. Large scale** | **10k+/second** | log-first ingest (Kafka/Kinesis), sharded or NoSQL transaction store, push notifications instead of polling, multi-AZ, full observability, reconciliation |

**What carries over unchanged from this project:** acknowledge only after durable storage; keep the request path minimal; idempotency keyed on `transaction_id` with the storage layer as the final guarantee; leases or visibility timeouts for crash recovery; batch writes; and measure before optimising.
