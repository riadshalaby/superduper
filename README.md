
# SUPERDUPER — Secure, Uninterrupted Processing of Events with Robust Delivery and Ultra Persistent Error Recovery

> A **resilient**, **ordered**, and **database-backed** Kafka consumption pattern with pluggable workers (JDBC or Reactive),
> strict per-key ordering, failure retries, heartbeats, and orphan recovery.
>
> Group: `net.rsworld` · Modules: `superduper-*`

---

## Why SUPERDUPER? (Problem & Goals)

### The core problem

Kafka gives you per-**partition** ordering, but not per-**key** ordering across failures and retries.
When message 5 for key A fails, Kafka alone cannot hold messages 6, 7, 8 for key A while continuing to process key B on the same partition.
Retry topics make this worse: a message on a retry topic can overtake an in-flight message on the main topic, breaking key order silently.

When multiple containers consume messages concurrently and persist them for later processing, you need to guarantee:

- **Per-key ordering**: All messages with the same `message_key` are processed strictly in the order they were received.
- **Exactly-one-worker-per-message**: No two containers ever process the same message concurrently.
- **Retries & cut-off**: Failed messages are retried up to a maximum (default `5`) and then marked as `STOPPED`.
- **Resilience**: Crashing containers leave **no** stuck work; **orphaned** `PROCESSING` rows are recovered.
- **Observability**: Heartbeats detect dead containers; status fields show lifecycle progress.
- **Flexibility**: Use **Spring Kafka + JDBC** or **Spring Kafka + R2DBC (reactive processing chain)** with the same algorithm and schema on PostgreSQL or MariaDB.

### Why a transactional inbox and not …

| Alternative | Why it falls short for this problem |
|---|---|
| **Kafka consumer groups alone** | Per-partition ordering, not per-key. A failed message blocks the entire partition, or you skip it and lose ordering for that key. No clean retry/inspect/redrive. |
| **Kafka retry topics + DLQ** | Per-key ordering across retries is extremely hard. A message on the retry topic can overtake an in-flight message on the main topic. |
| **Kafka Streams with state stores** | Solves ordering within a partition, but retry/stop/redrive/inspect requires building most of what SUPERDUPER already provides. |
| **SQS FIFO / Azure Service Bus sessions** | Vendor lock-in. Ordering semantics vary. Loses Kafka's strengths (replay, high throughput, ecosystem). |
| **Redis Streams with consumer groups** | Fast, but no transactional guarantees for the claim-process-update lifecycle. Ordering after failures is manual. |

### What the transactional inbox gives us

- **SQL atomicity for claiming** — `FOR UPDATE SKIP LOCKED` in a single statement is race-free and well-understood.
- **Per-key blocking in the claim query** — the `LEFT JOIN … WHERE status='PROCESSING'` elegantly prevents claiming a key that already has work in flight. This is the hardest part and the SQL handles it in one database round-trip.
- **Database as queryable state** — failures are rows you can `SELECT`, inspect, and redrive. Far more ergonomic than opaque Kafka DLQ topics.
- **Decoupled ingest from processing** — the consumer acks Kafka quickly; the worker processes at its own pace.
- **Portable** — works on PostgreSQL and MariaDB, blocking or reactive, with no vendor lock-in.

### Known tradeoffs

- **Database throughput ceiling.** Every message does INSERT (ingest) + UPDATE (claim) + UPDATE (outcome). Kafka can handle millions/sec; the DB caps throughput at tens of thousands/sec depending on hardware.
- **Poll-based latency.** The claim loop runs on a fixed interval. A message arriving right after a claim cycle waits for the next one, adding P50 latency of roughly half the interval.
- **ShedLock serializes claiming.** Processing is parallel, but the claim entry point is a single-threaded global lock. At very high scale this becomes a funnel.
- **Table growth pressure.** The claim index scan degrades as the `messages` table grows unless cleanup and retention are tuned.

---

## High-Level Architecture

```mermaid
sequenceDiagram
  autonumber
  participant K as Kafka Topic
  participant C as Consumer
  participant M as messages table
  participant L as shedlock
  participant W as Worker
  participant H as MessageHandler / ReactiveMessageHandler
  participant HB as container_heartbeats
  participant O as Orphan Reclaimer

  K->>C: Consume record
  C->>M: INSERT status=READY

  loop claim loop
    W->>L: Acquire claim lock
    W->>M: Claim eligible READY / FAILED rows\nset status=PROCESSING, container_id=workerId
    W->>M: Fetch claimed rows ordered by message_key, id

    loop per claimed row
      W->>H: Invoke business logic
      alt success
        H-->>W: SUCCESS
        W->>M: UPDATE status=PROCESSED\nset processed_at=NOW()
      else failure with retries left
        H-->>W: FAILURE / exception
        W->>M: UPDATE status=FAILED\nincrement retry_count
      else retry limit reached
        H-->>W: FAILURE / exception
        W->>M: UPDATE status=STOPPED
      end
    end
  end

  par heartbeat loop
    W->>HB: Upsert worker heartbeat
  and orphan recovery loop
    O->>M: Reset stale PROCESSING rows to READY
    O->>HB: Check missing / expired heartbeats
  end
```

---

## Data Model (core tables)

```sql
-- messages
id               BIGINT AUTO_INCREMENT / SERIAL PRIMARY KEY
topic            VARCHAR(255) NOT NULL DEFAULT 'default'
message_id       VARCHAR(36) UNIQUE NOT NULL
message_key      VARCHAR(255) NOT NULL
content          TEXT
status           VARCHAR(32) NOT NULL CHECK (status IN ('READY','PROCESSING','PROCESSED','FAILED','STOPPED'))
retry_count      INT DEFAULT 0
container_id     VARCHAR(255) NULL
correlation_id   VARCHAR(36) NULL
message_type     VARCHAR(255) NULL
occurred_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- event time from source header, fallback Kafka record timestamp
received_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- ingest time (consumer write time)
processed_at     TIMESTAMP NULL       -- set when a worker marks message as PROCESSED
last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP

-- heartbeat
container_heartbeats(container_id PRIMARY KEY, last_heartbeat TIMESTAMP)

-- ShedLock (JDBC and reactive worker claim coordination)
shedlock(name PRIMARY KEY, lock_until TIMESTAMP(3), locked_at TIMESTAMP(3), locked_by VARCHAR(255))
```

**Indexes:** the default schema creates `messages(topic, status, message_key, id)` for topic-aware claim scans plus the processing/reclaim indexes shown in `002-worker-claim-indexes-postgres.sql` and `003-worker-claim-indexes-mariadb.sql`.

---

## The Algorithm (Step-by-step)

1. **Ingest (Consumer) → Persist**
   - Spring-Kafka (JDBC) or Spring-Kafka (R2DBC variant) **inserts** each consumed record into `messages` as `READY`.
   - `occurred_at`, `correlation_id`, and `message_type` are resolved through `ConsumerMetadataResolver`.
   - `received_at` is set to ingest time (`NOW()`).
   - The row gets an **auto-increment `id`**, **`message_id`**, **`message_key`**, and payload.

2. **Claim Batch (Worker)**
   - Worker claim is coordinated by **ShedLock** (both JDBC and reactive worker variants).
   - Claiming is done with **one SQL statement per dialect** (candidate selection + update in a single DB roundtrip).
   - Candidate filter rules:
     - Only `READY` or retry-eligible `FAILED` rows (`retry_count < maxRetries`).
     - Rows already being processed for the same key are excluded.
     - A single batch can contain multiple rows for the same key, still ordered by `id`.
   - The `claimBatch` API returns only the **number of updated rows** (for metrics/logging), not a list of ids.
   - Important: the status/retry eligibility predicate is only in the candidate selection; the update phase joins by candidate id.

   **PostgreSQL claim SQL (JDBC and R2DBC variants):**
   ```sql
   WITH candidate AS (
     SELECT m1.id
     FROM messages m1
     LEFT JOIN messages p
       ON p.topic = m1.topic AND p.message_key = m1.message_key AND p.status = 'PROCESSING'
     WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries))
       AND m1.topic = :topic
       AND p.id IS NULL
     ORDER BY m1.id
     LIMIT :batch
     FOR UPDATE OF m1 SKIP LOCKED
   )
   UPDATE messages m
   SET status='PROCESSING', container_id=:cid, last_updated=NOW()
   FROM candidate c
   WHERE m.id = c.id;
   ```

   **MariaDB claim SQL (JDBC and R2DBC variants):**
   ```sql
   UPDATE messages m
   JOIN (
     SELECT id FROM (
       SELECT m1.id
       FROM messages m1
       LEFT JOIN messages p
         ON p.topic = m1.topic AND p.message_key = m1.message_key AND p.status = 'PROCESSING'
       WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries))
         AND m1.topic = :topic
         AND p.id IS NULL
       ORDER BY m1.id
       LIMIT :batch
       FOR UPDATE SKIP LOCKED
     ) candidate_ids
   ) c ON c.id = m.id
   SET m.status='PROCESSING', m.container_id=:cid, m.last_updated=NOW();
   ```

3. **Fetch + Process (Business Logic)**
   - After claiming, each worker fetches all rows currently marked for its own `workerId`:
   ```sql
   SELECT id, message_key, content, retry_count, container_id, correlation_id, message_type
   FROM messages
   WHERE status='PROCESSING' AND container_id=:cid AND topic=:topic
   ORDER BY message_key, id;
   ```
   - Then it invokes your business function:
     - **JDBC:** `MessageHandler` -> returns `SUCCESS` or `FAILURE`.
     - **Reactive:** `ReactiveMessageHandler` -> `Mono<SUCCESS|FAILURE>`.
   - Workers process rows per key in `id` order. If one row for a key fails, later rows for that key already in the batch are released back to `READY`.

4. **On Success or Failure**
   - `SUCCESS` → `status='PROCESSED'`, `processed_at=NOW()`.
   - `FAILURE` (explicit return or thrown exception) → `retry_count = retry_count + 1`.
     - If `retry_count < max` → `status='FAILED'` (will be retried later).
     - Else → `status='STOPPED'` (manual intervention required).

5. **Heartbeat**
   - Every worker periodically **upserts** its `container_id` into `container_heartbeats` with `last_heartbeat=NOW()`.

6. **Orphan Reclaimer**
   - Periodically:
     - If a `PROCESSING` row is **older than timeout**, set it back to `READY`.
     - Or if `container_id` has **no recent heartbeat**, set to `READY`.
   - Guarantees **no stuck** messages when a container dies or loses heartbeats.

7. **Ordering Guarantee**
   - Using **auto-increment `id`** as the **order marker** per key (not `occurred_at`/`received_at`).
   - SQL prevents claiming a key that is already in `PROCESSING`.
   - Worker batch logic preserves in-batch per-key order and releases later same-key rows when an earlier row fails.

---

## Delivery & Processing Guarantees

- **At-least-once ingest:** Kafka offsets are acknowledged only after the consumer persists to `messages`, so a crash before persist acknowledgement can cause redelivery.
- **Ingest deduplication on redelivery:** `message_id` is deterministic from `topic:partition:offset` by default, and consumer writes use upsert semantics, so the same Kafka record does not create duplicate rows.
- **Ownership-safe processing updates:** Worker status updates (`PROCESSED`/`FAILED`/`STOPPED`) require both `id` and `container_id` to match. A stale worker cannot overwrite a row after another worker reclaims it.
- **At-least-once processing overall:** Each claim cycle processes a row at most once for the owning worker, but orphan reclaim can reassign and reprocess messages after failures/timeouts.
- **Operational redrive:** `FAILED` and `STOPPED` rows can be inspected and explicitly redriven to `READY` without bypassing the normal per-key ordering rules.
- **Handler contract:** user handlers should be idempotent because reprocessing can occur.

---

## Project Documentation

## Multi-Topic Configuration

Single-topic mode still works with `superduper.kafka.topic`. Multi-topic mode uses `superduper.topics` and maps each Kafka topic to a handler, optional per-topic overrides, and an optional dedicated table.

```yaml
superduper:
  consumer:
    type: spring
  topics:
    orders:
      kafka-topic: orders.events
      handler: ordersMessageHandler
      batch-size: 200
      max-retries: 5
    invoices:
      kafka-topic: invoices.events
      handler: invoicesMessageHandler
      table: invoices_messages
```

In multi-topic mode:

- the `messages.topic` column stores the Kafka topic name
- workers claim and fetch per topic
- backlog, cleanup, reclaim, and redrive operations route to the correct shared or dedicated table
- observability always carries the topic dimension for worker and maintenance signals

> See [docs/USAGE.md](docs/USAGE.md) for integration guide and configuration reference.
>
> See [docs/EXAMPLES.md](docs/EXAMPLES.md) for running examples locally.
>
> See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the module map and internal architecture.

---

## Integration Tests (Testcontainers)

- **Consumer-only E2E** (Kafka + Postgres): verifies Kafka→DB insert.
- **Combined E2E** (Consumer + Worker): verifies **ordered** processing across keys with multi-step claim/process cycles.
- **Worker ITs**: Heartbeat upsert and orphan reclaim reset.

Run all:
```bash
mvn -T 1C test
```

---

## Examples

Four runnable apps are included:

- `examples/app-blocking` — classic, blocking JDBC + Spring Kafka.
- `examples/app-multitopic-shared` — multi-topic example with a shared `messages` table (blocking).
- `examples/app-multitopic-dedicated` — multi-topic example with per-topic tables (blocking).
- `examples/app-reactive` — reactive processing with Spring Kafka + R2DBC.

See [docs/EXAMPLES.md](docs/EXAMPLES.md) for local setup, multi-container runs, and reproducible SQL assertions.
See [docs/EXAMPLES.md#multi-topic-examples](docs/EXAMPLES.md#multi-topic-examples) for the shared-vs-dedicated multitopic walkthrough.

---

## License

MIT (or choose your own).

---

**S.U.P.E.R.D.U.P.E.R**: _Secure, Uninterrupted Processing of Events with Robust Delivery and Ultra Persistent Error Recovery_ 🚀
