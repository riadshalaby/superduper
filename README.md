
# SUPERDUPER — Secure, Uninterrupted Processing of Events with Robust Delivery and Ultra Persistent Error Recovery

> A **resilient**, **ordered**, and **database-backed** Kafka consumption pattern with pluggable workers (JDBC or Reactive),
> strict per-key ordering, retries with backoff, heartbeats, and orphan recovery.
>
> Group: `net.rsworld` · Modules: `superduper-*`

---

## Why SUPERDUPER? (Problem & Goals)

When multiple containers consume messages concurrently and persist them for later processing, you need to guarantee:

- **Per-key ordering**: All messages with the same `key` are processed strictly in the order they were received.
- **Exactly-one-worker-per-message**: No two containers ever process the same message concurrently.
- **Retries & cut-off**: Failed messages are retried up to a maximum (default `5`) and then marked as `STOPPED`.
- **Resilience**: Crashing containers leave **no** stuck work; **orphaned** `PROCESSING` rows are recovered.
- **Observability**: Heartbeats detect dead containers; status fields show lifecycle progress.
- **Flexibility**: Use **Spring Kafka + JDBC** or **Reactor Kafka + R2DBC** with the same algorithm and schema.

---

## High-Level Architecture

```mermaid
graph LR
  subgraph Kafka
    T[Topic(s)]
  end

  subgraph Apps
    C1[Consumer (Spring-Kafka)]
    C2[Consumer (Reactor-Kafka)]
    W1[Worker JDBC]
    W2[Worker Reactive]
  end

  subgraph DB[(PostgreSQL/MariaDB)]
    M[(messages)]
    H[(container_heartbeats)]
    L[(shedlock)]
  end

  T -->|consume| C1
  T -->|consume| C2
  C1 -->|INSERT READY| M
  C2 -->|INSERT READY| M

  W1 -->|ShedLock claim batch| L
  W1 -->|UPDATE ... PROCESSING| M
  W1 -->|business logic| W1B[MessageHandler] --> W1
  W1 -->|UPDATE PROCESSED / READY / STOPPED| M
  W1 -->|upsert| H

  W2 -->|claim batch (reactive)| M
  W2 -->|business logic (Reactive)| W2B[ReactiveMessageHandler] --> W2
  W2 -->|UPDATE PROCESSED / READY / STOPPED| M
  W2 -->|upsert| H

  ORP[Orphan Reclaimer] -->|RESET stale PROCESSING -> READY| M
  HB[Heartbeat] -->|UPDATE last_heartbeat| H

  classDef db fill:#f7f7f7,stroke:#888
  classDef comp fill:#eef7ff,stroke:#669
  classDef infra fill:#fff5e6,stroke:#c93
  class M,H,L db
  class C1,C2,W1,W2,W1B,W2B comp
  class ORP,HB infra
```

---

## Data Model (core tables)

```sql
-- messages
id               BIGINT AUTO_INCREMENT / SERIAL PRIMARY KEY
uuid             VARCHAR(36) UNIQUE NOT NULL
key              VARCHAR(255) NOT NULL
content          TEXT
status           ENUM('READY','PROCESSING','PROCESSED','FAILED','STOPPED') -- MariaDB: VARCHAR + CHECK
retry_count      INT DEFAULT 0
container_id     VARCHAR(255) NULL
timestamp        TIMESTAMP NULL       -- original send-ts (optional)
last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP

-- heartbeat
container_heartbeats(container_id PRIMARY KEY, last_heartbeat TIMESTAMP)

-- ShedLock (if JDBC workflow)
shedlock(name PRIMARY KEY, lock_until TIMESTAMP(3), locked_at TIMESTAMP(3), locked_by VARCHAR(255))
```

**Indexes:** `messages(key, id)` keeps **per-key ordering by `id`** (auto-increment).

---

## The Algorithm (Step-by-step)

1. **Ingest (Consumer) → Persist**
   - Spring-Kafka (JDBC) or Reactor-Kafka (R2DBC) **inserts** each consumed record into `messages` as `READY`.
   - The row gets an **auto-increment `id`**, **`uuid`**, **`key`**, **payload**, and timestamps.

2. **Claim Batch (Worker)**
   - A single worker instance (JDBC worker protected by **ShedLock**) or each reactive worker **claims** a batch of candidate rows:
     - Only rows with `status IN ('READY') OR ('FAILED' AND retry_count < max)`.
     - **No per-key conflict**: We only claim a row if **no earlier row** for the same key is still `READY|FAILED|PROCESSING`.
     - Each claimed row is atomically set to `PROCESSING` with `container_id = current_worker`.

   **JDBC (PostgreSQL/MariaDB) — JOIN-based claim (no subquery LIMITs):**
   ```sql
   WITH candidate AS (
     SELECT m1.id
     FROM messages m1
     LEFT JOIN messages p
       ON p.`key` = m1.`key` AND p.status = 'PROCESSING'
     WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries))
       AND p.id IS NULL
       AND NOT EXISTS (
           SELECT 1 FROM messages prev
           WHERE prev.`key` = m1.`key`
             AND prev.id   < m1.id
             AND prev.status IN ('READY','FAILED','PROCESSING')
       )
     ORDER BY m1.id
     LIMIT :batch
   )
   UPDATE messages m
      JOIN candidate c ON c.id = m.id
      SET m.status='PROCESSING', m.container_id=:cid, m.last_updated=CURRENT_TIMESTAMP;
   ```

   **PostgreSQL flavor:**
   ```sql
   WITH candidate AS (
     SELECT m1.id
     FROM messages m1
     LEFT JOIN messages p
       ON p.key = m1.key AND p.status = 'PROCESSING'
     WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries))
       AND p.id IS NULL
       AND NOT EXISTS (
           SELECT 1 FROM messages prev
           WHERE prev.key = m1.key
             AND prev.id  < m1.id
             AND prev.status IN ('READY','FAILED','PROCESSING')
       )
     ORDER BY m1.id
     LIMIT :batch
   )
   UPDATE messages m
      SET status='PROCESSING', container_id=:cid, last_updated=NOW()
     FROM candidate c
    WHERE m.id = c.id
   RETURNING m.id;
   ```

3. **Process (Business Logic)**
   - Worker fetches the claimed rows and **invokes your business function**:
     - **JDBC:** `MessageHandler` → returns `SUCCESS` or `RETRY`.
     - **Reactive:** `ReactiveMessageHandler` → `Mono<SUCCESS|RETRY>`.

4. **On Success or Failure**
   - `SUCCESS` → `status='PROCESSED'`.
   - `RETRY` → `retry_count = retry_count + 1`.
     - If `retry_count < max` → `status='READY'` (will be retried later; **per-key ordering** is preserved).
     - Else → `status='STOPPED'` (manual intervention required).

5. **Heartbeat**
   - Every worker periodically **upserts** its `container_id` into `container_heartbeats` with `last_heartbeat=NOW()`.

6. **Orphan Reclaimer**
   - Periodically:
     - If a `PROCESSING` row is **older than timeout**, set it back to `READY`.
     - Or if `container_id` has **no recent heartbeat**, set to `READY`.
   - Guarantees **no stuck** messages when a container dies or loses heartbeats.

7. **Ordering Guarantee**
   - Using **auto-increment `id`** as the **order marker** per key (not `timestamp`), combined with the **candidate filter** that blocks later rows while an earlier row is pending.

---

## Library Modules & How to Use

### Maven coordinates
```xml
<dependency>
  <groupId>net.rsworld</groupId>
  <artifactId>superduper-starter-autoselect</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>

<!-- Select one of the consumer modules -->
<dependency>
  <groupId>net.rsworld</groupId>
  <artifactId>superduper-consumer-kafka</artifactId> <!-- classic Spring-Kafka + JDBC -->
  <version>1.0.0-SNAPSHOT</version>
</dependency>

<dependency>
  <groupId>net.rsworld</groupId>
  <artifactId>superduper-consumer-kafka-reactor-r2dbc</artifactId> <!-- Reactor-Kafka + R2DBC -->
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Choose stack via properties
```yaml
# classic stack
superduper:
  consumer:
    type: spring   # or reactor
  kafka:
    bootstrap-servers: localhost:9092
    group-id: my-group
    topic: my-topic
```

### Provide your business logic

**JDBC (synchronous)**
```java
@Bean
MessageHandler superduperWorker() {
  return row -> {
    // do your processing...
    return ProcessingResult.SUCCESS; // or RETRY
  };
}
```

**Reactive**
```java
@Bean
ReactiveMessageHandler superduperWorkerReactive() {
  return row -> Mono.fromCallable(() -> {
    // reactive processing, I/O, etc.
    return ProcessingResult.SUCCESS; // or RETRY
  });
}
```

The starter:
- Autowires **JDBC Worker + ShedLock** when `type=spring`.
- Autowires **Reactive Worker** when `type=reactor`.
- Consumers persist Kafka records into `messages` automatically.

---

## Running locally (docker-compose)

1. Start infra:
   ```bash
   docker compose up -d
   ```
2. Run **JDBC example app**:
   ```bash
   mvn -pl examples/app-jdbc -am spring-boot:run
   ```
3. Or run **Reactive example app**:
   ```bash
   mvn -pl examples/app-reactive -am spring-boot:run
   ```
4. Send a message to Kafka (via Kafka UI http://localhost:8089):
   - Topic: `superduper.example`
   - Key: `k1`
   - Value: `{ "hello": "world" }`
5. Inspect DB:
   ```sql
   SELECT id, key, content, status, retry_count FROM messages ORDER BY key, id;
   ```

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

## Tuning & Operational Guidance

- **Batch size** (`superduper.worker.batch-size`): Larger batches claim more keys at once; keep moderate to reduce lock contention.
- **Claim interval** (`superduper.worker.claim-interval-ms`): Shorter interval → faster throughput, more DB load.
- **Heartbeat interval** & **orphan timeout**: Tune to your SLO for failover speed.
- **Max retries**: After `STOPPED`, set up alerting; operator can **manually fix** and requeue (`status='READY', retry_count=retry_count-1` or reset to 0).
- **Indexes**: Ensure `messages(key, id)` exists; consider partitioning for very large tables.
- **Idempotency**: Use `uuid` to avoid duplicate inserts from at-least-once Kafka delivery.

---

## Security & Consistency Notes

- The **claim UPDATE** runs in a transaction (JDBC) or atomically (reactive SQL) to avoid race conditions.
- Only the **oldest** pending row per key can be claimed, preserving ordering.
- ShedLock ensures only **one JDBC worker** does the claim at a time (you can still run multiple workers; the critical section is short).
- The reactive variant avoids ShedLock and relies on **SQL invariants** + periodic claiming (and can be scaled horizontally).

---

## Examples

Two runnable apps are included:

- `examples/app-jdbc` — classic, blocking JDBC + Spring Kafka.
- `examples/app-reactive` — fully reactive with Reactor Kafka + R2DBC.

See each module’s `application.yml` and the root `docker-compose.yml` for local setup.

---

## License

MIT (or choose your own).

---

**S.U.P.E.R.D.U.P.E.R**: _Secure, Uninterrupted Processing of Events with Robust Delivery and Ultra Persistent Error Recovery_ 🚀
