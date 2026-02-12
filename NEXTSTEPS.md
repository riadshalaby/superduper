# NEXTSTEPS — Implementation Findings & Fix Guide

> **Purpose:** This file documents all algorithm and implementation problems found in SUPERDUPER
> and provides precise, step-by-step fix instructions for each issue.
> Each finding contains: problem description, all affected files with line references, and the
> exact code change required.
>
> **Severity labels:** 🔴 Critical · 🟠 Significant · 🟡 Medium · 🔵 Design/Doc

---

## Table of Contents

| ID  | Severity | Title |
|-----|----------|-------|
| F1  | 🔴       | Missing ownership check in status-update SQL |
| F2  | 🔴       | Heartbeat window equals heartbeat interval → false orphan detection |
| F3  | 🟠       | `FAILED` status referenced in claim SQL but never set |
| F4  | 🟠       | Reactive consumer silently swallows DB write errors |
| F5  | 🟠       | `ReactiveOrphanReclaimer` uses `subscribe()` with `fixedDelay` → overlapping runs |
| F6  | 🟡       | `reclaimStaleProcessingSql` does not reset `last_updated` |
| F7  | 🟡       | `uuid` not available to message handlers |
| F8  | 🟡       | Heartbeat uses `fixedRate` instead of `fixedDelay` |
| F9  | 🔵       | At-least-once semantics undocumented |
| F10 | 🔵       | `block()` inside reactive `@Scheduled` methods |

---

## F1 🔴 — Missing Ownership Check in Status-Update SQL

### Problem

All three status-update SQL statements (`markProcessed`, `markReadyForRetry`, `markStopped`)
use only `WHERE id=:id`. They do not verify that the row still belongs to the calling worker
(`container_id=:cid`).

**Race condition scenario:**
1. Worker A holds message ID=42 (`PROCESSING`, `container_id=A`)
2. Orphan-Reclaimer resets ID=42 → `READY` (A was processing slowly)
3. Worker B claims and starts processing ID=42 (`PROCESSING`, `container_id=B`)
4. Worker A finishes → calls `markProcessed(42)` → overwrites B's state to `PROCESSED`

Result: double processing, silent state corruption, no error reported.

### Affected Files

| File | Method / Line |
|------|--------------|
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/PostgresJdbcSqlDialect.java` | `markProcessedSql()` L47, `markReadyForRetrySql()` L52, `markStoppedSql()` L57 |
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/MariaDbJdbcSqlDialect.java` | `markProcessedSql()` L48, `markReadyForRetrySql()` L53, `markStoppedSql()` L59 |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/PostgresR2dbcSqlDialect.java` | `markProcessedSql()` L46, `markReadyForRetrySql()` L51, `markStoppedSql()` L57 |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/MariaDbR2dbcSqlDialect.java` | `markProcessedSql()` L48, `markReadyForRetrySql()` L53, `markStoppedSql()` L59 |
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/JdbcSqlDialect.java` | interface method signatures |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/R2dbcSqlDialect.java` | interface method signatures |
| `repository-api/src/main/java/net/rsworld/superduper/repository/api/WorkerMessageRepository.java` | `markProcessed`, `markReadyForRetry`, `markStopped` |
| `repository-api/src/main/java/net/rsworld/superduper/repository/api/ReactiveWorkerMessageRepository.java` | same |
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/JdbcWorkerMessageRepository.java` | `markProcessed()` L47, `markReadyForRetry()` L52, `markStopped()` L58 |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/R2dbcWorkerMessageRepository.java` | `markProcessed()` L55, `markReadyForRetry()` L64, `markStopped()` L74 |
| `worker-blocking/src/main/java/net/rsworld/superduper/worker/blocking/SuperDuperWorkerService.java` | `process()` L132–L164 |
| `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/SuperDuperWorkerReactiveService.java` | `processOne()` L140–L173 |

### Fix

**Step 1 — SQL dialects: add `AND container_id=:cid` to WHERE clause in all three mark methods.**

All four SQL dialect classes. Example for Postgres (identical change for MariaDB and both R2DBC variants):

```java
// markProcessedSql — BEFORE
"UPDATE %s SET status='PROCESSED', processed_at=NOW(), last_updated=NOW() WHERE id=:id"

// markProcessedSql — AFTER
"UPDATE %s SET status='PROCESSED', processed_at=NOW(), last_updated=NOW() WHERE id=:id AND container_id=:cid"

// markReadyForRetrySql — BEFORE
"UPDATE %s SET status='READY', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id"

// markReadyForRetrySql — AFTER
"UPDATE %s SET status='READY', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id AND container_id=:cid"

// markStoppedSql — BEFORE
"UPDATE %s SET status='STOPPED', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id"

// markStoppedSql — AFTER
"UPDATE %s SET status='STOPPED', retry_count=:r, container_id=NULL, last_updated=NOW() WHERE id=:id AND container_id=:cid"
```

**Step 2 — Repository API interfaces: add `workerId` parameter.**

```java
// WorkerMessageRepository.java — BEFORE
void markProcessed(long id);
void markReadyForRetry(long id, int retryCount);
void markStopped(long id, int retryCount);

// WorkerMessageRepository.java — AFTER
void markProcessed(long id, String workerId);
void markReadyForRetry(long id, int retryCount, String workerId);
void markStopped(long id, int retryCount, String workerId);

// ReactiveWorkerMessageRepository.java — BEFORE
Mono<Void> markProcessed(long id);
Mono<Void> markReadyForRetry(long id, int retryCount);
Mono<Void> markStopped(long id, int retryCount);

// ReactiveWorkerMessageRepository.java — AFTER
Mono<Void> markProcessed(long id, String workerId);
Mono<Void> markReadyForRetry(long id, int retryCount, String workerId);
Mono<Void> markStopped(long id, int retryCount, String workerId);
```

**Step 3 — JDBC repository implementation: bind `:cid` parameter.**

```java
// JdbcWorkerMessageRepository.java — BEFORE
public void markProcessed(long id) {
    jdbc.update(dialect.markProcessedSql(), new MapSqlParameterSource().addValue("id", id));
}
public void markReadyForRetry(long id, int retryCount) {
    jdbc.update(dialect.markReadyForRetrySql(),
        new MapSqlParameterSource().addValue("r", retryCount).addValue("id", id));
}
public void markStopped(long id, int retryCount) {
    jdbc.update(dialect.markStoppedSql(),
        new MapSqlParameterSource().addValue("r", retryCount).addValue("id", id));
}

// JdbcWorkerMessageRepository.java — AFTER
public void markProcessed(long id, String workerId) {
    jdbc.update(dialect.markProcessedSql(),
        new MapSqlParameterSource().addValue("id", id).addValue("cid", workerId));
}
public void markReadyForRetry(long id, int retryCount, String workerId) {
    jdbc.update(dialect.markReadyForRetrySql(),
        new MapSqlParameterSource().addValue("r", retryCount).addValue("id", id).addValue("cid", workerId));
}
public void markStopped(long id, int retryCount, String workerId) {
    jdbc.update(dialect.markStoppedSql(),
        new MapSqlParameterSource().addValue("r", retryCount).addValue("id", id).addValue("cid", workerId));
}
```

**Step 4 — R2DBC repository implementation: bind `:cid` parameter.**

```java
// R2dbcWorkerMessageRepository.java — AFTER (same pattern for all three)
public Mono<Void> markProcessed(long id, String workerId) {
    return db.sql(dialect.markProcessedSql())
            .bind("id", id)
            .bind("cid", workerId)
            .fetch().rowsUpdated().then();
}
public Mono<Void> markReadyForRetry(long id, int retryCount, String workerId) {
    return db.sql(dialect.markReadyForRetrySql())
            .bind("id", id)
            .bind("r", retryCount)
            .bind("cid", workerId)
            .fetch().rowsUpdated().then();
}
public Mono<Void> markStopped(long id, int retryCount, String workerId) {
    return db.sql(dialect.markStoppedSql())
            .bind("id", id)
            .bind("r", retryCount)
            .bind("cid", workerId)
            .fetch().rowsUpdated().then();
}
```

**Step 5 — Worker services: pass `workerId` in all call sites.**

```java
// SuperDuperWorkerService.java process() — AFTER
messageRepository.markProcessed(row.id(), workerId);
messageRepository.markReadyForRetry(row.id(), retry, workerId);
messageRepository.markStopped(row.id(), retry, workerId);

// SuperDuperWorkerReactiveService.java processOne() — AFTER
return messageRepository.markProcessed(id, workerId)...
return messageRepository.markReadyForRetry(id, next, workerId)...
return messageRepository.markStopped(id, next, workerId)...
```

### Verification

- Existing unit tests for JDBC/R2DBC repositories must be updated to pass `workerId`.
- Add a test scenario: claim a message, reset it via `reclaimStaleProcessing`, claim it with a
  second worker, then call `markProcessed` with the first worker's ID → assert 0 rows updated.

---

## F2 🔴 — Heartbeat Window Equals Heartbeat Interval

### Problem

`OrphanReclaimer` and `ReactiveOrphanReclaimer` compute `heartbeatWindowSec` from the
heartbeat interval:

```java
this.heartbeatWindowSec = hbMs / 1000;   // default: 30 000 ms → 30 s
```

`HeartbeatService` fires with `fixedRate=30s`. The orphan reclaimer considers any container
dead if no heartbeat arrived in the last 30 s. A single GC pause, load spike, or scheduler
jitter lasting more than 0 seconds causes the heartbeat to arrive late → messages are
wrongly reset to `READY` while the container is still alive and processing them → double
processing.

### Affected Files

| File | Line |
|------|------|
| `worker-blocking/src/main/java/net/rsworld/superduper/worker/blocking/OrphanReclaimer.java` | L21–L26 constructor |
| `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/ReactiveOrphanReclaimer.java` | L22–L27 constructor |

### Fix

Introduce a dedicated property `superduper.worker.heartbeat-window-ms` with a default of
3 × the heartbeat interval (90 000 ms). Replace the inline multiplication with a separate
`@Value`-injected field.

```java
// OrphanReclaimer.java — constructor BEFORE
public OrphanReclaimer(
        WorkerMaintenanceRepository maintenanceRepository,
        SuperduperObserver observer,
        @Value("${superduper.worker.orphan-timeout-ms:120000}") int orphanTimeoutMs,
        @Value("${superduper.worker.heartbeat-interval-ms:30000}") int hbMs) {
    this.orphanTimeoutSec = orphanTimeoutMs / 1000;
    this.heartbeatWindowSec = hbMs / 1000;
}

// OrphanReclaimer.java — constructor AFTER
public OrphanReclaimer(
        WorkerMaintenanceRepository maintenanceRepository,
        SuperduperObserver observer,
        @Value("${superduper.worker.orphan-timeout-ms:120000}") int orphanTimeoutMs,
        @Value("${superduper.worker.heartbeat-window-ms:90000}") int heartbeatWindowMs) {
    this.orphanTimeoutSec = orphanTimeoutMs / 1000;
    this.heartbeatWindowSec = heartbeatWindowMs / 1000;
}
```

Apply the identical change to `ReactiveOrphanReclaimer` (same field names, same annotation).

Also add the new property to `WorkerProperties.java` so it appears in configuration
documentation / IDE autocompletion:

```java
// WorkerProperties.java — add field
private long heartbeatWindowMs = 90_000;
// with getter/setter following the existing pattern
```

Document the new property in `README.md` under "Tuning & Operational Guidance":

```markdown
- **Heartbeat window** (`superduper.worker.heartbeat-window-ms`): How long a container may
  be silent before its PROCESSING rows are reclaimed. Must be > `heartbeat-interval-ms`.
  Default: `90000` (3 × heartbeat interval).
```

### Verification

- Update test constructor calls for `OrphanReclaimer` / `ReactiveOrphanReclaimer` that pass
  `hbMs` directly.
- Confirm default value is 90 000 in both classes.

---

## F3 🟠 — `FAILED` Status Referenced in Claim SQL but Never Set

### Problem

All four SQL dialect `claimBatchSql()` methods include:

```sql
WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries))
```

There is no `markFailed()` method anywhere in the codebase. No code path ever sets
`status='FAILED'`. The `NOT EXISTS` ordering guard also includes `'FAILED'` in its IN-list.
This clause is dead code and adds unnecessary complexity and confusion.

### Affected Files

| File | Line |
|------|------|
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/PostgresJdbcSqlDialect.java` | `claimBatchSql()` L29, `NOT EXISTS` L31 |
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/MariaDbJdbcSqlDialect.java` | `claimBatchSql()` L31, `NOT EXISTS` L33 |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/PostgresR2dbcSqlDialect.java` | `claimBatchSql()` L28, `NOT EXISTS` L31 |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/MariaDbR2dbcSqlDialect.java` | `claimBatchSql()` L30, `NOT EXISTS` L33 |

### Fix

Remove the `FAILED` branch from the candidate WHERE clause and from the ordering guard's
IN-list in all four dialect classes.

```java
// BEFORE (PostgresJdbcSqlDialect — same pattern for all four dialects)
"WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries)) "
+ "AND p.id IS NULL "
+ "AND NOT EXISTS (SELECT 1 FROM %s prev WHERE prev.key = m1.key "
+ "AND prev.id < m1.id AND prev.status IN ('READY','FAILED','PROCESSING')) "

// AFTER
"WHERE m1.status = 'READY' "
+ "AND p.id IS NULL "
+ "AND NOT EXISTS (SELECT 1 FROM %s prev WHERE prev.key = m1.key "
+ "AND prev.id < m1.id AND prev.status IN ('READY','PROCESSING')) "
```

Because `maxRetries` is then no longer used in the SQL, remove the `:maxRetries` bind
parameter from the SQL and from all call sites:

- `JdbcSqlDialect.claimBatchSql()` — remove `:maxRetries` from SQL
- `R2dbcSqlDialect.claimBatchSql()` — same
- `WorkerMessageRepository.claimBatch(String workerId, int batchSize, int maxRetries)` — keep
  `maxRetries` in the Java interface (the service uses it for retry threshold logic), but stop
  binding it to the SQL.
- `JdbcWorkerMessageRepository.claimBatch()` — remove `.addValue("maxRetries", maxRetries)`
- `R2dbcWorkerMessageRepository.claimBatch()` — remove `.bind("maxRetries", maxRetries)`

> **Note:** The DB schema constraint `CHECK (status IN ('READY','PROCESSING','PROCESSED',
> 'FAILED','STOPPED'))` may stay as-is for forward compatibility, but `FAILED` is not used.

### Verification

- All four `claimBatchSql()` unit tests should pass without `maxRetries` bind parameter.
- Run full integration tests to confirm claim behavior is unchanged for READY messages.

---

## F4 🟠 — Reactive Consumer Silently Swallows DB Write Errors

### Problem

`KafkaReactiveR2dbcConsumerService.onMessage()` uses `subscribe()` with `onErrorResume`
that returns `Mono.empty()`:

```java
.onErrorResume(e -> {
    observer.consumerFailed(..., e);
    return Mono.empty();   // error is gone
})
.subscribe();              // no error consumer
```

`ack.acknowledge()` is inside `doOnSuccess`, so on DB failure the message is correctly NOT
acknowledged. However:
1. The error is absorbed by `Mono.empty()` — no exception reaches the `subscribe()` caller.
2. `subscribe()` has no error consumer, so even if the error propagated, it would be silently
   dropped by the default subscriber.
3. The behavior diverges from the blocking consumer (`KafkaConsumerService`) which re-throws,
   letting Spring-Kafka's error handler decide what to do (DLQ, retry, etc.).

### Affected File

| File | Lines |
|------|-------|
| `consumer-kafka-reactive/src/main/java/net/rsworld/superduper/consumer/reactive/KafkaReactiveR2dbcConsumerService.java` | L47–L78 |

### Fix

Replace `subscribe()` with `block()`. The Kafka listener runs on a Kafka consumer thread
(not a Reactor NIO thread), so blocking here is safe and mirrors the blocking consumer's
behavior exactly. Exceptions then propagate to the Spring-Kafka container error handler.

```java
// BEFORE
messageIngestRepository
        .upsertReadyMessage(...)
        .doOnSuccess(unused -> {
            ack.acknowledge();
            observer.consumerSucceeded(...);
        })
        .onErrorResume(e -> {
            observer.consumerFailed(..., e);
            return Mono.empty();
        })
        .subscribe();

// AFTER
try {
    messageIngestRepository
            .upsertReadyMessage(...)
            .doOnSuccess(unused -> {
                ack.acknowledge();
                observer.consumerSucceeded(...);
            })
            .block();
} catch (RuntimeException e) {
    observer.consumerFailed(..., e);
    throw e;
}
```

### Verification

- Add a unit test: mock `upsertReadyMessage` to return `Mono.error(new RuntimeException("DB down"))`,
  verify that `onMessage` throws and `ack.acknowledge()` is NOT called.
- Confirm the blocking consumer test pattern is mirrored.

---

## F5 🟠 — `ReactiveOrphanReclaimer` Uses `subscribe()` + `fixedDelay` → Overlapping Runs

### Problem

`ReactiveOrphanReclaimer.reclaim()` is annotated with `@Scheduled(fixedDelayString=...)`.
The fixed-delay timer starts when the **method returns**, not when the reactive chain
completes. Since `subscribe()` is non-blocking and returns immediately, the delay starts
at once. Under slow DB conditions, the next reclaim run starts before the previous one
finishes → multiple concurrent reclaim operations on the same rows.

```java
@Scheduled(fixedDelayString = "${superduper.worker.orphan-timeout-ms:120000}", ...)
public void reclaim() {
    Mono.when(
            maintenanceRepository.reclaimStaleProcessing(orphanTimeoutSec),
            maintenanceRepository.reclaimMissingHeartbeats(heartbeatWindowSec))
        .doOnSuccess(...)
        .onErrorResume(...)
        .subscribe();   // returns immediately → fixedDelay timer fires right after
}
```

`ReactiveHeartbeatService.heartbeat()` has the same pattern (`subscribe()` + `fixedRate`).

### Affected Files

| File | Method / Line |
|------|--------------|
| `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/ReactiveOrphanReclaimer.java` | `reclaim()` L32–L44 |
| `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/ReactiveHeartbeatService.java` | `heartbeat()` L26–L38 |

### Fix

Replace `.subscribe()` with `.block()` so the method blocks until the reactive chain
completes and the `fixedDelay` timer starts only after actual completion.

```java
// ReactiveOrphanReclaimer.java — BEFORE
.subscribe();

// ReactiveOrphanReclaimer.java — AFTER
.block();
```

```java
// ReactiveHeartbeatService.java — BEFORE
maintenanceRepository
        .heartbeat(workerId)
        .doOnSuccess(...)
        .onErrorResume(...)
        .subscribe();

// ReactiveHeartbeatService.java — AFTER
try {
    maintenanceRepository
            .heartbeat(workerId)
            .doOnSuccess(...)
            .block();
} catch (RuntimeException e) {
    observer.maintenanceFailed(..., e);
    // do not re-throw for heartbeat — a single missed heartbeat is non-fatal
}
```

### Verification

- Confirm that `reclaim()` and `heartbeat()` no longer return before the DB operations finish.
- Add a test that checks the sequential nature of repeated reclaim invocations.

---

## F6 🟡 — `reclaimStaleProcessingSql` Does Not Reset `last_updated`

### Problem

When messages are reclaimed from `PROCESSING` → `READY`, `last_updated` is not refreshed:

```sql
UPDATE messages SET status='READY', container_id=NULL
WHERE status='PROCESSING' AND last_updated < (NOW() - (:t * INTERVAL '1 second'))
```

After reclaim, the READY row still carries the old stale timestamp. This makes
`last_updated` unreliable as an indicator of when the status last changed, and can
confuse observability/dashboards.

### Affected Files

| File | Method / Line |
|------|--------------|
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/PostgresJdbcSqlDialect.java` | `reclaimStaleProcessingSql()` L65–L68 |
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/MariaDbJdbcSqlDialect.java` | `reclaimStaleProcessingSql()` L67–L70 |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/PostgresR2dbcSqlDialect.java` | `reclaimStaleProcessingSql()` L71–L74 |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/MariaDbR2dbcSqlDialect.java` | `reclaimStaleProcessingSql()` L73–L76 |

### Fix

Add `last_updated=NOW()` to the SET clause in all four dialect implementations.

```java
// PostgresJdbcSqlDialect — BEFORE
return ("UPDATE %s SET status='READY', container_id=NULL "
        + "WHERE status='PROCESSING' AND last_updated < (NOW() - (:t * INTERVAL '1 second'))")
        .formatted(messagesTable);

// PostgresJdbcSqlDialect — AFTER
return ("UPDATE %s SET status='READY', container_id=NULL, last_updated=NOW() "
        + "WHERE status='PROCESSING' AND last_updated < (NOW() - (:t * INTERVAL '1 second'))")
        .formatted(messagesTable);

// MariaDbJdbcSqlDialect — BEFORE
return ("UPDATE %s SET status='READY', container_id=NULL "
        + "WHERE status='PROCESSING' AND last_updated < TIMESTAMPADD(SECOND, -:t, NOW())")
        .formatted(messagesTable);

// MariaDbJdbcSqlDialect — AFTER
return ("UPDATE %s SET status='READY', container_id=NULL, last_updated=NOW() "
        + "WHERE status='PROCESSING' AND last_updated < TIMESTAMPADD(SECOND, -:t, NOW())")
        .formatted(messagesTable);
```

Apply the identical change to `PostgresR2dbcSqlDialect` and `MariaDbR2dbcSqlDialect`.

### Verification

- After a reclaim, query `last_updated` for the affected rows and confirm it is approximately
  `NOW()`, not the old stale timestamp.

---

## F7 🟡 — `uuid` Not Available to Message Handlers

### Problem

`fetchClaimedForWorker` does not select the `uuid` column. `ClaimedMessage` has no `uuid`
field. Both worker services pass `null` as uuid when constructing `MessageRow`:

```java
// SuperDuperWorkerService.java L139
new MessageRow(row.id(), null, row.key(), ...)
//                        ^^^^ always null
```

Handlers that need the UUID for idempotency checks (e.g., writing to an external system
with a deduplication key) cannot obtain it.

### Affected Files

| File | Change needed |
|------|--------------|
| `repository-api/src/main/java/net/rsworld/superduper/repository/api/ClaimedMessage.java` | Add `uuid` field |
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/PostgresJdbcSqlDialect.java` | `fetchClaimedForWorkerSql()` L41 |
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/MariaDbJdbcSqlDialect.java` | `fetchClaimedForWorkerSql()` L43 |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/PostgresR2dbcSqlDialect.java` | `fetchClaimedForWorkerSql()` L40 |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/MariaDbR2dbcSqlDialect.java` | `fetchClaimedForWorkerSql()` L42 |
| `repository-jdbc/src/main/java/net/rsworld/superduper/repository/jdbc/JdbcWorkerMessageRepository.java` | `fetchClaimedForWorker()` L34–L44: read `uuid` from ResultSet |
| `repository-r2dbc/src/main/java/net/rsworld/superduper/repository/r2dbc/R2dbcWorkerMessageRepository.java` | `fetchClaimedForWorker()` L38–L51: read `uuid` from row |
| `worker-blocking/src/main/java/net/rsworld/superduper/worker/blocking/SuperDuperWorkerService.java` | `process()` L139: pass `row.uuid()` |
| `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/SuperDuperWorkerReactiveService.java` | `processOne()` L144: pass `row.uuid()` |

### Fix

**Step 1 — Extend `ClaimedMessage` record:**

```java
// ClaimedMessage.java — BEFORE
public record ClaimedMessage(Long id, String key, String content, Integer retryCount, String containerId) {}

// ClaimedMessage.java — AFTER
public record ClaimedMessage(Long id, String uuid, String key, String content, Integer retryCount, String containerId) {}
```

**Step 2 — Add `uuid` to fetch SQL in all four dialect classes:**

```java
// BEFORE (all four dialects, slight backtick/alias differences)
"SELECT id, key AS message_key, content, retry_count, container_id "
+ "FROM %s WHERE status='PROCESSING' AND container_id=:cid ORDER BY key, id"

// AFTER
"SELECT id, uuid, key AS message_key, content, retry_count, container_id "
+ "FROM %s WHERE status='PROCESSING' AND container_id=:cid ORDER BY key, id"
```

**Step 3 — Read `uuid` in JDBC repository:**

```java
// JdbcWorkerMessageRepository.fetchClaimedForWorker — AFTER
(rs, rn) -> new ClaimedMessage(
        rs.getLong("id"),
        rs.getString("uuid"),           // new
        rs.getString("message_key"),
        rs.getString("content"),
        rs.getInt("retry_count"),
        rs.getString("container_id"))
```

**Step 4 — Read `uuid` in R2DBC repository:**

```java
// R2dbcWorkerMessageRepository.fetchClaimedForWorker — AFTER
return new ClaimedMessage(
        id == null ? null : id.longValue(),
        row.get("uuid", String.class),  // new
        row.get("message_key", String.class),
        row.get("content", String.class),
        retry == null ? 0 : retry.intValue(),
        row.get("container_id", String.class));
```

**Step 5 — Pass `uuid` in worker services:**

```java
// SuperDuperWorkerService.java process() — AFTER
new MessageRow(row.id(), row.uuid(), row.key(), row.content(), "PROCESSING", row.retryCount(), row.containerId())

// SuperDuperWorkerReactiveService.java processOne() — AFTER
MessageRow mr = new MessageRow(id, row.uuid(), row.key(), row.content(), "PROCESSING", retry, workerId);
```

### Verification

- Update repository unit tests that construct `ClaimedMessage` to include a `uuid` argument.
- Add an assertion that `MessageRow.uuid()` is non-null when processing a seeded message.

---

## F8 🟡 — Heartbeat Uses `fixedRate` Instead of `fixedDelay`

### Problem

`HeartbeatService` and `ReactiveHeartbeatService` use `@Scheduled(fixedRateString=...)`.
All other scheduled tasks use `fixedDelay`. With `fixedRate`, if a heartbeat DB call takes
longer than the interval (DB slowness, connection pool exhaustion), the next invocation is
queued immediately after the previous finishes, causing thread stacking and bursty load on
the DB. `fixedDelay` waits for the configured interval *after* completion, which is the
safer and more consistent behavior.

### Affected Files

| File | Line |
|------|------|
| `worker-blocking/src/main/java/net/rsworld/superduper/worker/blocking/HeartbeatService.java` | L22–L24 |
| `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/ReactiveHeartbeatService.java` | L23–L25 |

### Fix

```java
// HeartbeatService.java — BEFORE
@Scheduled(
        fixedRateString = "${superduper.worker.heartbeat-interval-ms:30000}",
        initialDelayString = "${superduper.worker.heartbeat-initial-delay-ms:0}")

// HeartbeatService.java — AFTER
@Scheduled(
        fixedDelayString = "${superduper.worker.heartbeat-interval-ms:30000}",
        initialDelayString = "${superduper.worker.heartbeat-initial-delay-ms:0}")
```

Apply the identical change (only `fixedRateString` → `fixedDelayString`) to
`ReactiveHeartbeatService`.

### Verification

- No functional test needed; confirm the annotation attribute name is changed.
- Review scheduler configuration to ensure the heartbeat thread pool can handle the load.

---

## F9 🔵 — At-Least-Once Semantics Undocumented

### Problem

Messages can be processed more than once:
1. Handler executes business logic (external side-effect occurs).
2. The subsequent `markProcessed()` DB call fails (network glitch, crash).
3. Orphan-Reclaimer resets the message to `READY`.
4. Another worker picks it up and the handler runs again.

This is unavoidable given the design (at-least-once delivery), but it is not documented.
Users may implement non-idempotent handlers without realising they can be called multiple
times for the same message.

### Affected Files

| File | Where to add |
|------|-------------|
| `README.md` | Section "The Algorithm", Step 3 |
| `worker-blocking/src/main/java/net/rsworld/superduper/worker/blocking/MessageHandler.java` | Javadoc on interface |
| `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/ReactiveMessageHandler.java` | Javadoc on interface |

### Fix

**README.md — add warning in Step 3 ("Fetch + Process"):**

```markdown
> **Important:** Processing is **at-least-once**. If a container crashes after business logic
> completes but before `status='PROCESSED'` is written, the orphan reclaimer will reset the
> message to `READY` and another worker will process it again. **Your `MessageHandler` /
> `ReactiveMessageHandler` must be idempotent.**
```

**MessageHandler.java — add Javadoc:**

```java
/**
 * Processes a single message claimed from the queue.
 *
 * <p><strong>Idempotency contract:</strong> This method may be invoked more than once for
 * the same message (at-least-once delivery). Implementations must be idempotent — applying
 * the same message twice must produce the same observable result as applying it once.
 *
 * @param row the message to process
 * @return {@link ProcessingResult#SUCCESS} if the message was handled successfully,
 *         {@link ProcessingResult#RETRY} to schedule a retry
 * @throws MessageHandlingException if a non-retriable error occurs
 */
ProcessingResult handle(MessageRow row) throws MessageHandlingException;
```

Apply equivalent Javadoc to `ReactiveMessageHandler.handle(MessageRow)`.

---

## F10 🔵 — `block()` Inside Reactive `@Scheduled` Methods

### Problem

`SuperDuperWorkerReactiveService.schedule()` and `claimBatch()` call `.block()` and
`.blockOptional()` respectively. This bridges the reactive pipeline to the imperative
Spring scheduler thread.

While this does not cause a deadlock (Spring's task scheduler uses regular JVM threads,
not Reactor NIO threads), it defeats many benefits of the reactive stack: the scheduler
thread is blocked during the entire claim+process cycle, and Reactor's backpressure and
error propagation are bypassed.

### Affected File

| File | Lines |
|------|-------|
| `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/SuperDuperWorkerReactiveService.java` | `claimBatch()` L133–L138, `schedule()` L130 |

### Recommended Approach

This is a **design-level issue**, not a bug. A proper fix would involve:

1. Configuring a dedicated `ThreadPoolTaskScheduler` for the worker (avoiding any possibility
   of scheduler thread starvation).
2. Keeping `.block()` as-is but documenting that the reactive worker is "reactive at the
   repository layer, imperative at the scheduling layer" — a pragmatic hybrid.

For now, document the trade-off with a code comment and ensure the task scheduler has
sufficient threads to avoid blocking all other scheduled tasks:

```java
// In AutoSelectConfiguration or application configuration:
@Bean
public TaskScheduler superduperScheduler() {
    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(3);  // heartbeat + orphan-reclaimer + worker each need a thread
    scheduler.setThreadNamePrefix("superduper-");
    return scheduler;
}
```

---

## Suggested Implementation Order

For maximum safety and correctness, implement findings in this order:

1. **F6** — `last_updated` reset (isolated SQL change, zero interface impact)
2. **F8** — `fixedRate` → `fixedDelay` for heartbeat (annotation change only)
3. **F5** — `ReactiveOrphanReclaimer` `subscribe()` → `block()` (two-line change)
4. **F4** — Reactive consumer error handling (single file)
5. **F2** — Heartbeat window property (add property, update two constructors)
6. **F3** — Remove `FAILED` from claim SQL (SQL-only, remove `maxRetries` binding)
7. **F7** — Add `uuid` to `ClaimedMessage` + fetch SQL (multi-file but mechanical)
8. **F1** — Ownership check in status updates (largest change, touch 12 files)
9. **F9** — Documentation (README + Javadoc, no code)
10. **F10** — Dedicated scheduler bean (optional, infrastructure improvement)

After each group of changes run:

```bash
mvn -q spotless:apply
mvn -T 1C verify
```
