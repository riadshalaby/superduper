# Plan — v0.4.1

Status: **ready**

Goal: stabilize the `0.4.x` line for production operations — operable failure handling, improved observability, and claim-path regression guardrails.

---

## Priority 1: Redrive and Failure Operations

Approach: **service-layer + repository-layer**. New repository methods for query/redrive, wrapped by dedicated service classes with validation and observability integration.

### Step 1.1 — Repository API: add redrive query and mutation methods

**Files to modify:**

- `repository-api/.../WorkerMessageRepository.java`
- `repository-api/.../ReactiveWorkerMessageRepository.java`

**New methods (blocking):**

```java
List<ClaimedMessage> findByStatus(String status, int limit);
int redriveById(long id);
int redriveByStatus(String status, int limit);
```

**New methods (reactive):**

```java
Flux<ClaimedMessage> findByStatus(String status, int limit);
Mono<Integer> redriveById(long id);
Mono<Integer> redriveByStatus(String status, int limit);
```

**Semantics:**

- `findByStatus` — returns messages in `FAILED` or `STOPPED` status, ordered by `id`, capped by `limit`. Reuses the existing `ClaimedMessage` record (it already carries all needed fields).
- `redriveById` — sets `status='READY'`, `retry_count=0`, `container_id=NULL`, `last_updated=NOW()` for a single row WHERE `id=? AND status IN ('FAILED','STOPPED')`. Returns number of rows updated (0 or 1).
- `redriveByStatus` — same transition but applies to up to `limit` rows matching the given status, ordered by `id`. Returns number of rows updated.

### Step 1.2 — SQL Dialect: add redrive SQL

**Files to modify:**

- `repository-jdbc/.../JdbcSqlDialect.java` (interface)
- `repository-jdbc/.../PostgresJdbcSqlDialect.java`
- `repository-jdbc/.../MariaDbJdbcSqlDialect.java`
- `repository-r2dbc/.../R2dbcSqlDialect.java` (interface)
- `repository-r2dbc/.../PostgresR2dbcSqlDialect.java`
- `repository-r2dbc/.../MariaDbR2dbcSqlDialect.java`

**New dialect methods:**

```java
String findByStatusSql();
String redriveByIdSql();
String redriveByStatusSql();
```

**SQL templates (Postgres):**

```sql
-- findByStatusSql
SELECT id, message_id, message_key, content, retry_count, container_id, correlation_id, message_type
FROM messages WHERE status = :status ORDER BY id LIMIT :limit;

-- redriveByIdSql
UPDATE messages
SET status = 'READY', retry_count = 0, container_id = NULL, last_updated = NOW()
WHERE id = :id AND status IN ('FAILED', 'STOPPED');

-- redriveByStatusSql (Postgres)
UPDATE messages
SET status = 'READY', retry_count = 0, container_id = NULL, last_updated = NOW()
WHERE id IN (
  SELECT id FROM messages WHERE status = :status ORDER BY id LIMIT :limit
);
```

**SQL templates (MariaDB):**

```sql
-- redriveByStatusSql (MariaDB — needs subquery wrapper)
UPDATE messages m
JOIN (
  SELECT id FROM (
    SELECT id FROM messages WHERE status = :status ORDER BY id LIMIT :limit
  ) redrive_ids
) r ON r.id = m.id
SET m.status = 'READY', m.retry_count = 0, m.container_id = NULL, m.last_updated = NOW();
```

`findByStatusSql` and `redriveByIdSql` are dialect-independent and can use a shared default.

### Step 1.3 — Repository implementations: wire redrive SQL

**Files to modify:**

- `repository-jdbc/.../JdbcWorkerMessageRepository.java`
- `repository-r2dbc/.../R2dbcWorkerMessageRepository.java`

Implement the three new interface methods by delegating to the dialect SQL strings, binding parameters, and executing via `NamedParameterJdbcTemplate` (JDBC) or `DatabaseClient` (R2DBC). Follow the existing patterns in each class.

### Step 1.4 — Observer API: add redrive signal

**Files to modify:**

- `observability-api/.../SuperduperObserver.java`

**New method:**

```java
default void workerRedriven(WorkerObservation observation, int redrivenCount) {}
```

Default no-op so existing implementations compile without changes, but we update the concrete implementations in Step 1.5.

### Step 1.5 — Observer implementations: emit redrive events

**Files to modify:**

- `observability-api/.../ObservabilitySignal.java` — add `REDRIVE` signal (if the enum exists, otherwise add to the signal-filtering logic in `ObservabilitySettings`).
- `observability-logging/.../LoggingSuperduperObserver.java` — log `"worker.redriven mode={} workerId={} redrivenCount={} durationMs={}"` at INFO level.
- `observability-metrics/.../MetricsSuperduperObserver.java` — register counter `superduper.worker.redriven.total` with tag `mode`.

### Step 1.6 — Redrive service classes

**New files:**

- `worker-blocking/.../RedriveService.java`
- `worker-reactive/.../ReactiveRedriveService.java`

**RedriveService (blocking):**

```java
public class RedriveService {

    private final WorkerMessageRepository repository;
    private final SuperduperObserver observer;
    private final String workerId;

    /** Inspect messages in FAILED or STOPPED status. */
    public List<ClaimedMessage> inspect(String status, int limit);

    /** Redrive a single message by id. Returns true if the message was redriven. */
    public boolean redriveOne(long id);

    /** Redrive up to limit messages in the given status. Returns count redriven. */
    public int redriveBatch(String status, int limit);
}
```

- `inspect` validates that `status` is `FAILED` or `STOPPED`, then delegates to `repository.findByStatus()`.
- `redriveOne` delegates to `repository.redriveById()`, emits `workerRedriven(observation, count)`.
- `redriveBatch` delegates to `repository.redriveByStatus()`, emits `workerRedriven(observation, count)`.
- All methods guard against invalid status values (throw `IllegalArgumentException` for anything other than `FAILED`/`STOPPED`).

**ReactiveRedriveService:** same contract but returns `Flux`/`Mono`.

### Step 1.7 — Auto-configuration: wire redrive services

**Files to modify:**

- `starter-autoselect/.../AutoSelectConfiguration.java`

Register `RedriveService` bean (blocking stack) and `ReactiveRedriveService` bean (reactive stack) alongside the existing worker/heartbeat/orphan beans, injecting the same `workerId`, repository, and observer.

### Step 1.8 — Integration tests for redrive

**New test files:**

- `repository-jdbc/.../JdbcRedriveIntegrationTest.java` (Postgres)
- `repository-jdbc/.../JdbcRedriveMariaDbIntegrationTest.java` (MariaDB)
- `repository-r2dbc/.../R2dbcRedriveIntegrationTest.java` (Postgres)
- `repository-r2dbc/.../R2dbcRedriveMariaDbIntegrationTest.java` (MariaDB)
- `worker-blocking/.../RedriveServiceTest.java` (unit, mocked repo)
- `worker-reactive/.../ReactiveRedriveServiceTest.java` (unit, mocked repo)

**Scenarios to cover:**

1. `findByStatus("FAILED", N)` returns only `FAILED` rows, ordered by `id`, capped at `N`.
2. `findByStatus("STOPPED", N)` returns only `STOPPED` rows.
3. `redriveById(id)` transitions a `FAILED` row to `READY` with `retry_count=0`, `container_id=NULL`.
4. `redriveById(id)` transitions a `STOPPED` row to `READY`.
5. `redriveById(id)` returns 0 for a `PROCESSING` or `READY` row (no-op).
6. `redriveByStatus("FAILED", N)` redrives up to N rows.
7. Same-key redrive: redrive a `STOPPED` message when another message for the same key is `READY` — verify the claim query picks them up in `id` order on the next cycle.
8. Same-key redrive with `PROCESSING` sibling: redrive a `STOPPED` message whose key has a `PROCESSING` sibling — verify the redriven row is not claimed until the `PROCESSING` sibling completes.
9. Observer emission: `workerRedriven` is called with correct count.

### Step 1.9 — Documentation

**Files to modify:**

- `docs/USAGE.md` — add "Redrive" section documenting the `RedriveService` / `ReactiveRedriveService` API, the retry/redrive contract, and example usage.
- `README.md` — add a short mention of redrive capability under the algorithm or delivery-guarantees section.

---

## Priority 2: Observability and Runtime Diagnostics

### Step 2.1 — Batch summary observation

**Files to modify:**

- `observability-api/.../WorkerObservation.java` — no structural change needed; the existing fields (`batchSize`, `durationMs`) are sufficient for batch-level events.
- `observability-api/.../SuperduperObserver.java` — add:

```java
default void workerBatchCompleted(WorkerObservation observation, int processed, int failed, int stopped) {}
```

- `observability-logging/.../LoggingSuperduperObserver.java` — log batch summary at INFO level:

```
worker.batch.completed mode={} workerId={} batchSize={} processed={} failed={} stopped={} durationMs={}
```

- `observability-metrics/.../MetricsSuperduperObserver.java` — no new metric needed; the existing per-message counters already track processed/retried/stopped totals. The batch-completed log line is the primary deliverable here.

### Step 2.2 — Emit batch summary from worker services

**Files to modify:**

- `worker-blocking/.../SuperDuperWorkerService.java` — after `process()` completes, count outcomes (processed, failed, stopped) and call `observer.workerBatchCompleted(observation, processed, failed, stopped)`.
- `worker-reactive/.../SuperDuperWorkerReactiveService.java` — same, accumulate counts across the reactive chain and emit batch summary on completion.

Track counts by adding simple counters in the process loop / reactive chain (no new fields on `ClaimedMessage`).

### Step 2.3 — Move per-message processing logs to DEBUG

**Files to modify:**

- `observability-logging/.../LoggingSuperduperObserver.java`
  - Change `workerProcessed` log from `info()` to `debug()`.
  - Change `consumerSucceeded` log from `info()` to `debug()`.
  - Keep `workerRetried` at WARN, `workerStopped` at ERROR, `workerFailed` at ERROR (unchanged).
  - Keep `workerClaimed` at INFO (lifecycle event, low frequency).
  - The new `workerBatchCompleted` at INFO replaces per-message INFO noise.

### Step 2.4 — Expand maintenance observability: reclaim counts

**Files to modify:**

- `observability-api/.../MaintenanceObservation.java` — this is a record; no change needed because we can pass reclaim counts through the observer method signature instead.
- `observability-api/.../SuperduperObserver.java` — change signature (or add overload):

```java
default void maintenanceSucceeded(MaintenanceObservation observation, int reclaimedCount) {
    maintenanceSucceeded(observation); // backward compat
}
```

- `repository-api/.../WorkerMaintenanceRepository.java` — change `reclaimStaleProcessing` and `reclaimMissingHeartbeats` return types from `void` to `int` (number of rows reclaimed).
- `repository-api/.../ReactiveWorkerMaintenanceRepository.java` — change return types from `Mono<Void>` to `Mono<Integer>`.
- `repository-jdbc/.../JdbcWorkerMaintenanceRepository.java` — return `jdbcTemplate.update(...)` result (already returns int from JDBC, just not exposed).
- `repository-r2dbc/.../R2dbcWorkerMaintenanceRepository.java` — capture `rowsUpdated()` from the reactive result.
- `worker-blocking/.../OrphanReclaimer.java` — pass reclaim counts to `maintenanceSucceeded(observation, count)`.
- `worker-reactive/.../ReactiveOrphanReclaimer.java` — same.
- `observability-logging/.../LoggingSuperduperObserver.java` — log reclaim count:

```
maintenance.ok mode={} workerId={} operation={} reclaimedCount={} durationMs={}
```

- `observability-metrics/.../MetricsSuperduperObserver.java` — add gauge or counter `superduper.maintenance.reclaimed.total` with tags `mode`, `operation`.

### Step 2.5 — Queue-health metrics

**Files to modify:**

- `repository-api/.../WorkerMessageRepository.java` — add:

```java
Map<String, Long> countByStatus();
```

- `repository-api/.../ReactiveWorkerMessageRepository.java` — add:

```java
Mono<Map<String, Long>> countByStatus();
```

- SQL dialects — add `countByStatusSql()`:

```sql
SELECT status, COUNT(*) AS cnt FROM messages GROUP BY status;
```

- JDBC/R2DBC implementations — implement query, return map of `status -> count`.
- `observability-metrics/.../MetricsSuperduperObserver.java` — register a `Gauge` backed by periodic polling (or the caller polls and sets). Gauge names:
  - `superduper.queue.backlog{status=READY}` — messages waiting to be claimed.
  - `superduper.queue.backlog{status=FAILED}` — messages awaiting retry.
  - `superduper.queue.backlog{status=STOPPED}` — messages requiring intervention.
  - `superduper.queue.backlog{status=PROCESSING}` — messages currently in-flight.

Implementation note: to avoid coupling the observer to a repository, add a lightweight `QueueHealthService` (blocking) / `ReactiveQueueHealthService` (reactive) in the worker modules that periodically polls `countByStatus()` and publishes gauge values. Auto-configure alongside other services. Schedule at a configurable interval (default: 60s).

**New files:**

- `worker-blocking/.../QueueHealthService.java`
- `worker-reactive/.../ReactiveQueueHealthService.java`

### Step 2.6 — Documentation: operational monitoring

**Files to modify:**

- `docs/USAGE.md` — add "Operational Monitoring" section:
  - Table of all metric names, tags, and what they represent.
  - Table of key log lines and their levels.
  - Example log output with `correlation_id` and `message_type` metadata fields.
  - Recommended Grafana/Prometheus queries for: backlog growth, claim rate, failure rate, stopped-message alerts.
  - Heartbeat monitoring: how to alert on missing heartbeats.
  - Orphan reclaim: how to detect stale `PROCESSING` rows.

---

## Priority 3: Claim-Path Performance Regression Guardrails

### Step 3.1 — MariaDB explain-plan integration test

**Context:** `JdbcWorkerClaimExplainIntegrationTest` already exists for Postgres. Add the MariaDB equivalent.

**New files:**

- `repository-jdbc/.../JdbcWorkerClaimExplainMariaDbIntegrationTest.java`

**Approach:**

- Use Testcontainers with MariaDB.
- Seed the table with a representative dataset (e.g., 10k rows: mixed `READY`, `FAILED`, `PROCESSING`, `PROCESSED` across 100 distinct keys).
- Run `EXPLAIN` on the claim query and assert:
  - The query uses the expected indexes (`idx_messages_ready_claim_id_key`, `idx_messages_processing_key_id`).
  - No full table scan (`type` is not `ALL`).
- Run `EXPLAIN` on the fetch query and assert index usage.

### Step 3.2 — R2DBC explain-plan integration tests

**New files:**

- `repository-r2dbc/.../R2dbcWorkerClaimExplainIntegrationTest.java` (Postgres)
- `repository-r2dbc/.../R2dbcWorkerClaimExplainMariaDbIntegrationTest.java` (MariaDB)

Same dataset and assertions as the JDBC variants, executed through the R2DBC `DatabaseClient`.

### Step 3.3 — Hot-key and mixed-key scenarios

**Files to modify:**

- `repository-jdbc/.../JdbcWorkerClaimExplainIntegrationTest.java` (existing, Postgres)
- `repository-jdbc/.../JdbcWorkerClaimExplainMariaDbIntegrationTest.java` (new, Step 3.1)
- `repository-r2dbc/.../R2dbcWorkerClaimExplainIntegrationTest.java` (new, Step 3.2)
- `repository-r2dbc/.../R2dbcWorkerClaimExplainMariaDbIntegrationTest.java` (new, Step 3.2)

**Add parameterized test scenarios:**

1. **Uniform distribution** — 10k rows, 100 keys, ~100 rows/key. All `READY`.
2. **Hot-key skew** — 10k rows, 1 key has 5k rows, remaining 99 keys split the rest.
3. **Mixed status** — 10k rows across all statuses (`READY`, `FAILED`, `PROCESSING`, `PROCESSED`, `STOPPED`), 100 keys.
4. **High-retry FAILED** — 10k rows, 2k in `FAILED` with `retry_count` near `maxRetries`.

For each scenario, validate `EXPLAIN` output shows index usage and no sequential scan.

### Step 3.4 — Document expected query plans

**Files to modify:**

- `docs/USAGE.md` — add "Claim Query Performance" section:
  - Expected `EXPLAIN` output for Postgres and MariaDB claim queries.
  - Required indexes (reference the Liquibase changelogs).
  - How to run the explain-plan tests locally as pre-release validation.
  - Warning signs of regression (sequential scan, missing index, high row estimate).

---

## Implementation Order

The priorities have limited inter-dependencies. Recommended order:

| Phase | Steps | Rationale |
|---|---|---|
| Phase A | 2.3, 2.4 | Logging level changes and reclaim-count returns are small, self-contained refactors. Do first to reduce diff noise in later phases. |
| Phase B | 1.1 → 1.3 | Repository API + dialect SQL + implementations. Foundation for redrive services. |
| Phase C | 1.4 → 1.5 | Observer API + implementations for redrive signal. |
| Phase D | 1.6 → 1.7 | Redrive service classes + auto-configuration. |
| Phase E | 2.1 → 2.2 | Batch summary observer signal + worker integration. Depends on observer pattern established in Phase C. |
| Phase F | 2.5 | Queue-health metrics. Independent of redrive but benefits from the observer pattern. |
| Phase G | 1.8 | Redrive integration tests. Run after services are wired. |
| Phase H | 3.1 → 3.3 | Explain-plan tests. Fully independent; can run in parallel with other phases. |
| Phase I | 1.9, 2.6, 3.4 | Documentation. Last, after all code is stable. |

---

## Files Summary

### New files

| File | Module | Purpose |
|---|---|---|
| `RedriveService.java` | `worker-blocking` | Blocking redrive workflow |
| `ReactiveRedriveService.java` | `worker-reactive` | Reactive redrive workflow |
| `QueueHealthService.java` | `worker-blocking` | Periodic queue-status gauge publisher |
| `ReactiveQueueHealthService.java` | `worker-reactive` | Reactive queue-status gauge publisher |
| `JdbcRedriveIntegrationTest.java` | `repository-jdbc` | Postgres redrive IT |
| `JdbcRedriveMariaDbIntegrationTest.java` | `repository-jdbc` | MariaDB redrive IT |
| `R2dbcRedriveIntegrationTest.java` | `repository-r2dbc` | Postgres redrive IT (reactive) |
| `R2dbcRedriveMariaDbIntegrationTest.java` | `repository-r2dbc` | MariaDB redrive IT (reactive) |
| `RedriveServiceTest.java` | `worker-blocking` | Redrive unit test |
| `ReactiveRedriveServiceTest.java` | `worker-reactive` | Redrive unit test |
| `JdbcWorkerClaimExplainMariaDbIntegrationTest.java` | `repository-jdbc` | MariaDB explain-plan IT |
| `R2dbcWorkerClaimExplainIntegrationTest.java` | `repository-r2dbc` | Postgres explain-plan IT (reactive) |
| `R2dbcWorkerClaimExplainMariaDbIntegrationTest.java` | `repository-r2dbc` | MariaDB explain-plan IT (reactive) |

### Modified files

| File | Module | Change |
|---|---|---|
| `WorkerMessageRepository.java` | `repository-api` | Add `findByStatus`, `redriveById`, `redriveByStatus`, `countByStatus` |
| `ReactiveWorkerMessageRepository.java` | `repository-api` | Reactive equivalents |
| `WorkerMaintenanceRepository.java` | `repository-api` | Change reclaim return type `void` → `int` |
| `ReactiveWorkerMaintenanceRepository.java` | `repository-api` | Change reclaim return type `Mono<Void>` → `Mono<Integer>` |
| `JdbcSqlDialect.java` | `repository-jdbc` | Add dialect methods for redrive + countByStatus SQL |
| `PostgresJdbcSqlDialect.java` | `repository-jdbc` | Implement new dialect methods |
| `MariaDbJdbcSqlDialect.java` | `repository-jdbc` | Implement new dialect methods |
| `JdbcWorkerMessageRepository.java` | `repository-jdbc` | Implement new interface methods |
| `JdbcWorkerMaintenanceRepository.java` | `repository-jdbc` | Return reclaim row count |
| `R2dbcSqlDialect.java` | `repository-r2dbc` | Add dialect methods for redrive + countByStatus SQL |
| `PostgresR2dbcSqlDialect.java` | `repository-r2dbc` | Implement new dialect methods |
| `MariaDbR2dbcSqlDialect.java` | `repository-r2dbc` | Implement new dialect methods |
| `R2dbcWorkerMessageRepository.java` | `repository-r2dbc` | Implement new interface methods |
| `R2dbcWorkerMaintenanceRepository.java` | `repository-r2dbc` | Return reclaim row count |
| `SuperduperObserver.java` | `observability-api` | Add `workerRedriven`, `workerBatchCompleted`, overloaded `maintenanceSucceeded` |
| `LoggingSuperduperObserver.java` | `observability-logging` | Implement new signals; change `workerProcessed`/`consumerSucceeded` to DEBUG |
| `MetricsSuperduperObserver.java` | `observability-metrics` | Add `redriven.total` counter, `reclaimed.total` counter, `queue.backlog` gauges |
| `SuperDuperWorkerService.java` | `worker-blocking` | Emit batch summary after `process()` |
| `SuperDuperWorkerReactiveService.java` | `worker-reactive` | Emit batch summary after reactive chain completes |
| `OrphanReclaimer.java` | `worker-blocking` | Pass reclaim count to observer |
| `ReactiveOrphanReclaimer.java` | `worker-reactive` | Pass reclaim count to observer |
| `AutoSelectConfiguration.java` | `starter-autoselect` | Wire `RedriveService`, `ReactiveRedriveService`, `QueueHealthService`, `ReactiveQueueHealthService` |
| `docs/USAGE.md` | docs | Redrive section, operational monitoring, claim performance |
| `README.md` | root | Mention redrive capability |

---

## Validation Checklist

- [ ] `mvn -q -DskipTests test-compile` — all modules compile.
- [ ] `mvn -T 1C -q test` — all existing + new tests pass.
- [ ] Redrive integration tests pass on Postgres and MariaDB (JDBC + R2DBC).
- [ ] Explain-plan tests pass on Postgres and MariaDB (JDBC + R2DBC).
- [ ] `mvn -q spotless:apply` — formatting clean.
- [ ] No new public API without Javadoc.
