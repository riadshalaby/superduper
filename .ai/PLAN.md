# Plan — ROADMAP v2

> Branch: `feature/next-version` · Date: 2025-03-04

---

## Overview

This plan addresses all items from `ROADMAP.md`. Items are grouped into
logical work packages ordered by dependency and risk.

---

## WP-1 — Rename `RETRY` to `FAILURE` and introduce `FAILED` DB status

**ROADMAP refs:**
- _Processing failure should be FAILURE not RETRY_
- _`ProcessingResult` has no FAILURE variant; schema's FAILED status is never written_

**Design decision:** `ProcessingResult` keeps exactly two values: `SUCCESS`
and `FAILURE` (renamed from `RETRY`). There is no third state.
An explicit `FAILURE` return and an unhandled exception are treated
identically: the message is marked `FAILED` with an incremented retry count.
When `retry_count >= maxRetries` the message transitions to `STOPPED`.

**Current behaviour:** Enum has `SUCCESS` and `RETRY`. On exception the
result defaults to `RETRY`. The DB status `FAILED` (allowed by the schema
CHECK constraint) is never written; instead, the worker writes `READY` for
retry and `STOPPED` at max retries.

**Changes:**

1. **`ProcessingResult` enum (both `worker-blocking` and `worker-reactive`)**
   - Rename `RETRY` → `FAILURE`.

2. **Repository API (`repository-api`)**
   - Add `markFailed(long id, int retryCount)` to `WorkerMessageRepository`.
   - Add `markFailed(long id, int retryCount)` to
     `ReactiveWorkerMessageRepository` (returns `Mono<Void>`).
   - Remove `markReadyForRetry` from both interfaces.

3. **SQL dialects (JDBC + R2DBC, Postgres + MariaDB)**
   - Add `markFailedSql`:
     ```sql
     UPDATE <table> SET status='FAILED', retry_count=:r,
       container_id=NULL, last_updated=NOW() WHERE id=:id
     ```
   - Remove `markReadyForRetrySql`.

4. **Worker services — new result flow (both blocking and reactive):**
   - On handler exception → result = `FAILURE`.
   - On explicit `FAILURE` return from handler → same path.
   - On `FAILURE`:
     - `retry_count++`
     - If `retry_count < maxRetries` → `markFailed(id, retry)` (status `FAILED`).
     - Else → `markStopped(id, retry)`.
   - On `SUCCESS` → `markProcessed(id)` (unchanged).
   - Claim SQL candidate filter already includes
     `(m1.status = 'FAILED' AND m1.retry_count < :maxRetries)` alongside
     `m1.status = 'READY'`, so failed rows are automatically re-claimed.

5. **Observability** — emit `workerFailed` event on `FAILURE` (already called
   on exception path; ensure it also fires on explicit `FAILURE` return).

6. **Tests** — unit + integration tests for the new flow in both blocking and
   reactive workers. Update any existing tests that reference `RETRY`.

---

## WP-2 — Add ownership check to status-update SQL

**ROADMAP ref:** _Ownership check in status update SQL is missing_

**Current behaviour:** `markProcessed`, `markReadyForRetry`, `markStopped`
filter only on `WHERE id=:id` — no `container_id` guard. A slow original
worker can clobber a row already reclaimed and re-assigned to another worker.

**Changes:**

1. **All four SQL dialects** (JDBC Postgres, JDBC MariaDB, R2DBC Postgres,
   R2DBC MariaDB):
   - `markProcessedSql` → add `AND container_id=:cid`
   - `markStoppedSql` → add `AND container_id=:cid`
   - `markFailedSql` (new from WP-1) → add `AND container_id=:cid`

2. **Repository API method signatures** — add `containerId` parameter where
   missing.

3. **Worker services** — pass `workerId` to every status-update call.

4. **Return value** — change `void` / `Mono<Void>` to `boolean` /
   `Mono<Boolean>` (or row-count) so callers can detect a no-op (ownership
   lost). Log a warning when the update affects 0 rows.

5. **Tests** — add test proving a stale worker's update is rejected after
   reclaim.

---

## WP-3 — Fix heartbeat window ≠ heartbeat interval

**ROADMAP ref:** _Heartbeat window equals heartbeat interval → false orphan
detection_

**Current behaviour:** `OrphanReclaimer` and `ReactiveOrphanReclaimer`
receive `heartbeatWindowSec = heartbeatIntervalMs / 1000` (default 30 s).
Any GC pause or network blip causes a false positive.

**Changes:**

1. **`WorkerProperties`** — add new property:
   ```
   superduper.worker.heartbeat-window-ms  (default: 90000)   # 3× interval
   ```

2. **`OrphanReclaimer` + `ReactiveOrphanReclaimer`** — inject the new
   `heartbeatWindowMs` instead of reusing `heartbeatIntervalMs`.

3. **Tests** — verify that with default values, a heartbeat 31 s old is NOT
   reclaimed (window = 90 s).

---

## WP-4 — Refresh `last_updated` on orphan reclaim

**ROADMAP ref:** _When messages are reclaimed PROCESSING → READY, last_updated
is not refreshed_

**Current behaviour:** `reclaimStaleProcessingSql` and
`reclaimMissingHeartbeatsSql` do `SET status='READY', container_id=NULL` but
omit `last_updated=NOW()`.

**Changes:**

1. **All four SQL dialects** — add `last_updated=NOW()` to the SET clause of:
   - `reclaimStaleProcessingSql`
   - `reclaimMissingHeartbeatsSql`

2. **Tests** — assert `last_updated` is refreshed after reclaim.

---

## WP-5 — Fix reactive consumer swallowing DB write errors

**ROADMAP ref:** _Reactive consumer silently swallows DB write errors_

**Current behaviour:** `KafkaReactiveR2dbcConsumerService` uses
`.onErrorResume(e -> Mono.empty())` + `.subscribe()` (fire-and-forget).
DB failures are logged but Spring Kafka's error handler is never engaged.

**Changes:**

1. Replace `.subscribe()` with `.block()` so the `@KafkaListener` thread
   waits for the DB write.
2. On DB error, **rethrow** the exception (matching the blocking consumer
   pattern) so Spring Kafka can engage its error handler and the message is
   redelivered.
3. Remove `.onErrorResume(… Mono.empty())` — let the error propagate after
   calling `observer.consumerFailed()`.

**Resulting pattern:**
```java
messageIngestRepository.upsertReadyMessage(...)
    .doOnSuccess(unused -> {
        ack.acknowledge();
        observer.consumerSucceeded(...);
    })
    .doOnError(e -> observer.consumerFailed(..., e))
    .block();
// exception propagates to Spring Kafka error handler
```

4. **Tests** — verify that a DB failure causes the listener to throw (and
   therefore not acknowledge the offset).

---

## WP-6 — Switch heartbeat from `fixedRate` to `fixedDelay`

**ROADMAP ref:** _Heartbeat uses fixedRate instead of fixedDelay_

**Current behaviour:** Both `HeartbeatService` and `ReactiveHeartbeatService`
use `@Scheduled(fixedRateString = …)`. Under slow DB writes, scheduled
invocations can pile up.

**Changes:**

1. Change `fixedRateString` → `fixedDelayString` in both heartbeat services.
2. **Tests** — existing heartbeat tests remain valid; no behavioural change
   other than scheduling semantics.

---

## WP-7 — Reactive worker: use virtual-thread Scheduler

**ROADMAP ref:** _`SuperDuperWorkerReactiveService.schedule()` should use a
Scheduler with virtual threads_

**Current behaviour:** `schedule()` calls `.block()` on the Spring
`@Scheduled` thread pool thread (platform thread), blocking it for the
entire claim+process cycle.

**Changes:**

1. Create a virtual-thread-backed `ExecutorService` (Java 21+):
   ```java
   Executors.newVirtualThreadPerTaskExecutor()
   ```
2. Wrap it as a Reactor `Scheduler`:
   ```java
   Schedulers.fromExecutorService(vtExecutor)
   ```
3. In `schedule()`, use `.subscribeOn(vtScheduler)` before `.block()` so the
   reactive pipeline runs on a virtual thread, freeing the platform
   scheduling thread.

   **Preferred approach:** `.subscribeOn(vtScheduler).block()` — minimal
   change, keeps the sequential guarantee from `fixedDelay`.

4. **Tests** — verify schedule completes on a virtual thread
   (`Thread.currentThread().isVirtual()`).

---

## WP-8 — Replace `claimedCount` array with `AtomicLong`

**ROADMAP ref:** _Why does the WorkerServices use a claimedCount Array?_

**Current behaviour:** `final long[] claimedCount = new long[]{0L}` is a
workaround for the effectively-final lambda constraint.

**Changes:**

1. Replace with `AtomicLong` in both `SuperDuperWorkerService` and
   `SuperDuperWorkerReactiveService`.
2. Use `.set()` / `.get()` instead of array index access.

---

## WP-9 — Document at-least-once semantics

**ROADMAP ref:** _At-least-once semantics undocumented_

**Changes:**

1. Add a section to `README.md` under "## Delivery & Processing Guarantees"
   explaining:
   - Kafka consumer commits after DB persist → at-least-once ingest.
   - `uuid` deduplication (deterministic from `topic:partition:offset`) →
     upsert prevents duplicate rows on redelivery.
   - Worker claim + ownership check → at-most-once *processing* per claim
     cycle, but orphan reclaim can cause reprocessing → at-least-once
     processing overall.
   - User handlers should be idempotent.

---

## Dependency Order

```
WP-1  (FAILED status + rename RETRY→FAILURE)
  └─► WP-2  (ownership check — applies to markFailed too)

WP-3  (heartbeat window)      — independent
WP-4  (last_updated reclaim)  — independent
WP-5  (reactive consumer)     — independent
WP-6  (fixedRate → fixedDelay)— independent
WP-7  (virtual-thread sched)  — independent
WP-8  (AtomicLong)            — independent
WP-9  (docs)                  — after all code changes
```

## Suggested Implementation Order

| Step | WP | Risk | Effort |
|------|----|------|--------|
| 1 | WP-1 | High — changes processing semantics | Medium |
| 2 | WP-2 | High — safety-critical SQL change | Medium |
| 3 | WP-5 | High — data-loss risk in reactive consumer | Small |
| 4 | WP-3 | Medium — false orphan detection | Small |
| 5 | WP-4 | Medium — stale timestamps | Small |
| 6 | WP-6 | Low — scheduling fix | Trivial |
| 7 | WP-7 | Low — performance improvement | Small |
| 8 | WP-8 | Low — code hygiene | Trivial |
| 9 | WP-9 | Low — documentation | Small |

---

## Out of Scope

- Unifying the two `ProcessingResult` enums into a shared module (can be done
  later; both modules currently define it identically).
- `ReactiveOrphanReclaimer` overlap fix — already resolved in commit `ccc25c5`;
  test `ReactiveMaintenanceSchedulingTest.java` needs to be committed.

---

## Validation

After all WPs are implemented:
```bash
mvn -q spotless:apply
mvn -T 1C -q test
```
