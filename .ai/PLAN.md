# Plan — v0.4.0

Status: **active**

## Overview

Three priorities from `ROADMAP.md`, implemented in two tracks:

- **Track A** (Priority 2 + 3): Schema rename + new columns + Consumer Metadata SPI
- **Track B** (Priority 1): Worker batch throughput improvements

Track A is done first because it stabilises the schema and API surface before the batch-throughput rework.

Column renames are done as a clean break (no deprecation/dual-write) since the library is not yet in production use.

---

## Track A — Schema, Naming, and Consumer Metadata SPI

### Step 1: Liquibase schema migration

Add new changeset files for Postgres and MariaDB:

- `004-rename-and-add-columns-postgres.sql`
- `005-rename-and-add-columns-mariadb.sql`

Changes:

| Old column | New column | Notes |
|---|---|---|
| `uuid` | `message_id` | `RENAME COLUMN`, keep `UNIQUE NOT NULL` |
| `key` | `message_key` | `RENAME COLUMN` |
| — | `correlation_id` | `ADD COLUMN VARCHAR(36) NULL` |
| — | `message_type` | `ADD COLUMN VARCHAR(255) NULL` |

Update `db.changelog-master.yaml` to include the new changesets.

Update index `idx_messages_key_id` to reference `message_key` (drop + recreate or rename).

### Step 2: Update SQL dialects (all four)

Update every SQL string in these classes to use the new column names:

- `repository-jdbc/PostgresJdbcSqlDialect` — all methods (`upsertReadyMessageSql`, `claimBatchSql`, `fetchClaimedForWorkerSql`, etc.)
- `repository-jdbc/MariaDbJdbcSqlDialect` — same
- `repository-r2dbc/PostgresR2dbcSqlDialect` — same
- `repository-r2dbc/MariaDbR2dbcSqlDialect` — same

Specifically:

- `uuid` references become `message_id`.
- `key` references become `message_key` (in `ON` clauses, `WHERE` clauses, `SELECT`, `INSERT`, `ORDER BY`).
- `upsertReadyMessageSql()` adds `correlation_id` and `message_type` as new bind params.
- `fetchClaimedForWorkerSql()` adds `correlation_id` and `message_type` to the `SELECT` list.

### Step 3: Update repository API interfaces

**`MessageIngestRepository`**:

```java
void upsertReadyMessage(String messageId, String messageKey, String content,
                        Instant occurredAt, String correlationId, String messageType);
```

**`ReactiveMessageIngestRepository`**:

```java
Mono<Void> upsertReadyMessage(String messageId, String messageKey, String content,
                               Instant occurredAt, String correlationId, String messageType);
```

**`ClaimedMessage`** record — rename fields + add new ones:

```java
public record ClaimedMessage(Long id, String messageKey, String content,
                              Integer retryCount, String containerId,
                              String correlationId, String messageType) {}
```

### Step 4: Update repository implementations

- `JdbcMessageIngestRepository.upsertReadyMessage()` — pass `correlationId` and `messageType` as bind params.
- `R2dbcMessageIngestRepository.upsertReadyMessage()` — same.
- `JdbcWorkerMessageRepository.fetchClaimedForWorker()` — map `correlation_id` and `message_type` from result set into `ClaimedMessage`.
- `R2dbcWorkerMessageRepository.fetchClaimedForWorker()` — same.

### Step 5: Consumer Metadata SPI

**New module or package**: add the SPI in the existing `repository-api` module (it has no heavy dependencies) or as a small new API. Recommend keeping it in `repository-api` to avoid a new module for a single interface.

**New interface** `ConsumerMetadataResolver`:

```java
package net.rsworld.superduper.repository.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerMetadataResolver {
    String resolveMessageId(ConsumerRecord<String, String> record);
    Instant resolveOccurredAt(ConsumerRecord<String, String> record);
    String resolveCorrelationId(ConsumerRecord<String, String> record);
    String resolveMessageType(ConsumerRecord<String, String> record);
}
```

> Note: `repository-api` currently has no Kafka dependency. The SPI should live in a place that can reference `ConsumerRecord`. Two options:
> - Add it to the consumer modules themselves (e.g. a shared `consumer-api` or directly in each consumer module).
> - Use a generic `Map<String, byte[]> headers` parameter instead of `ConsumerRecord`.
>
> **Decision**: Use a generic parameter approach — the SPI accepts a `java.util.Map<String, byte[]>` of headers plus topic/partition/offset so it stays in `repository-api` without a Kafka dependency. The consumer modules adapt `ConsumerRecord` headers into this map before calling the SPI.

Revised SPI interface in `repository-api`:

```java
package net.rsworld.superduper.repository.api;

import java.time.Instant;

public interface ConsumerMetadataResolver {
    String resolveMessageId(String topic, int partition, long offset, java.util.Map<String, byte[]> headers);
    Instant resolveOccurredAt(long recordTimestamp, java.util.Map<String, byte[]> headers);
    String resolveCorrelationId(java.util.Map<String, byte[]> headers);
    String resolveMessageType(java.util.Map<String, byte[]> headers);
}
```

**Default implementation** `DefaultConsumerMetadataResolver` (same package):

| Method | Behaviour |
|---|---|
| `resolveMessageId` | header `message_id` if present; otherwise deterministic UUID from `topic:partition:offset` |
| `resolveOccurredAt` | header `occurred_at` if present and parseable; otherwise `Instant.ofEpochMilli(recordTimestamp)`; otherwise `Instant.now()` |
| `resolveCorrelationId` | header `correlationId` if present; otherwise `UUID.randomUUID().toString()` |
| `resolveMessageType` | header `message_type` if present; otherwise `null` |

### Step 6: Wire SPI into consumers

- **`consumer-kafka-blocking/KafkaConsumerService`**: inject `ConsumerMetadataResolver` (default if none provided). Replace inline `resolveOccurredAt` and UUID generation with SPI calls. Pass `correlationId` and `messageType` to `upsertReadyMessage()`.
- **`consumer-kafka-reactive/KafkaReactiveR2dbcConsumerService`**: same pattern.
- **`starter-autoselect/AutoSelectConfiguration`**: register `DefaultConsumerMetadataResolver` as a `@Bean` with `@ConditionalOnMissingBean`.

### Step 7: Update worker `MessageRow` records

- `worker-blocking/MessageRow` — rename `uuid` to `messageId`, `key` to `messageKey`, add `correlationId` and `messageType`.
- `worker-reactive/MessageRow` — same.
- Update all usages in `SuperDuperWorkerService` and `SuperDuperWorkerReactiveService` that reference these fields.

### Step 8: Update tests (Track A)

- All unit tests referencing old column/field names.
- All integration tests (Testcontainers) — schema will auto-migrate via new Liquibase changesets.
- Add unit tests for `DefaultConsumerMetadataResolver`.
- Add consumer tests verifying SPI is called and `correlationId`/`messageType` flow end-to-end.

### Step 9: Update documentation (Track A)

- `README.md` — update Data Model SQL, algorithm description (column names).
- `docs/USAGE.md` — add Consumer Metadata SPI section.
- `docs/ARCHITECTURE.md` — mention SPI.

---

## Track B — Worker Batch Throughput

### Step 10: Rework claim SQL to allow multiple same-key messages

Current claim SQL filters to only the oldest pending row per key. Change to:

- Remove the `NOT EXISTS (... prev.id < m1.id ...)` sub-select that limits to oldest-per-key.
- Keep the `LEFT JOIN ... PROCESSING` guard so no new messages for a key currently being processed are claimed.
- The claim now returns up to `batchSize` rows, potentially multiple rows per key, ordered by `id`.

Updated candidate logic (Postgres):

```sql
WITH candidate AS (
  SELECT m1.id
  FROM messages m1
  LEFT JOIN messages p
    ON p.message_key = m1.message_key AND p.status = 'PROCESSING'
  WHERE (m1.status = 'READY' OR (m1.status = 'FAILED' AND m1.retry_count < :maxRetries))
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

Same change for MariaDB dialect in all four dialect classes (JDBC + R2DBC x Postgres + MariaDB).

### Step 11: Worker-side per-key ordering within a batch

After claiming, the worker fetches rows ordered by `message_key, id`. The worker must enforce:

- Process messages per key in `id` order.
- If a message for a key fails, skip all subsequent messages for the same key in the current batch (release them back to `READY` or mark them `FAILED` depending on design rule).

Per the ROADMAP design rule: _"if one message for a key fails and later messages of the same key are already inside the current batch, handling of those later messages must be done inside worker batch logic, not through SQL-level key blocking."_

Implementation in `SuperDuperWorkerService.process()` and `SuperDuperWorkerReactiveService`:

- Group fetched rows by `messageKey`.
- Iterate each key group in `id` order.
- On first failure for a key, mark remaining rows of that key back to `READY` (reset `container_id=NULL`, `status='READY'`) so they are picked up in the next cycle.
- Add a new repository method: `releaseMessages(List<Long> ids, String containerId)` to batch-reset skipped rows.

### Step 12: Repository API additions for batch release

Add to `WorkerMessageRepository`:

```java
int releaseMessages(List<Long> ids, String containerId);
```

Add to `ReactiveWorkerMessageRepository`:

```java
Mono<Integer> releaseMessages(List<Long> ids, String containerId);
```

New SQL in all dialects:

```sql
UPDATE messages SET status='READY', container_id=NULL, last_updated=NOW()
WHERE id IN (:ids) AND container_id=:cid
```

### Step 13: Integration tests (Track B)

- **Same-key batch test**: insert N messages with same key, verify all N are claimed in one batch and processed in order.
- **Same-key partial failure test**: insert N same-key messages, fail message K, verify messages K+1..N are released back to `READY` and re-claimed in the next cycle.
- **Mixed-key batch test**: insert messages across multiple keys, verify per-key ordering is maintained while different keys are processed independently.
- **Performance/throughput test**: insert a hot-key backlog of 100+ messages, measure claim+process cycles vs. baseline (old one-per-key behaviour).

### Step 14: Update documentation (Track B)

- `README.md` — update Algorithm section to reflect new batch semantics and same-key handling.
- `docs/USAGE.md` — document new batch behaviour and any configuration changes.

---

## Final Validation

- `mvn -q spotless:apply`
- `mvn -q -DskipTests test-compile`
- `mvn -T 1C -q test`
- Manual review of example apps (`app-blocking`, `app-reactive`) to ensure they compile and run with the updated schema.

---

## Summary of new/changed files

| Module | File | Action |
|---|---|---|
| `schema-liquibase` | `004-rename-and-add-columns-postgres.sql` | new |
| `schema-liquibase` | `005-rename-and-add-columns-mariadb.sql` | new |
| `schema-liquibase` | `db.changelog-master.yaml` | edit |
| `repository-api` | `MessageIngestRepository.java` | edit |
| `repository-api` | `ReactiveMessageIngestRepository.java` | edit |
| `repository-api` | `ClaimedMessage.java` | edit |
| `repository-api` | `ConsumerMetadataResolver.java` | new |
| `repository-api` | `DefaultConsumerMetadataResolver.java` | new |
| `repository-api` | `WorkerMessageRepository.java` | edit |
| `repository-api` | `ReactiveWorkerMessageRepository.java` | edit |
| `repository-jdbc` | `PostgresJdbcSqlDialect.java` | edit |
| `repository-jdbc` | `MariaDbJdbcSqlDialect.java` | edit |
| `repository-jdbc` | `JdbcMessageIngestRepository.java` | edit |
| `repository-jdbc` | `JdbcWorkerMessageRepository.java` | edit |
| `repository-r2dbc` | `PostgresR2dbcSqlDialect.java` | edit |
| `repository-r2dbc` | `MariaDbR2dbcSqlDialect.java` | edit |
| `repository-r2dbc` | `R2dbcMessageIngestRepository.java` | edit |
| `repository-r2dbc` | `R2dbcWorkerMessageRepository.java` | edit |
| `consumer-kafka-blocking` | `KafkaConsumerService.java` | edit |
| `consumer-kafka-reactive` | `KafkaReactiveR2dbcConsumerService.java` | edit |
| `starter-autoselect` | `AutoSelectConfiguration.java` | edit |
| `worker-blocking` | `MessageRow.java` | edit |
| `worker-blocking` | `SuperDuperWorkerService.java` | edit |
| `worker-reactive` | `MessageRow.java` | edit |
| `worker-reactive` | `SuperDuperWorkerReactiveService.java` | edit |
| all test modules | various test files | edit |
| docs | `README.md`, `USAGE.md`, `ARCHITECTURE.md` | edit |
