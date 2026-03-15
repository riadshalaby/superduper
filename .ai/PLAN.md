# Plan — 0.6.0

Status: **approved**

Goal: push the transactional inbox algorithm closer to its theoretical limits — reduce latency, increase claim parallelism, and extend the scale ceiling.

---

## T-001: Per-Topic Claim Locks — Verification

### Current State

Already fully implemented and tested. Investigation confirms:

- `AutoSelectConfiguration.topicRegistry()` generates `"superduper-claim-" + topicName` per topic (line 145).
- `TopicRegistry.ResolvedTopicConfig` carries `claimLockName` through `TopicConfigView`.
- `TopicWorkerInstance` and `ReactiveTopicWorkerInstance` pass the per-topic lock name to the worker service constructors.
- `SuperDuperWorkerService` and `SuperDuperWorkerReactiveService` use the per-topic lock name in `LockConfiguration` when calling `lockExec.executeWithLock()`.
- Unit test `topicWorkerInstance_usesTopicScopedLockName` in `TopicWorkerCoordinatorTest` explicitly asserts the per-topic lock name.

### Scope

Verification-only. No code changes expected.

1. Run existing test suite and confirm all per-topic lock tests pass.
2. If no multi-topic **integration test** exists that proves two topics claim concurrently (not just unit-level), add one to `worker-blocking` or `consumer-kafka-blocking` E2E tests.
3. Close the task.

### Acceptance Criteria

- `mvn -T 1C -q test` passes.
- At least one test proves per-topic lock independence (unit or integration level).

---

## T-002: Batch Inserts on Ingest

### Current State

Both consumers (`KafkaConsumerService`, `KafkaReactiveR2dbcConsumerService`) use single-record `@KafkaListener`:

- Receive one `ConsumerRecord<String, String>` per invocation.
- Call `upsertReadyMessage()` (single INSERT/UPSERT) per record.
- Manual-immediate ack per record (`AckMode.MANUAL_IMMEDIATE`).
- No batch methods exist on `MessageIngestRepository` or `ReactiveMessageIngestRepository`.

### Approach

Use Spring Kafka's native batch listener mode. Kafka delivers one poll's worth of records as a `List<ConsumerRecord>`. The consumer groups records by topic, calls a new batch upsert on the correct repository, and acks the entire batch after persist succeeds.

### Error Handling Strategy

1. Try batch upsert (all records for a given topic in one DB call).
2. On DB failure: fall back to single-record upserts to isolate the failing record.
3. If any single-record upsert still fails: throw exception → no ack → Kafka re-delivers the entire poll batch.
4. Upsert is idempotent (`ON CONFLICT`/`ON DUPLICATE KEY`), so re-processing is safe.

### Configuration

| Property | Default | Description |
|---|---|---|
| `superduper.consumer.max-poll-records` | `500` | Maps to Kafka `max.poll.records`. Controls batch size. |

### Implementation Phases

#### Phase 1 — Repository API + DTO

**Files to create:**

| File | Module | Description |
|---|---|---|
| `MessageIngestData.java` | `repository-api` | Record: `topic`, `messageId`, `messageKey`, `content`, `occurredAt`, `correlationId`, `messageType` |

**Files to modify:**

| File | Module | Change |
|---|---|---|
| `MessageIngestRepository.java` | `repository-api` | Add `default void batchUpsertReadyMessages(List<MessageIngestData> messages)` — default iterates and calls single-record upsert |
| `ReactiveMessageIngestRepository.java` | `repository-api` | Add `default Mono<Void> batchUpsertReadyMessages(List<MessageIngestData> messages)` — default iterates with `Flux.fromIterable().concatMap()` |

#### Phase 2 — JDBC Implementation

**Files to modify:**

| File | Module | Change |
|---|---|---|
| `JdbcMessageIngestRepository.java` | `repository-jdbc` | Override `batchUpsertReadyMessages()` using `NamedParameterJdbcTemplate.batchUpdate(dialect.upsertReadyMessageSql(), SqlParameterSource[])` |

No new SQL dialect method needed — `batchUpdate()` reuses the existing `upsertReadyMessageSql()` with an array of parameter sources.

**Tests to add/update:**

| File | Change |
|---|---|
| `JdbcMessageIngestRepositoryTest.java` | Add test for `batchUpsertReadyMessages()` verifying batch parameter source construction |

#### Phase 3 — R2DBC Implementation

**Files to modify:**

| File | Module | Change |
|---|---|---|
| `R2dbcMessageIngestRepository.java` | `repository-r2dbc` | Override `batchUpsertReadyMessages()` using `DatabaseClient.inConnectionMany()` → `Connection.createStatement()` + `Statement.add()` for true DB-level batching |

**Tests to add/update:**

| File | Change |
|---|---|
| `R2dbcMessageIngestRepositoryTest.java` | Add test for `batchUpsertReadyMessages()` |

#### Phase 4 — Blocking Consumer Batch Listener

**Files to modify:**

| File | Module | Change |
|---|---|---|
| `KafkaConsumerAutoConfiguration.java` | `consumer-kafka-blocking` | 1. Add `MAX_POLL_RECORDS_CONFIG` from new property. 2. Set `f.setBatchListener(true)`. 3. Change `AckMode` to `MANUAL` (batch-level ack). |
| `KafkaConsumerService.java` | `consumer-kafka-blocking` | 1. Change `onMessage` signature to `List<ConsumerRecord<String, String>>` + `Acknowledgment`. 2. Group records by kafka topic. 3. Resolve metadata per record → build `MessageIngestData` list per topic. 4. Call `batchUpsertReadyMessages()` per repository. 5. On DB failure: fall back to single-record loop. 6. Ack after all records persisted. 7. Emit per-record observability signals (received/succeeded/failed). |

**Property wiring:**

| File | Module | Change |
|---|---|---|
| `KafkaConsumerAutoConfiguration.java` | `consumer-kafka-blocking` | Inject `@Value("${superduper.consumer.max-poll-records:500}")` and set on consumer factory props |

**Tests to add/update:**

| File | Change |
|---|---|
| `KafkaConsumerServiceTest.java` | Rewrite/add tests for: batch happy path, batch fallback to single-record on DB error, multi-topic batch routing, empty batch, null keys in batch |

#### Phase 5 — Reactive Consumer Batch Listener

**Files to modify:**

| File | Module | Change |
|---|---|---|
| `KafkaReactiveR2dbcAutoConfiguration.java` | `consumer-kafka-reactive` | Same changes as blocking auto-config: `MAX_POLL_RECORDS_CONFIG`, `setBatchListener(true)`, `AckMode.MANUAL` |
| `KafkaReactiveR2dbcConsumerService.java` | `consumer-kafka-reactive` | Same pattern as blocking: batch signature, group by topic, batch upsert, fallback, ack, observability |

**Tests to add/update:**

| File | Change |
|---|---|
| `KafkaReactiveR2dbcConsumerServiceTest.java` | Same test coverage as blocking counterpart |

#### Phase 6 — Integration Tests + Examples

**Tests to update:**

| File | Change |
|---|---|
| `KafkaConsumerE2ETest.java` | Verify batch ingest writes N records in one poll cycle, verify deduplication works with batch |
| `KafkaReactiveE2ETest.java` | Same coverage for reactive path |

**Examples to update:**

| File | Change |
|---|---|
| `examples/app-blocking/src/main/resources/application.yml` | Add `superduper.consumer.max-poll-records: 500` |
| `examples/app-reactive/src/main/resources/application.yml` | Add `superduper.consumer.max-poll-records: 500` |
| Multi-topic example configs | Add `superduper.consumer.max-poll-records: 500` |

### Key Design Decisions

1. **No new SQL dialect method.** `NamedParameterJdbcTemplate.batchUpdate()` reuses the existing single-row upsert SQL with an array of parameter sources. R2DBC uses `Statement.add()` with the same SQL.
2. **Default method on interface.** The batch method has a default that iterates over single upserts. This keeps `TopicRepositoryFactory` and custom implementations backward-compatible. JDBC and R2DBC implementations override for performance.
3. **Batch ack, not per-record.** `AckMode.MANUAL` acks after the full batch persists. If the batch fails and fallback also fails, no ack is sent → Kafka re-delivers. Idempotent upserts make this safe.
4. **`max.poll.records` as the batch size knob.** No custom buffer — Kafka's native poll batching controls how many records arrive per listener invocation.

### Acceptance Criteria

- `mvn -T 1C -q test` passes.
- Blocking consumer persists records via batch upsert (verified by unit + E2E tests).
- Reactive consumer persists records via batch upsert (verified by unit + E2E tests).
- DB failure triggers single-record fallback (verified by unit test).
- `message_id` deduplication works correctly with batch upserts (verified by E2E test).
- `superduper.consumer.max-poll-records` is configurable and wired to Kafka consumer.
- Kafka offsets are not acknowledged before the batch is persisted.

## Validation

- `mvn -q -DskipTests test-compile`
- `mvn -T 1C -q test`
