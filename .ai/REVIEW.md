# REVIEW — T-001 & T-002 (v0.6.0)

**Reviewer:** claude
**Date (UTC):** 2026-03-15
**Commit reviewed:** `9baf12b feat(consumer): batch kafka ingest upserts`

Review Round: **1**

---

## Verdict
`PASS`

---

## T-001: Per-Topic Claim Locks — Verification

### Verdict: PASS

### Findings

| Severity | Finding |
|---|---|
| INFO | `topicWorkerInstance_usesTopicScopedLockName` in `TopicWorkerCoordinatorTest` proves per-topic lock independence at unit level — acceptance criterion satisfied. |
| INFO | No new code changes were made (verification-only as planned). |

### Acceptance Criteria

| Criterion | Status |
|---|---|
| `mvn -T 1C -q test` passes | ✅ Confirmed in HANDOFF.md |
| At least one test proves per-topic lock independence | ✅ `topicWorkerInstance_usesTopicScopedLockName` asserts lock name `superduper-claim-orders` per topic |

---

## T-002: Batch Inserts on Ingest

### Verdict: PASS

### Findings

| Severity | Finding |
|---|---|
| INFO | Commit hash in HANDOFF.md (`de5b3db5c...`) does not match git log short hash (`9baf12b`). Non-blocking — same commit message confirms correct commit. |
| INFO | `KafkaReactiveR2dbcConsumerService.onMessage()` uses `.block()` on the reactive chain. Expected — Spring Kafka listener threads are not reactor scheduler threads; blocking is the standard approach here. |
| INFO | `R2dbcMessageIngestRepositoryTest` instantiates a real `PostgresqlConnectionFactory` (not mocked) solely to resolve bind markers. Acceptable; no live connection is made. |

### Phase-by-phase verification

#### Phase 1 — Repository API + DTO
- `MessageIngestData` record created with all 7 fields. ✅
- `MessageIngestRepository.batchUpsertReadyMessages()` default iterates single-record upsert; null-safe. ✅
- `ReactiveMessageIngestRepository.batchUpsertReadyMessages()` default uses `Flux.fromIterable().concatMap()`; null-safe. ✅

#### Phase 2 — JDBC Implementation
- `JdbcMessageIngestRepository.batchUpsertReadyMessages()` uses `NamedParameterJdbcTemplate.batchUpdate()`. ✅
- Empty list guard returns early. ✅
- Reuses `dialect.upsertReadyMessageSql()` — no new SQL dialect method. ✅
- `JdbcMessageIngestRepositoryTest.batchUpsertReadyMessages_buildsBatchParameterSources()` verifies parameter source construction including null values. ✅

#### Phase 3 — R2DBC Implementation
- `R2dbcMessageIngestRepository.batchUpsertReadyMessages()` uses `db.inConnectionMany()` + `statement.add()`. ✅
- `BatchStatementTemplate` pre-resolves bind markers once at construction time. ✅
- Empty list guard returns `Mono.empty()`. ✅
- `R2dbcMessageIngestRepositoryTest.batchUpsertReadyMessages_usesDriverBatchStatement()` verifies positional params, `statement.add()` called, `execute()` invoked. ✅

#### Phase 4 — Blocking Consumer
- `KafkaConsumerAutoConfiguration`: `MAX_POLL_RECORDS_CONFIG` wired from `superduper.consumer.max-poll-records` (default 500). ✅
- `setBatchListener(true)`, `AckMode.MANUAL`. ✅
- `KafkaConsumerService.onMessage()` groups by kafka topic, batch upserts, falls back to `persistIndividually()` on failure. ✅
- `ack.acknowledge()` only after all groups persist successfully. ✅
- `consumerFailed` emitted and exception re-thrown when individual fallback fails → no ack. ✅
- Auto-config test verifies `isBatchListener()`, `AckMode.MANUAL`, `MAX_POLL_RECORDS_CONFIG`. ✅
- 6 unit tests: happy path, null keys, fallback, fallback failure/no-ack, multi-topic routing, empty batch. ✅

#### Phase 5 — Reactive Consumer
- Mirror of blocking; `setBatchListener(true)`, `AckMode.MANUAL`, `MAX_POLL_RECORDS_CONFIG`. ✅
- `onErrorResume()` → `persistIndividually()`, `.block()` per group before ack. ✅
- Same 6 unit tests as blocking. ✅

#### Phase 6 — Integration Tests + Examples
- `KafkaConsumerE2ETest`: 5 records (1 dup) → 4 rows; `batchCalls > 0`; `maxBatchSize >= 2`; `singleCalls == 0`. ✅
- `KafkaReactiveE2ETest`: same assertions. ✅
- All 4 example `application.yml` files include `max-poll-records: 500`. ✅

### Acceptance Criteria

| Criterion | Status |
|---|---|
| `mvn -T 1C -q test` passes | ✅ |
| Blocking consumer persists via batch upsert (unit + E2E) | ✅ |
| Reactive consumer persists via batch upsert (unit + E2E) | ✅ |
| DB failure triggers single-record fallback (unit test) | ✅ |
| Deduplication works with batch upserts (E2E) | ✅ |
| `superduper.consumer.max-poll-records` configurable and wired | ✅ |
| Kafka offsets not acknowledged before batch persist | ✅ |

---

## Architecture Compliance

| Rule | Status |
|---|---|
| No direct SQL in service classes; repository ports used | ✅ |
| Workers and consumers use repository ports | ✅ |
| English for all comments/log messages | ✅ |
| `plan` and `review` roles never commit | ✅ |
| Conventional Commit format on implementation commit | ✅ |

---

## Required Fixes

None. Both tasks pass all acceptance criteria.

## Open Questions
- None.
