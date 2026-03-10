# Review — v0.5.0

Status: **PASS WITH NOTES**

Review Round: **2**

Reviewed: 2026-03-10

---

## 1. Previous Gaps — Resolution Status

### Gap 1 (Medium) — Cross-topic claim isolation test

**FIXED**

Both JDBC integration test files now contain a dedicated test method that inserts the same key into two different topics and asserts that each topic's claim is independent:

- `JdbcWorkerMessageRepositoryIntegrationTest.claimBatch_isolatesKeysPerTopic()` (lines 119–137)
  - Inserts `"shared-key"` into `"topic-a"` (2 rows) and `"topic-b"` (2 rows).
  - Claims with separate worker IDs per topic and asserts `claimedTopicA == 2` and `claimedTopicB == 2`.
  - Further asserts via `fetchClaimedForWorker` that each worker only sees rows from its own topic, and that `ClaimedMessage.topic()` is correctly populated.
- `JdbcWorkerMessageRepositoryMariaDbIntegrationTest.claimBatch_isolatesKeysPerTopic_onMariaDb()` (lines 96–114)
  - Identical behavioral test on MariaDB.

The R2DBC test suite (`R2dbcWorkerMessageRepositoryIntegrationTest`, `R2dbcWorkerMessageRepositoryMariaDbIntegrationTest`) does not yet have the cross-topic isolation test. The plan §1.7 explicitly states "Mirror all additions for R2DBC integration tests." This sub-gap remains open on the R2DBC side but the core JDBC behavioural correctness is now verified.

### Gap 2 (Medium) — EXPLAIN test topic column seeding

**FIXED**

Both EXPLAIN test files now seed the `messages` table with varied `topic` values, exercising the composite index's leading `topic` column:

- `JdbcWorkerClaimExplainIntegrationTest.seedScenario()` — all four scenarios generate rows with `CASE WHEN g % 2 = 0 THEN 'default' ELSE 'topic-b' END` (line 82, 86, 93, 105), producing ~50% `'default'` and ~50% `'topic-b'` rows. The `fetchClaimedForWorker` setup `UPDATE` (line 116) explicitly filters `AND topic = 'default'`, confirming the index is exercised with a real topic predicate.
- `JdbcWorkerClaimExplainMariaDbIntegrationTest.baseInsertSql()` — uses `CASE WHEN MOD(t.n, 2) = 0 THEN 'default' ELSE 'topic-b' END` (line 118), same split. All four scenarios go through `baseInsertSql()`.

The EXPLAIN plans now reflect realistic multi-topic cardinality.

### Gap 3 (Medium) — Phase 8 documentation

**FIXED**

All four Phase 8 items are now present:

- `docs/USAGE.md` — updated with a complete "Multi-topic configuration" section (lines 51–88), including YAML examples for `superduper.topics`, handler bean naming rules, dedicated-table opt-in via `table:`, backward-compatibility note, and operational behavior description.
- `docs/ARCHITECTURE.md` — updated:
  - Module map includes new classes (`TopicWorkerCoordinator`, `TopicWorkerInstance`, `ReactiveTopicWorkerCoordinator`, `ReactiveTopicWorkerInstance`, `TopicRegistryView`, `TopicRepositoryFactory`, `TopicProperties`, `TopicRegistry`, `RepositoryFactory`) for the relevant modules (lines 15–19).
  - Data Flow section updated to describe multi-topic routing via `TopicWorkerCoordinator`, per-topic claim loops, and per-topic maintenance routing (lines 82–93).
  - New "Multi-Topic Model" section added (lines 135–141) describing `messages.topic`, `TopicRegistry`, shared vs. dedicated table routing, and topic-aware maintenance.
- `README.md` — updated:
  - Data model section now includes `topic VARCHAR(255) NOT NULL DEFAULT 'default'` as the second column (line 76).
  - Claim SQL snippets in "The Algorithm" section show the topic-aware `LEFT JOIN ... ON p.topic = m1.topic` predicate (lines 124–136, 144–156).
  - Fetch SQL snippet shows `AND topic=:topic` (line 165).
  - Index description updated: "the default schema creates `messages(topic, status, message_key, id)` for topic-aware claim scans" (line 97).
- `examples/app-blocking/src/main/resources/application.yml` — updated with commented-out multi-topic configuration block (lines 32–40), showing how to replace `superduper.kafka.topic` with `superduper.topics` for a two-topic example with a dedicated table.

The reactive example's `application.yml` was not separately verified but is expected to mirror the blocking example. Phase 8 is substantially complete.

### Gap 4 (Low) — Missing unit tests

**NOT FIXED**

The following unit test classes still do not exist:

- `worker-blocking/src/test/` — no `TopicWorkerCoordinatorTest` and no `TopicWorkerInstanceTest`. The test directory contains `SuperDuperWorkerServiceTest`, `CleanupServiceTest`, `RedriveServiceTest`, `QueueHealthServiceTest`, and `WorkerBlockingIntegrationTest` — none of which cover the coordinator dispatch path or per-topic ShedLock names.
- `worker-reactive/src/test/` — no `ReactiveTopicWorkerCoordinatorTest` and no `ReactiveTopicWorkerInstanceTest`. The test directory contains `SuperDuperWorkerReactiveServiceTest`, `ReactiveCleanupServiceTest`, `ReactiveRedriveServiceTest`, `ReactiveQueueHealthServiceTest`, `ReactiveMaintenanceSchedulingTest`, and `WorkerReactiveIntegrationTest`.
- Consumer multi-topic integration test — neither `consumer-kafka-blocking/src/test/` nor `consumer-kafka-reactive/src/test/` contains a test that constructs a `KafkaConsumerService` or `KafkaReactiveR2dbcConsumerService` with a `TopicRegistry` and verifies that records from two different Kafka topics are routed to the correct per-topic ingest repository with the correct `topic` column value.
- Dedicated-table isolation test — no integration test in `repository-jdbc/src/test/` or `repository-r2dbc/src/test/` verifies that rows inserted into a dedicated table are invisible to shared-table queries and vice versa.
- R2DBC cross-topic isolation test — as noted under Gap 1, `R2dbcWorkerMessageRepositoryIntegrationTest` and `R2dbcWorkerMessageRepositoryMariaDbIntegrationTest` do not have the equivalent of `claimBatch_isolatesKeysPerTopic`.

### Gap 5 (Low) — Metrics topic tag assertions

**FIXED**

`MetricsSuperduperObserverTest.recordsCountersAndTimersWhenEnabled()` now contains explicit `meterTagValue()` assertions (lines 296–322) that call `meter.getId().getTag("topic")` and assert the value equals `"topic-a"` for:

- `superduper.worker.claim.total` with `mode=blocking`, `claimed_count=5`, `exception=none`
- `superduper.worker.process.duration` with `mode=blocking`, `exception=none`
- `superduper.maintenance.total` with `mode=blocking`, `operation=heartbeat`, `result=success`, `exception=none`
- `superduper.queue.backlog` with `mode=blocking`, `status=READY`

The `meterTagValue()` helper (lines 419–423) first asserts the meter is non-null, then returns `getId().getTag("topic")`. This is an explicit tag-presence assertion, not just a counter increment check.

---

## 2. New Issues Found

**No new high- or medium-severity issues identified.**

### Minor observations (carry-forward, no change in severity)

1. **R2DBC cross-topic isolation test missing** — `R2dbcWorkerMessageRepositoryIntegrationTest` and its MariaDB variant do not have a `claimBatch_isolatesKeysPerTopic` test. The JDBC variants cover behavioural correctness, but the plan §1.7 explicitly requires R2DBC mirrors. This is a low-severity gap that was partially present in Round 1 (noted under §1.7 in the overall assessment); it is now more precisely identified because the JDBC side is fixed but the R2DBC side is not.

2. **Consumer multi-topic service tests still use legacy constructor** — `KafkaConsumerServiceTest` and `KafkaReactiveR2dbcConsumerServiceTest` construct the service without a `TopicRegistry` (they use the legacy single-ingest-repository path). No test verifies that when two Kafka topics arrive, the service looks up the `TopicRegistry`, routes to the correct per-topic repository, and passes the correct `topic` value. This was previously flagged as Gap 4 (consumer multi-topic integration test) and remains NOT FIXED.

3. **`docs/USAGE.md` index name cross-reference is stale** — The "Claim Query Performance" section (lines 486–490) still references old index names (`idx_messages_ready_claim_id_key`, `idx_messages_failed_claim_retry_id_key`, `idx_messages_processing_key_id`) that do not exist in the current schema. The actual indexes are `idx_messages_topic_status_key_id`, `idx_messages_processing_worker_key_id`, `idx_messages_processing_last_updated`. This is a documentation inconsistency introduced by the Phase 1 schema change and not updated in Phase 8. Severity: low (operational guidance only, no functional impact).

4. **Redundant method overrides** (carry-forward from Round 1) — `JdbcMessageIngestRepository`, `JdbcWorkerMessageRepository` and their R2DBC counterparts still override old no-topic signatures explicitly even though the interface already provides the `default` delegation. Not a correctness issue.

---

## 3. Overall Plan Compliance

| Phase | Status |
|-------|--------|
| Phase 1 — Schema and Repository | DONE (R2DBC cross-topic isolation test gap remains) |
| Phase 2 — Per-Topic Configuration Model | DONE |
| Phase 3 — Consumer: Multi-Topic Ingest | DONE (functional); unit/integration test for multi-topic routing NOT DONE |
| Phase 4 — Worker: Coordinator Pattern | DONE (functional); unit tests for coordinator/instance NOT DONE |
| Phase 5 — Separate Table Strategy | DONE (functional); dedicated-table isolation integration test NOT DONE |
| Phase 6 — Observability: Topic Dimension | DONE |
| Phase 7 — Auto-Configuration Wiring | DONE |
| Phase 8 — Documentation and Examples | DONE |

---

## 4. Architecture Compliance

- Workers and consumers use repository ports only — no direct SQL in service classes. Compliant.
- Repository split (`repository-api`, `repository-jdbc`, `repository-r2dbc`) maintained. Compliant.
- Observability split maintained. Compliant.
- Schema migrations centralized in `schema-liquibase`. Compliant.
- Language rule (English for all code comments and log messages). Compliant.
- `docs/ARCHITECTURE.md` module map is up to date and includes all new v0.5.0 classes. Compliant.

---

## 5. Verdict

**PASS WITH NOTES**

Since Round 1, three of the five gaps have been fully resolved (Gap 1 — JDBC cross-topic isolation, Gap 2 — EXPLAIN test seeding, Gap 5 — metrics topic tag assertions) and one has been resolved at the documentation level (Gap 3 — Phase 8 docs). Gap 4 (unit tests for coordinator/instance and consumer multi-topic routing) remains entirely absent.

The implementation is functionally correct and architecturally sound. All core claim, fetch, reclaim, maintenance, and observability logic is verified. The remaining open items are:

- R2DBC cross-topic isolation test (plan §1.7, low severity).
- `TopicWorkerCoordinator` and `TopicWorkerInstance` unit tests (plan §4.7, low severity).
- Consumer multi-topic routing test (plan §3.4, low severity).
- Dedicated-table isolation integration test (plan §5.4, low severity).
- Stale index names in `docs/USAGE.md` "Claim Query Performance" section (documentation only, low severity).

None of these are blocking correctness issues for a pre-production library, but Gap 4 and the R2DBC isolation test should be resolved before the v0.5.0 release tag to ensure the plan's stated test matrix is satisfied.
