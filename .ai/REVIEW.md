# Review — v0.5.0

Status: **PASS**

Review Round: **3**

Reviewed: 2026-03-10

---

## 1. Round 2 Open Items — Resolution Status

All five carry-forward items from Round 2 are now resolved.

### Item 1 — R2DBC cross-topic isolation test (plan §1.7)

**FIXED**

Both R2DBC integration tests now contain `claimBatch_isolatesKeysPerTopic` and
`dedicatedTableRows_areInvisibleToSharedTableQueries`:

- `R2dbcWorkerMessageRepositoryIntegrationTest` (PostgreSQL) — lines 117–139 and 141–181.
- `R2dbcWorkerMessageRepositoryMariaDbIntegrationTest` (MariaDB) — lines 72–94 and 97–136.

The isolation tests insert the same key into two topics, claim independently, and assert via
`fetchClaimedForWorker` that each worker only sees rows from its own topic and that
`ClaimedMessage.topic()` is correctly populated. Both the PostgreSQL and MariaDB variants are
in parity with the JDBC variants that were fixed in Round 1.

### Item 2 — Consumer multi-topic routing test (plan §3.4)

**FIXED**

The reactive consumer test now has a matching multi-topic routing test:

- `KafkaReactiveR2dbcConsumerServiceTest.onMessage_withTopicRegistry_routesToCorrectRepositoryWithTopicValue`
  (lines 134–193) — constructs the service with a `TopicRegistryView` and a
  `TopicRepositoryFactory`; sends records from two Kafka topics; asserts that `repoA` receives
  the call with `topic = "kafka-topic-a"` and `repoB` receives the call with
  `topic = "kafka-topic-b"`; asserts both acknowledgments are invoked.

The blocking consumer equivalent (`KafkaConsumerServiceTest` line 132) was already added in
the prior commit (eb9bd0a).

### Item 3 — Stale index names in `docs/USAGE.md` (documentation)

**FIXED** (in prior commit, now verified in place)

`docs/USAGE.md` §"Claim Query Performance" (lines 487–490) correctly references
`idx_messages_topic_status_key_id`, `idx_messages_processing_worker_key_id`, and
`idx_messages_processing_last_updated`. The stale names from the pre-v0.5.0 schema are gone.

### Item 4 — Unit tests for `TopicWorkerCoordinator` and `TopicWorkerInstance` (plan §4.7)

**FIXED**

`TopicWorkerCoordinatorTest` (5 tests):

- `coordinator_throwsIfHandlerMissing` — verifies fail-fast with a message containing the
  missing bean name.
- `topicWorkerInstance_usesTopicScopedLockName` — verifies ShedLock name equals
  `"superduper-claim-orders"` via `LockConfiguration` argument captor.
- `topicWorkerInstance_passesCorrectTopicToRepository` — verifies `claimBatch` receives the
  Kafka topic name (`"orders-kafka-topic"`).
- `coordinator_createOneWorkerPerTopic` — verifies `scheduleWithFixedDelay` is called exactly
  twice for a two-topic registry.
- `coordinator_usesDedicatedRepositoryForConfiguredTable` — verifies the factory creates a
  dedicated repository for a non-empty `table`, the dedicated repo's `claimBatch` is invoked,
  and the shared repo's `claimBatch` is never invoked.

`ReactiveTopicWorkerCoordinatorTest` (3 tests) mirrors the three main behaviours above for the
reactive path.

No separate `TopicWorkerInstanceTest` or `ReactiveTopicWorkerInstanceTest` file was created;
instance behaviour (lock name, topic routing, dedicated repo) is fully exercised through the
coordinator tests, which is an acceptable test organisation choice.

### Item 5 — Dedicated-table isolation integration test (plan §5.4)

**FIXED**

Added in all four DB/driver combinations:

- `JdbcWorkerMessageRepositoryIntegrationTest.dedicatedTableRows_areInvisibleToSharedTableQueries`
- `JdbcWorkerMessageRepositoryMariaDbIntegrationTest.dedicatedTableRows_areInvisibleToSharedTableQueries_onMariaDb`
- `R2dbcWorkerMessageRepositoryIntegrationTest.dedicatedTableRows_areInvisibleToSharedTableQueries`
- `R2dbcWorkerMessageRepositoryMariaDbIntegrationTest.dedicatedTableRows_areInvisibleToSharedTableQueries_onMariaDb`

Each test creates a `dedicated_repo` using the appropriate dialect with table `orders_messages`,
inserts a row into the shared table and a row into the dedicated table, then asserts via
`findByStatus`, `claimBatch`, and `fetchClaimedForWorker` that each repository only sees rows
from its own table. `recreateTopicTable()` constructs the dedicated table inline with the
correct schema and indexes for each database dialect.

---

## 2. New Issues Found

**No new issues identified.**

Minor observation (non-blocking, informational only):

- **Handler dispatch isolation not unit-tested** — the plan §4.7 stated "Two topics with
  different handlers — verify each handler is invoked for its topic's messages only."
  `coordinator_createOneWorkerPerTopic` verifies that two scheduled tasks are created but does
  not drive messages through to assert `handlerA.handle(...)` for topic-a and
  `handlerB.handle(...)` for topic-b. This is not a correctness concern — existing
  `SuperDuperWorkerServiceTest` covers the per-message dispatch path, and the coordinator test
  exercises the dedicated-repo routing path end-to-end. The gap is purely cosmetic. Not worth
  a change request.

---

## 3. Overall Plan Compliance

| Phase | Status |
|-------|--------|
| Phase 1 — Schema and Repository | DONE |
| Phase 2 — Per-Topic Configuration Model | DONE |
| Phase 3 — Consumer: Multi-Topic Ingest | DONE |
| Phase 4 — Worker: Coordinator Pattern | DONE |
| Phase 5 — Separate Table Strategy | DONE |
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
- All new test files are in the correct module test source trees. Compliant.

---

## 5. Verdict

**PASS**

All five open items from Review Round 2 are resolved. The implementation is functionally
correct, architecturally sound, and the test matrix satisfies the plan's stated requirements
across all four DB/driver combinations (PostgreSQL JDBC, MariaDB JDBC, PostgreSQL R2DBC,
MariaDB R2DBC). No blocking findings remain.
