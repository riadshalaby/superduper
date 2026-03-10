# Plan — v0.5.0

Status: **ready**

Goal: Enable multi-topic consumption within a single application — each topic with its own message handler, independent worker tuning, and configurable table strategy — while sharing a single database connection pool.

Architecture decision: **Coordinator Pattern** — a single `TopicWorkerCoordinator` Spring bean manages per-topic worker instances as plain Java objects; maintenance services receive a topic registry and iterate over configured topics.

---

## Phase 1 — Schema and Repository: Topic Awareness

### 1.1 Schema changes (no migration — library not in production)

- `schema-liquibase/src/main/resources/db/changelog/superduper/001-init-schema-postgres.sql`
  - Add column `topic VARCHAR(255) NOT NULL DEFAULT 'default'` to `messages` table.
  - Drop index `idx_messages_message_key_id(key, id)`.
  - Add composite index `idx_messages_topic_status_key_id(topic, status, message_key, id)`.
- `schema-liquibase/src/main/resources/db/changelog/superduper/001-init-schema-mariadb.sql`
  - Same column and index changes, MariaDB syntax.
- Keep the `DEFAULT 'default'` in the schema so the column is never null even if the upsert somehow omits it; the application always supplies an explicit value.

### 1.2 ClaimedMessage — add topic field

- `repository-api/src/main/java/net/rsworld/superduper/repository/api/ClaimedMessage.java`
  - Add `String topic` as the second field: `ClaimedMessage(Long id, String topic, ...)`.
  - Update all call sites that construct or destructure the record.

### 1.3 Repository API — add topic parameter

- `repository-api/.../MessageIngestRepository.java`
  - `upsertReadyMessage(String topic, String messageId, String messageKey, String content, Instant occurredAt, String correlationId, String messageType)`
  - Topic becomes the first parameter for readability.
- `repository-api/.../ReactiveMessageIngestRepository.java`
  - Same signature change (returns `Mono<Void>`).
- `repository-api/.../WorkerMessageRepository.java`
  - `claimBatch(String workerId, int batchSize, int maxRetries, String topic)` — add topic param.
  - `fetchClaimedForWorker(String workerId, String topic)` — add topic param.
  - `findByStatus(String status, int limit, String topic)` — add topic param.
  - `releaseMessages(List<Long> ids, String containerId)` — no change (operates by id).
  - `markProcessed`, `markFailed`, `markStopped` — no change (operates by id + containerId).
  - `redriveById(long id)` — no change (operates by id).
  - `redriveByStatus(String status, int limit, String topic)` — add topic param.
  - `countByStatus(String topic)` — add topic param.
- `repository-api/.../ReactiveWorkerMessageRepository.java`
  - Mirror the same changes.
- `repository-api/.../WorkerMaintenanceRepository.java`
  - `reclaimStaleProcessing(int orphanTimeoutSec, String topic)` — add topic.
  - `reclaimMissingHeartbeats(int heartbeatWindowSec, String topic)` — add topic.
  - `deleteProcessedOlderThan(int retentionDays, String topic)` — add topic.
  - `deleteStoppedOlderThan(int retentionDays, String topic)` — add topic.
  - `heartbeat(String workerId)` — no change (shared).
  - `deleteStaleHeartbeats(int retentionDays)` — no change (shared).
- `repository-api/.../ReactiveWorkerMaintenanceRepository.java`
  - Mirror the same changes.

### 1.4 SQL dialect updates

Apply to all four dialect classes:
- `repository-jdbc/.../PostgresJdbcSqlDialect.java`
- `repository-jdbc/.../MariaDbJdbcSqlDialect.java`
- `repository-r2dbc/.../PostgresR2dbcSqlDialect.java`
- `repository-r2dbc/.../MariaDbR2dbcSqlDialect.java`

Changes per dialect:
- **upsertReadyMessageSql()** — add `topic` to INSERT column list and VALUES bind; ON CONFLICT/ON DUPLICATE KEY does not update `topic` (set once).
- **claimBatchSql()** — add `AND m1.topic = :topic` to the candidate WHERE clause.
- **fetchClaimedForWorkerSql()** — add `AND topic = :topic` to WHERE.
- **findByStatusSql()** — add `AND topic = :topic` to WHERE.
- **redriveByStatusSql()** — add `AND topic = :topic` to WHERE.
- **countByStatusSql()** — add `WHERE topic = :topic` (currently unfiltered GROUP BY).
- **reclaimStaleProcessingSql()** — add `AND topic = :topic`.
- **reclaimMissingHeartbeatsSql()** — add `AND m.topic = :topic`.
- **deleteProcessedOlderThanSql()** — add `AND topic = :topic`.
- **deleteStoppedOlderThanSql()** — add `AND topic = :topic`.
- **Row mapping** in fetch/find queries — map the `topic` column into the `ClaimedMessage` record.

### 1.5 Repository implementation updates

- `repository-jdbc/.../JdbcMessageIngestRepository.java` — pass topic to SQL bind params.
- `repository-jdbc/.../JdbcWorkerMessageRepository.java` — pass topic to SQL bind params in claim, fetch, find, redrive, count.
- `repository-jdbc/.../JdbcWorkerMaintenanceRepository.java` — pass topic to reclaim and delete SQL bind params.
- R2DBC equivalents — same changes.

### 1.6 Claim index migration scripts

- `002-worker-claim-indexes-postgres.sql` — update to reflect the new composite index `(topic, status, message_key, id)`. Drop the old index if it exists separately.
- `003-worker-claim-indexes-mariadb.sql` — same.

### 1.7 Integration tests

- Extend `JdbcWorkerMessageRepositoryIntegrationTest` and MariaDB variant:
  - Insert rows for two topics with the same key `K`; claim for topic A must not block key `K` in topic B.
  - Verify `ClaimedMessage.topic()` is correctly populated.
- Extend `JdbcRedriveIntegrationTest` — redrive scoped to topic.
- Extend `JdbcCleanupIntegrationTest` — cleanup scoped to topic.
- Mirror all additions for R2DBC integration tests.

---

## Phase 2 — Per-Topic Configuration Model

### 2.1 TopicProperties

- New class: `starter-autoselect/src/main/java/net/rsworld/superduper/starter/TopicProperties.java`
- `@ConfigurationProperties(prefix = "superduper")` (binds the `topics` map at this level).
- Fields:
  ```java
  Map<String, TopicConfig> topics = Collections.emptyMap();
  ```
- Inner record/class `TopicConfig`:
  ```java
  String kafkaTopic;       // actual Kafka topic name
  String handler;          // bean name of MessageHandler / ReactiveMessageHandler
  int batchSize = -1;      // -1 means "use global default"
  int maxRetries = -1;     // -1 means "use global default"
  String table = "";       // empty means shared table
  ```
- Annotate with `@EnableConfigurationProperties` in `AutoSelectConfiguration`.

### 2.2 TopicRegistry

- New class: `starter-autoselect/src/main/java/net/rsworld/superduper/starter/TopicRegistry.java`
- Built by auto-configuration from `TopicProperties` + `WorkerProperties` (global defaults).
- Holds a `List<ResolvedTopicConfig>` where each entry merges per-topic overrides with global defaults.
- `ResolvedTopicConfig` record:
  ```java
  record ResolvedTopicConfig(
      String name,             // logical name (map key)
      String kafkaTopic,       // Kafka topic
      String handlerBeanName,  // handler bean name
      int batchSize,           // resolved (topic override or global)
      int maxRetries,          // resolved
      String table,            // "" for shared, otherwise custom table name
      String claimLockName     // "superduper-claim-<name>"
  ) {}
  ```
- Convenience: `List<String> kafkaTopics()` — returns all Kafka topic names (for consumer subscription).

### 2.3 Backward compatibility

- When `superduper.topics` is empty:
  - Build a single-entry `TopicRegistry` using `superduper.kafka.topic` (existing property), the single `MessageHandler` bean, and global defaults from `WorkerProperties`.
  - The logical name is `"default"`, the topic column value is `"default"`.
- Existing users require zero config changes.

---

## Phase 3 — Consumer: Multi-Topic Ingest

### 3.1 Blocking consumer

- `consumer-kafka-blocking/.../KafkaConsumerService.java`
  - `@KafkaListener` topics expression: derive from `TopicRegistry.kafkaTopics()` at auto-config time (inject as a bean property or use SpEL referencing the registry bean).
  - In `onMessage(ConsumerRecord)`: extract `record.topic()` and look up the `ResolvedTopicConfig` from `TopicRegistry`.
  - Pass the resolved topic name (or Kafka topic, depending on config — use the logical topic name for the `topic` column) to `messageIngestRepository.upsertReadyMessage(topic, ...)`.
  - If a topic uses a dedicated table, resolve the correct `MessageIngestRepository` instance for that table (see Phase 5).

### 3.2 Reactive consumer

- `consumer-kafka-reactive/.../KafkaReactiveR2dbcConsumerService.java`
  - Same changes, reactive variant.

### 3.3 Topic column value convention

- The `topic` column stores the **Kafka topic name** (`record.topic()`), not the logical config name. This is the most natural and debuggable value. The `TopicRegistry` lookup maps Kafka topic → config.

### 3.4 Tests

- Extend consumer integration tests to verify that records from different Kafka topics are persisted with the correct `topic` column value.

---

## Phase 4 — Worker: Per-Topic Claim and Handler Dispatch (Coordinator Pattern)

### 4.1 TopicWorkerInstance (POJO)

- New class: `worker-blocking/src/main/java/net/rsworld/superduper/worker/blocking/TopicWorkerInstance.java`
- Constructor:
  ```java
  TopicWorkerInstance(
      ResolvedTopicConfig topicConfig,
      WorkerMessageRepository messageRepository,
      PlatformTransactionManager txm,
      LockingTaskExecutor lockExec,
      MessageHandler handler,
      SuperduperObserver observer,
      String workerId)
  ```
- Methods:
  - `void claimAndProcess()` — same logic as current `SuperDuperWorkerService.schedule()` but scoped to `topicConfig.kafkaTopic()`:
    - ShedLock name: `topicConfig.claimLockName()` (e.g., `"superduper-claim-orders"`).
    - `claimBatch(workerId, topicConfig.batchSize(), topicConfig.maxRetries(), topicConfig.kafkaTopic())`.
    - `fetchClaimedForWorker(workerId, topicConfig.kafkaTopic())`.
    - Handler: resolved from `topicConfig.handlerBeanName()`.
  - Processing logic (per-key ordering, mark processed/failed/stopped) stays identical.

### 4.2 TopicWorkerCoordinator (Spring bean)

- New class: `worker-blocking/src/main/java/net/rsworld/superduper/worker/blocking/TopicWorkerCoordinator.java`
- Single Spring bean created by auto-configuration.
- Constructor receives:
  - `TopicRegistry topicRegistry`
  - `WorkerMessageRepository messageRepository` (shared-table instance)
  - `PlatformTransactionManager txm`
  - `LockingTaskExecutor lockExec`
  - `Map<String, MessageHandler> handlers` (all handler beans by name)
  - `SuperduperObserver observer`
  - `WorkerProperties workerProperties`
- On construction:
  - For each `ResolvedTopicConfig` in the registry, create a `TopicWorkerInstance`.
  - Resolve the handler bean from the `handlers` map using `topicConfig.handlerBeanName()`.
  - If topic uses a dedicated table, create a dedicated `WorkerMessageRepository` (see Phase 5).
- Implements `SmartLifecycle` or uses `@PostConstruct` / `@PreDestroy`:
  - On start: register a `ScheduledFuture` per topic with `TaskScheduler`, using the global `claim-interval-ms`.
  - On stop: cancel all scheduled tasks.
- The `TaskScheduler` bean is created in auto-configuration (or use an existing one).

### 4.3 Reactive variant

- New class: `worker-reactive/.../ReactiveTopicWorkerInstance.java` — same pattern, reactive.
- New class: `worker-reactive/.../ReactiveTopicWorkerCoordinator.java` — same pattern, reactive.

### 4.4 Deprecate / refactor existing worker services

- `SuperDuperWorkerService` — keep as-is for now; the coordinator internally creates `TopicWorkerInstance` objects that reuse the same processing logic. Extract shared processing logic into a package-private helper if needed to avoid duplication.
- Alternatively, refactor `SuperDuperWorkerService` so its core logic is in a method that `TopicWorkerInstance` delegates to. The `@Scheduled` method in the old service class is only used in backward-compatible single-topic mode (but since backward compat is handled by the coordinator with a single-entry registry, the old `@Scheduled` class can be removed).
- **Decision: remove the old `SuperDuperWorkerService` `@Scheduled` bean.** The coordinator handles both single-topic (backward compat) and multi-topic modes. This avoids duplicate scheduling.

### 4.5 HeartbeatService

- Stays shared — one heartbeat per container, unchanged.
- No topic awareness needed.

### 4.6 Maintenance services become topic-aware

- **OrphanReclaimer** (`worker-blocking/.../OrphanReclaimer.java`):
  - Inject `TopicRegistry`.
  - In `reclaim()`, iterate over all topics and call `reclaimStaleProcessing(timeout, topic)` + `reclaimMissingHeartbeats(window, topic)` for each.
  - Sum reclaimed counts across topics for observation.
- **CleanupService** (`worker-blocking/.../CleanupService.java`):
  - Inject `TopicRegistry`.
  - Iterate over topics; call `deleteProcessedOlderThan(days, topic)` and `deleteStoppedOlderThan(days, topic)` per topic.
  - `deleteStaleHeartbeats` remains topic-agnostic (heartbeats are shared).
- **RedriveService** (`worker-blocking/.../RedriveService.java`):
  - Add topic parameter to `redriveBatch(String status, int limit, String topic)` and `inspect(String status, int limit, String topic)`.
  - `redriveOne(long id)` stays topic-agnostic (operates by id).
- **QueueHealthService** (`worker-blocking/.../QueueHealthService.java`):
  - Inject `TopicRegistry`.
  - In `poll()`, call `countByStatus(topic)` per topic.
  - Emit separate `queueBacklogObserved` per topic (with topic tag).
- Apply same changes to all reactive maintenance service variants.

### 4.7 Tests

- Unit test `TopicWorkerCoordinator`:
  - Two topics with different handlers — verify each handler is invoked for its topic's messages only.
  - Verify per-topic ShedLock names.
- Unit test `TopicWorkerInstance`:
  - Same per-key ordering guarantees, scoped to a topic.
- Integration tests:
  - End-to-end: produce to two topics → consume → two workers claim independently → correct handlers invoked.
- Update existing `SuperDuperWorkerServiceTest` and integration tests to pass topic parameter.

---

## Phase 5 — Separate Table Strategy (Opt-In)

### 5.1 Per-topic repository instances

- When `ResolvedTopicConfig.table()` is non-empty (e.g., `"orders_messages"`):
  - The coordinator creates a dedicated SQL dialect instance: `new PostgresJdbcSqlDialect("orders_messages", "container_heartbeats")` (dialects already accept table names via constructor).
  - Wraps it in a dedicated `JdbcWorkerMessageRepository` and `JdbcMessageIngestRepository`.
  - The `TopicWorkerInstance` and consumer use the dedicated repository for that topic.
- Shared tables (`shedlock`, `container_heartbeats`) remain single-instance.

### 5.2 Repository factory

- New class: `starter-autoselect/.../RepositoryFactory.java`
  - `WorkerMessageRepository createWorkerRepository(String tableName)` — constructs dialect + repository for the given table.
  - `MessageIngestRepository createIngestRepository(String tableName)` — same.
  - Reactive variants.
- The factory is injected into the coordinator and consumer to resolve per-topic repositories.

### 5.3 Liquibase template

- New file: `schema-liquibase/src/main/resources/db/changelog/superduper/topic-messages-template-postgres.sql`
  - Parameterized CREATE TABLE using Liquibase property `${table.name}`.
  - Same columns, constraints, and indexes as the main `messages` table.
- New file: `schema-liquibase/src/main/resources/db/changelog/superduper/topic-messages-template-mariadb.sql`
  - Same, MariaDB syntax.
- Document in `docs/USAGE.md` how to include the template with a custom table name.

### 5.4 Tests

- Integration test: configure one topic with a dedicated table, another with the shared table. Verify isolation — rows in the dedicated table are invisible to shared-table queries.

---

## Phase 6 — Observability: Topic Dimension

### 6.1 Observation records

- `observability-api/.../WorkerObservation.java`
  - Add `String topic` field (after `mode`).
- `observability-api/.../MaintenanceObservation.java`
  - Add `String topic` field. Use `"all"` or `"*"` for operations that span all topics (e.g., heartbeat).
- `ConsumerObservation` — already has a `topic` field. No change needed.

### 6.2 LoggingSuperduperObserver

- `observability-logging/.../LoggingSuperduperObserver.java`
  - Include `topic={}` in all worker and maintenance structured log lines.

### 6.3 MetricsSuperduperObserver

- `observability-metrics/.../MetricsSuperduperObserver.java`
  - Add `topic` tag to all worker meters (`superduper.worker.claim.total`, `superduper.worker.processed.total`, etc.).
  - Add `topic` tag to maintenance meters.
  - The `topic` tag on consumer meters already exists (based on `ConsumerObservation.topic()`); verify it works.
  - Queue backlog gauges: emit per-topic gauges with a `topic` tag.

### 6.4 Tests

- Update `MetricsSuperduperObserverTest` to verify topic tags are present on worker and maintenance meters.
- Update logging observer tests to verify topic field in structured output.

---

## Phase 7 — Auto-Configuration Wiring

### 7.1 AutoSelectConfiguration changes

- `starter-autoselect/.../AutoSelectConfiguration.java`
  - Add `@EnableConfigurationProperties({..., TopicProperties.class})`.
  - New `@Bean TopicRegistry topicRegistry(TopicProperties, WorkerProperties, @Value("${superduper.kafka.topic:}") String legacyTopic)` — builds the registry with backward-compat fallback.
  - New `@Bean TaskScheduler superduperTaskScheduler()` — shared scheduler for coordinator.
  - Replace `@Bean SuperDuperWorkerService` with `@Bean TopicWorkerCoordinator` (blocking path).
  - Replace `@Bean SuperDuperWorkerReactiveService` with `@Bean ReactiveTopicWorkerCoordinator` (reactive path).
  - Inject `Map<String, MessageHandler>` (or `Map<String, ReactiveMessageHandler>`) into coordinator bean creation.
  - `@Bean RepositoryFactory` — for per-topic dedicated table support.
  - Update `OrphanReclaimer`, `CleanupService`, `QueueHealthService` bean definitions to inject `TopicRegistry`.
  - Update `RedriveService` bean definition — topic parameter is caller-supplied, no registry injection needed (caller specifies which topic to redrive).

### 7.2 Consumer auto-configuration

- Inject `TopicRegistry` into consumer beans.
- Derive Kafka topic list from `topicRegistry.kafkaTopics()`.
- Inject `RepositoryFactory` for dedicated-table topic routing.

### 7.3 Backward compatibility verification

- When `superduper.topics` map is empty and `superduper.kafka.topic` is set:
  - `TopicRegistry` contains one entry: logical name `"default"`, Kafka topic from legacy property, single `MessageHandler` bean, global defaults.
  - All behavior is identical to v0.4.x.
- When neither is set: fail fast with a clear error message.

---

## Phase 8 — Documentation and Examples

### 8.1 docs/USAGE.md

- Add multi-topic configuration reference with YAML examples.
- Document separate-table opt-in pattern with Liquibase include instructions.
- Document handler bean naming convention.

### 8.2 docs/ARCHITECTURE.md

- Update data flow diagram to show per-topic routing.
- Add coordinator pattern description.
- Update module map with new classes.

### 8.3 README.md

- Update data model section: add `topic` column to the messages table description.
- Add a brief multi-topic overview in features.

### 8.4 Examples

- Update `examples/app-blocking` to demonstrate two-topic configuration with two handlers.
- Or add a new `examples/app-multi-topic` if the single-topic example should remain simple.

---

## Implementation Order

| Step | Phase | Depends On | Estimated Scope |
|------|-------|-----------|----------------|
| 1 | Phase 1 (Schema + Repository) | — | Foundation; all other phases depend on this |
| 2 | Phase 2 (Config Model) | — | Can start in parallel with Phase 1 |
| 3 | Phase 6 (Observability) | Phase 1 | Small; observation records updated early so workers can emit topic |
| 4 | Phase 3 (Consumer) | Phase 1, 2 | Consumer passes topic through |
| 5 | Phase 4 (Worker Coordinator) | Phase 1, 2, 3, 6 | Core orchestration change |
| 6 | Phase 5 (Separate Table) | Phase 4 | Opt-in extension |
| 7 | Phase 7 (Auto-Config) | Phase 2, 4, 5 | Wiring; done incrementally alongside Phases 4–5 |
| 8 | Phase 8 (Docs + Examples) | All above | Final |

---

## Risk Notes

- **ShedLock table sharing**: All per-topic claim locks share the same `shedlock` table. This is fine — ShedLock is designed for this. Lock names just need to be unique per topic (e.g., `superduper-claim-orders`).
- **Backward compatibility**: The single-topic fallback is critical. Must be covered by existing tests running unchanged against the new code.
- **Test matrix**: Each SQL change must be verified for both PostgreSQL and MariaDB, both JDBC and R2DBC — 4 combinations.
- **Claim index performance**: The new composite index `(topic, status, message_key, id)` replaces `(message_key, id)`. Verify via EXPLAIN that claim queries still use the index efficiently with the topic predicate. The existing `JdbcWorkerClaimExplainIntegrationTest` should be extended.
