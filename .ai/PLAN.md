# Plan

Status: **final**

Goal: implement the scope defined in `ROADMAP.md` — MariaDB payload truncation bugfix (Priority 1), Outbox Pattern support (Priority 2), and 1.0.0 stable release promotion (Priority 3).

## Task Dependency Graph

```
T-001 ──────────────────────────────────────────┐
T-002 ──────────────────────┐                   │
T-003 ──────────────────────┤                   │
T-004 ──────────────────────┤                   │
                            ▼                   │
                          T-005 ────────────────┤
                            │                   │
                            ▼                   │
                          T-006 ────────────────┤
                            │                   │
                            ▼                   │
                          T-007 ────────────────┤
                            │                   │
                            ▼                   ▼
                          T-008 (depends on all)
```

Suggested implement order: T-001 → T-002 → T-003 → T-004 → T-005 → T-006 → T-007 → T-008

---

## Task T-001 — MariaDB content column TEXT to LONGTEXT

Priority: 1 (bugfix) · Dependencies: none

### Scope

MariaDB `TEXT` columns are limited to ~64 KB. Large message payloads silently truncate. Change to `LONGTEXT` (~4 GB) for both the default `messages` table and the topic template used for new dedicated tables.

### Acceptance Criteria

- `topic-messages-template-mariadb.sql` declares `content LONGTEXT`.
- A new Liquibase migration changeset (`005-mariadb-content-longtext`) alters the existing `messages` table from `TEXT` to `LONGTEXT` (MariaDB only).
- PostgreSQL `TEXT` is already unbounded — no change needed there.
- `mvn -q -DskipTests test-compile` passes.
- `mvn -T 1C -q test` passes.

### Implementation

#### Step 1 — Schema template update

- File: `schema-liquibase/src/main/resources/db/changelog/superduper/topic-messages-template-mariadb.sql`
- Change: `content TEXT` to `content LONGTEXT`.

#### Step 2 — Migration changeset for existing tables

- File: `schema-liquibase/src/main/resources/db/changelog/superduper/005-mariadb-content-longtext.sql`
  ```sql
  ALTER TABLE messages MODIFY COLUMN content LONGTEXT;
  ```
- File: `schema-liquibase/src/main/resources/db/changelog/superduper/db.changelog-master.yaml`
  - Append new changeset `005-mariadb-content-longtext` (`dbms: mariadb`) referencing the new SQL file.

---

## Outbox Pattern — Key Design Decisions

These decisions apply across T-002 through T-007.

### Topic column alignment

Currently `TopicWorkerInstance` passes `topicConfig.kafkaTopic()` as the `topic` parameter to claim queries. Kafka consumers write `topicConfig.kafkaTopic()` into the `topic` column. These match for Kafka-sourced topics.

Outbox entries have no Kafka topic. To avoid breaking existing behavior, add a **new default method** `topicColumnValue()` to `TopicConfigView`:

```java
default String topicColumnValue() {
    String kafka = kafkaTopic();
    return kafka != null && !kafka.isBlank() ? kafka : name();
}
```

- For Kafka topics: returns `kafkaTopic()` (no change).
- For outbox topics: returns `name()` (the logical outbox name).
- Fully backward-compatible — existing data and queries are unaffected.

Then update `TopicWorkerInstance`, `ReactiveTopicWorkerInstance`, `KafkaConsumerService`, and `KafkaReactiveR2dbcConsumerService` to use `topicColumnValue()` instead of `kafkaTopic()` for the `topic` column value.

### Outbox entry representation in TopicRegistry

Outbox entries are `ResolvedTopicConfig` records with:
- `name` = outbox config key (e.g., `"order-events"`)
- `kafkaTopic` = `""` (empty string — not a Kafka source)
- `handlerBeanName` = user-configured handler
- `table` = optional dedicated table
- `claimLockName` = `"superduper-claim-<outbox-name>"`

`TopicRegistry.kafkaTopics()` must filter out entries where `kafkaTopic` is blank so the Kafka `@KafkaListener` does not subscribe to phantom topics.

### OutboxService API

Single-bean approach — one `OutboxService` bean (or `ReactiveOutboxService`) handles all configured outbox services, routing by name:

```java
public interface OutboxService {
    void send(String outboxName, String messageKey, String content);
    void send(String outboxName, String messageKey, String content,
              Instant occurredAt, String correlationId, String messageType);
}
```

- `message_id` is auto-generated as a UUID.
- `occurred_at` defaults to `Instant.now()` in the simple overload.
- `topic` column = outbox name (via `topicColumnValue()`).
- `status` = `READY`, `received_at` = now.
- Uses the existing `MessageIngestRepository.upsertReadyMessage()` for the INSERT (UUID guarantees no conflict; upsert behaves as pure INSERT).

The implementation holds a pre-resolved `Map<String, MessageIngestRepository>` (or reactive variant) keyed by outbox name, built by the starter during bean creation. It validates the outbox name on each `send()` call.

---

## Task T-002 — TopicConfigView evolution + worker/consumer alignment

Priority: 2 (outbox prep) · Dependencies: none

### Scope

Add the `topicColumnValue()` abstraction to `TopicConfigView`, filter blank Kafka topics from `TopicRegistry.kafkaTopics()`, and align workers and consumers to use the new method. This is a backward-compatible behavioral no-op for all existing Kafka-sourced topics — it only enables future outbox entries to have a non-Kafka topic column value.

### Acceptance Criteria

- `TopicConfigView` has a `topicColumnValue()` default method.
- `TopicRegistry.kafkaTopics()` filters out entries with blank `kafkaTopic`.
- `TopicWorkerInstance` and `ReactiveTopicWorkerInstance` pass `topicColumnValue()` as the topic.
- `KafkaConsumerService` and `KafkaReactiveR2dbcConsumerService` write `topicColumnValue()` in `MessageIngestData.topic`.
- All existing tests pass unchanged (behavioral no-op for Kafka topics).
- `mvn -q -DskipTests test-compile` passes.
- `mvn -T 1C -q test` passes.

### Implementation

#### Step 1 — TopicConfigView evolution (repository-api)

- Add `topicColumnValue()` default method to `TopicConfigView`.
- No breaking changes.

Files touched:
- `repository-api/src/main/java/net/rsworld/superduper/repository/api/TopicConfigView.java`

#### Step 2 — TopicRegistry filtering (starter-autoselect)

- Update `TopicRegistry.kafkaTopics()` to filter out entries with blank `kafkaTopic`.
- This prevents Kafka listeners from subscribing to outbox-only topics.

Files touched:
- `starter-autoselect/src/main/java/net/rsworld/superduper/starter/TopicRegistry.java`

#### Step 3 — Worker + Consumer alignment

- Update `TopicWorkerInstance` to pass `topicConfig.topicColumnValue()` instead of `topicConfig.kafkaTopic()` to `SuperDuperWorkerService`.
- Update `ReactiveTopicWorkerInstance` similarly.
- Update `KafkaConsumerService` to write `topicConfig.topicColumnValue()` in `MessageIngestData.topic`.
- Update `KafkaReactiveR2dbcConsumerService` similarly.
- Existing behavior is unchanged because `topicColumnValue()` == `kafkaTopic()` for all current Kafka topics.

Files touched:
- `worker-blocking/src/main/java/net/rsworld/superduper/worker/blocking/TopicWorkerInstance.java`
- `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/ReactiveTopicWorkerInstance.java`
- `consumer-kafka-blocking/src/main/java/net/rsworld/superduper/consumer/plain/KafkaConsumerService.java`
- `consumer-kafka-reactive/src/main/java/net/rsworld/superduper/consumer/kafka/reactive/KafkaReactiveR2dbcConsumerService.java`

---

## Task T-003 — outbox-blocking module

Priority: 2 (outbox core) · Dependencies: none

### Scope

Create the `outbox-blocking` Maven module containing the `OutboxService` interface and `JdbcOutboxService` implementation. This module is self-contained — it defines the user-facing API and delegates to `MessageIngestRepository` for persistence. Add the module to the parent POM.

### Acceptance Criteria

- New module `outbox-blocking` (artifact `superduper-outbox-blocking`) exists in parent POM `<modules>` and `<dependencyManagement>`.
- `OutboxService` interface with `send()` overloads.
- `JdbcOutboxService` implementation: validates outbox name, generates UUID `message_id`, routes to correct repository, calls `upsertReadyMessage()`.
- Unit tests verify: correct repository routing, UUID generation, parameter passing, unknown outbox name throws `IllegalArgumentException`.
- `mvn -q -DskipTests test-compile` passes.
- `mvn -T 1C -q test` passes.

### Implementation

#### Step 1 — Parent POM entry

- Add `outbox-blocking` to parent `pom.xml` `<modules>` list (after `consumer-kafka-reactive`).
- Add `superduper-outbox-blocking` to `<dependencyManagement>`.

Files touched:
- `pom.xml` (parent)

#### Step 2 — Module skeleton

New Maven module: `outbox-blocking`
- Artifact: `superduper-outbox-blocking`
- Package: `net.rsworld.superduper.outbox.blocking`

Dependencies:
- `superduper-repository-api`
- `superduper-observability-api`
- `superduper-repository-jdbc`
- `spring-boot-starter-jdbc`

Files created:
- `outbox-blocking/pom.xml`

#### Step 3 — OutboxService interface

```java
public interface OutboxService {
    void send(String outboxName, String messageKey, String content);
    void send(String outboxName, String messageKey, String content,
              Instant occurredAt, String correlationId, String messageType);
}
```

Files created:
- `outbox-blocking/src/main/java/net/rsworld/superduper/outbox/blocking/OutboxService.java`

#### Step 4 — JdbcOutboxService implementation

- Constructor: `(Map<String, MessageIngestRepository> repositories, SuperduperObserver observer)`
- `send()` validates outbox name exists in map, generates UUID `message_id`, resolves repository, calls `upsertReadyMessage(outboxName, messageId, messageKey, content, occurredAt, correlationId, messageType)`.
- Simple overload delegates to full overload with `occurredAt = Instant.now()`, `correlationId = null`, `messageType = null`.

Files created:
- `outbox-blocking/src/main/java/net/rsworld/superduper/outbox/blocking/JdbcOutboxService.java`

#### Step 5 — Unit tests

- `JdbcOutboxServiceTest.java` — mock `MessageIngestRepository` and `SuperduperObserver`:
  - Verify `send()` calls `upsertReadyMessage()` with correct topic = outbox name.
  - Verify `message_id` is a valid UUID.
  - Verify unknown outbox name throws `IllegalArgumentException`.
  - Verify repository routing for multiple outbox names.

Files created:
- `outbox-blocking/src/test/java/net/rsworld/superduper/outbox/blocking/JdbcOutboxServiceTest.java`

---

## Task T-004 — outbox-reactive module

Priority: 2 (outbox core) · Dependencies: none

### Scope

Create the `outbox-reactive` Maven module containing the `ReactiveOutboxService` interface and `R2dbcOutboxService` implementation. Mirrors `outbox-blocking` with reactive types. Add the module to the parent POM.

### Acceptance Criteria

- New module `outbox-reactive` (artifact `superduper-outbox-reactive`) exists in parent POM `<modules>` and `<dependencyManagement>`.
- `ReactiveOutboxService` interface with `send()` overloads returning `Mono<Void>`.
- `R2dbcOutboxService` implementation mirrors `JdbcOutboxService` with reactive semantics.
- Unit tests mirror blocking module tests with reactive assertions.
- `mvn -q -DskipTests test-compile` passes.
- `mvn -T 1C -q test` passes.

### Implementation

#### Step 1 — Parent POM entry

- Add `outbox-reactive` to parent `pom.xml` `<modules>` list (after `outbox-blocking`).
- Add `superduper-outbox-reactive` to `<dependencyManagement>`.

Files touched:
- `pom.xml` (parent)

#### Step 2 — Module skeleton

New Maven module: `outbox-reactive`
- Artifact: `superduper-outbox-reactive`
- Package: `net.rsworld.superduper.outbox.reactive`

Dependencies:
- `superduper-repository-api`
- `superduper-observability-api`
- `superduper-repository-r2dbc`
- `spring-boot-starter-data-r2dbc`
- `r2dbc-postgresql`

Files created:
- `outbox-reactive/pom.xml`

#### Step 3 — ReactiveOutboxService interface

```java
public interface ReactiveOutboxService {
    Mono<Void> send(String outboxName, String messageKey, String content);
    Mono<Void> send(String outboxName, String messageKey, String content,
                    Instant occurredAt, String correlationId, String messageType);
}
```

Files created:
- `outbox-reactive/src/main/java/net/rsworld/superduper/outbox/reactive/ReactiveOutboxService.java`

#### Step 4 — R2dbcOutboxService implementation

- Constructor: `(Map<String, ReactiveMessageIngestRepository> repositories, SuperduperObserver observer)`
- Same logic as blocking variant, reactive return types.

Files created:
- `outbox-reactive/src/main/java/net/rsworld/superduper/outbox/reactive/R2dbcOutboxService.java`

#### Step 5 — Unit tests

- `R2dbcOutboxServiceTest.java` — mirror of blocking tests with `StepVerifier` assertions.

Files created:
- `outbox-reactive/src/test/java/net/rsworld/superduper/outbox/reactive/R2dbcOutboxServiceTest.java`

---

## Task T-005 — Outbox configuration + TopicRegistry merge + starter wiring

Priority: 2 (outbox integration) · Dependencies: T-002, T-003, T-004

### Scope

Wire everything together: add `OutboxProperties` configuration, merge outbox entries into `TopicRegistry`, create `OutboxService`/`ReactiveOutboxService` beans in `AutoSelectConfiguration`, and update coverage aggregation. This is the integration point where workers start processing outbox messages.

### Acceptance Criteria

- Configuration under `superduper.outbox.<name>` is parsed with: `handler` (required), `batchSize`, `maxRetries`, `table` (optional).
- Outbox entries appear in `TopicRegistry` with `kafkaTopic = ""` and `claimLockName = "superduper-claim-<name>"`.
- Workers pick up outbox topics automatically.
- Kafka `@KafkaListener` does NOT subscribe to outbox topic names.
- `OutboxService` bean created for blocking mode; `ReactiveOutboxService` bean created for reactive mode.
- Dedicated-table outbox services use `RepositoryFactory`; shared-table outbox services use the default repository.
- Outbox modules included in coverage-report aggregation.
- `mvn -q -DskipTests test-compile` passes.
- `mvn -T 1C -q test` passes.

### Implementation

#### Step 1 — OutboxProperties (starter-autoselect)

- New class `OutboxProperties` under `starter-autoselect`:
  - Configuration prefix: `superduper` (same as `TopicProperties`)
  - `Map<String, OutboxConfig> outbox` where `OutboxConfig` has:
    - `handler` (required)
    - `batchSize` (optional, default 0 → falls back to `WorkerProperties.batchSize`)
    - `maxRetries` (optional, default 0 → falls back to `WorkerProperties.maxRetries`)
    - `table` (optional, empty → shared table)

Files created:
- `starter-autoselect/src/main/java/net/rsworld/superduper/starter/OutboxProperties.java`

#### Step 2 — TopicRegistry merges outbox entries

- Update `AutoSelectConfiguration.topicRegistry()` to accept `OutboxProperties` and append outbox entries as `ResolvedTopicConfig` with `kafkaTopic = ""`.
- Validate: `handler` must be set for each outbox entry.
- Workers will pick up outbox topics automatically.

Files touched:
- `starter-autoselect/src/main/java/net/rsworld/superduper/starter/AutoSelectConfiguration.java`

#### Step 3 — Starter wiring for outbox beans

- In `AutoSelectConfiguration`, add outbox bean definitions:
  - Blocking (`superduper.consumer.type=spring`): create `OutboxService` bean from `OutboxProperties` + `MessageIngestRepository` + `RepositoryFactory`.
  - Reactive (`superduper.consumer.type=reactor`): create `ReactiveOutboxService` bean similarly.
- Build the `Map<String, MessageIngestRepository>` (or reactive variant) by iterating outbox configs:
  - If config has a dedicated `table` → use `RepositoryFactory.createIngestRepository(table)`.
  - Otherwise → use the shared `MessageIngestRepository` bean.
- Conditional: only create outbox beans when `OutboxProperties.outbox` is non-empty.

Files touched:
- `starter-autoselect/pom.xml` (add dependencies on `outbox-blocking` and `outbox-reactive`)
- `starter-autoselect/src/main/java/net/rsworld/superduper/starter/AutoSelectConfiguration.java`

#### Step 4 — Coverage aggregation

- Add `outbox-blocking` and `outbox-reactive` to the `coverage-report` module's dependencies so JaCoCo aggregation includes them.

Files touched:
- `coverage-report/pom.xml`

---

## Task T-006 — Outbox examples in multitopic apps

Priority: 2 (outbox examples) · Dependencies: T-005

### Scope

Extend the existing `app-multitopic-shared` and `app-multitopic-dedicated` example applications to demonstrate outbox services running alongside Kafka consumers. This proves the composability requirement: users can run any mix of Kafka consumers + outbox services in the same application, with shared or dedicated table isolation.

Each example already has 2 Kafka consumer topics (orders, invoices) with their own handlers and a seeder. The addition introduces 1–2 outbox services (e.g., `notifications`, `audit-log`) with their own handlers plus a REST endpoint or seeder that writes via `OutboxService.send()` inside a `@Transactional` method — demonstrating the core transactional outbox pattern.

### Acceptance Criteria

- `app-multitopic-shared` runs with 2 Kafka topics + at least 1 outbox service, all sharing the `superduper_messages` table.
- `app-multitopic-dedicated` runs with 2 Kafka topics + at least 1 outbox service, the outbox service using its own dedicated table.
- Outbox messages are claimed and processed by the existing worker loop with the same per-key ordering and retry behavior.
- A seeder or REST endpoint demonstrates writing outbox messages inside a `@Transactional` method.
- New outbox message handlers log processing for observability.
- `application.yml` in each example app includes `superduper.outbox.*` configuration.
- `mvn -q -DskipTests test-compile` passes.
- `mvn -T 1C -q test` passes.

### Implementation

#### Step 1 — POM dependency update

Add `superduper-outbox-blocking` dependency to both example app POMs (they both use blocking mode).

Files touched:
- `examples/app-multitopic-shared/pom.xml`
- `examples/app-multitopic-dedicated/pom.xml`

#### Step 2 — app-multitopic-shared: outbox handler + config

- New handler: `NotificationsMessageHandler` implementing `MessageHandler`.
  - Logs received outbox messages, returns `SUCCESS` (with deterministic failure pattern like the existing handlers for demo purposes).
- Update `application.yml` to add outbox configuration:
  ```yaml
  superduper:
    outbox:
      notifications:
        handler: notificationsMessageHandler
        # no table → shares superduper_messages with Kafka topics
  ```

Files created:
- `examples/app-multitopic-shared/src/main/java/.../NotificationsMessageHandler.java`

Files touched:
- `examples/app-multitopic-shared/src/main/resources/application.yml`

#### Step 3 — app-multitopic-shared: outbox seeder

- New class `OutboxSeeder` (or extend existing `MultitopicSeeder`):
  - Injects `OutboxService`.
  - On startup (after a short delay), writes a batch of outbox messages via `outboxService.send("notifications", messageKey, content)` inside a `@Transactional` method.
  - Demonstrates: business data + outbox event persisted atomically.

Files created:
- `examples/app-multitopic-shared/src/main/java/.../OutboxSeeder.java`

#### Step 4 — app-multitopic-dedicated: outbox handler + config

- New handler: `NotificationsMessageHandler` implementing `MessageHandler` (same pattern as shared).
- Update `application.yml` to add outbox configuration with a dedicated table:
  ```yaml
  superduper:
    outbox:
      notifications:
        handler: notificationsMessageHandler
        table: outbox_notifications
  ```
- Add Liquibase changeset in the example app's changelog to create `outbox_notifications` table using the existing topic-messages template (same pattern as the dedicated Kafka topic tables `orders_messages` and `invoices_messages` in the existing example).

Files created:
- `examples/app-multitopic-dedicated/src/main/java/.../NotificationsMessageHandler.java`

Files touched:
- `examples/app-multitopic-dedicated/src/main/resources/application.yml`
- `examples/app-multitopic-dedicated/src/main/resources/db/changelog/` (new changeset for `outbox_notifications` table)

#### Step 5 — app-multitopic-dedicated: outbox seeder

- New class `OutboxSeeder` — same pattern as shared variant.

Files created:
- `examples/app-multitopic-dedicated/src/main/java/.../OutboxSeeder.java`

---

## Task T-007 — Documentation

Priority: 2 (outbox finalization) · Dependencies: T-006

### Scope

Update project documentation to reflect the new outbox modules, their architecture, usage, and examples.

### Acceptance Criteria

- `docs/ARCHITECTURE.md` has outbox modules in the Module Map, Dependency Graph, Data Flow, and Extension Points sections.
- `README.md` mentions the outbox service, shows a transactional usage snippet, and references the outbox examples.
- `mvn -q -DskipTests test-compile` passes (no code changes, but ensures nothing was broken).

### Implementation

#### Step 1 — ARCHITECTURE.md

- Add `outbox-blocking` and `outbox-reactive` to the Module Map table.
- Add outbox nodes and edges to the Dependency Graph mermaid diagram.
- Add an "Outbox Data Flow" section explaining the outbox ingest path.
- Add outbox service to the Extension Points section.

Files touched:
- `docs/ARCHITECTURE.md`

#### Step 2 — README.md

- Add outbox service description in the "Why SUPERDUPER" or introductory section.
- Add outbox to the high-level architecture diagram.
- Add outbox usage snippet to show the transactional pattern.
- Reference the multitopic example apps as outbox demos.

Files touched:
- `README.md`

---

## Task T-008 — 1.0.0 stable release promotion

Priority: 3 · Dependencies: T-001, T-002, T-003, T-004, T-005, T-006, T-007

### Scope

Promote the library to 1.0.0 stable release. All planned features (outbox pattern, MariaDB bugfix) must be complete. Use `release-as: 1.0.0` to trigger the major version bump via release-please.

### Acceptance Criteria

- `release-please-config.json` (or equivalent) includes `release-as: 1.0.0`.
- `README.md` and documentation reflect 1.0.0 stable status.
- All previous tasks (T-001 through T-007) are `done`.
- `mvn -q -DskipTests test-compile` passes.
- `mvn -T 1C -q test` passes.

### Implementation

#### Step 1 — release-please configuration

- Add `"release-as": "1.0.0"` to `.release-please-manifest.json` or the appropriate release-please configuration file.
- This ensures the next Release PR on `main` bumps to 1.0.0 regardless of conventional commit types.

Files touched:
- `.release-please-manifest.json` (or release-please config in CI workflow)

#### Step 2 — Documentation update

- Update `README.md` to reflect 1.0.0 stable status (remove any "pre-release" or "beta" language if present).
- Review docs for any version-specific references that need updating.

Files touched:
- `README.md`

---

## Validation

After each task:
- `mvn -q spotless:apply`
- `mvn -q -DskipTests test-compile`
- `mvn -T 1C -q test`
