# ROADMAP

Goal: enable multi-topic consumption within a single application — each topic with its own message handler, independent worker tuning, and configurable table strategy — while sharing a single database connection pool.

## Priority 1: Schema and Repository — Topic Awareness

Objective: make the `messages` table and repository layer topic-aware.

- Add a `topic VARCHAR(255) NOT NULL` column to the `messages` table in the init schema scripts (`001-init-schema-postgres.sql`, `001-init-schema-mariadb.sql`). No migration needed; the library is not in production use.
- Replace the existing `idx_messages_message_key_id` index with a composite index on `(topic, status, message_key, id)` to support topic-filtered claim queries.
- Add `topic` parameter to `MessageIngestRepository.upsertReadyMessage(...)` and `ReactiveMessageIngestRepository.upsertReadyMessage(...)`.
- Add `topic` field to the `ClaimedMessage` record.
- Update all SQL dialect implementations (Postgres and MariaDB, JDBC and R2DBC) so that claim, fetch, reclaim, cleanup, and redrive SQL includes `topic` filtering.
- Update upsert SQL to persist the topic on insert.
- Extend existing integration tests to verify topic-filtered claim and per-key ordering is scoped per topic (key `K` in topic A does not block key `K` in topic B).

## Priority 2: Per-Topic Configuration Model

Objective: define the property structure that lets users configure N topics with independent settings.

- Introduce a `superduper.topics.<name>` configuration map in a new `TopicProperties` class. Each entry holds:
  - `kafka-topic` — the actual Kafka topic name.
  - `handler` — bean name of the `MessageHandler` / `ReactiveMessageHandler` to use.
  - `batch-size` — override for the global default.
  - `max-retries` — override for the global default.
  - `table` — optional; when set, that topic uses a dedicated `messages` table instead of the shared one.
- Global defaults (`superduper.worker.batch-size`, `superduper.worker.max-retries`) apply when a topic entry omits them.
- Backward compatibility: when `superduper.topics` is absent, fall back to the current single-topic behavior (`superduper.kafka.topic` + single `MessageHandler` bean). Existing users require zero config changes.

## Priority 3: Consumer — Multi-Topic Ingest

Objective: route consumed Kafka records to the correct table and store the topic.

- Refactor `KafkaConsumerService` and `KafkaReactiveR2dbcConsumerService` to resolve the target repository (shared or per-topic) from the record's topic name.
- Pass `record.topic()` through to the repository `upsertReadyMessage` call so the `topic` column is populated.
- The `@KafkaListener` already supports comma-separated topics; derive the topic list from the `superduper.topics` map at auto-configuration time.
- `ConsumerMetadataResolver` stays unchanged — topic comes from the Kafka record, not from headers.

## Priority 4: Worker — Per-Topic Claim and Handler Dispatch

Objective: run an independent claim/process loop per topic, each bound to its own handler.

- For each entry in `superduper.topics`, auto-configuration creates a `SuperDuperWorkerService` (or reactive variant) that:
  - Claims only rows where `topic = :topic` (shared table) or from the topic's dedicated table.
  - Uses the handler bean named in the topic config.
  - Uses its own ShedLock entry (e.g., `superduper-claim-<name>`) so topics do not block each other.
  - Applies the topic-level `batch-size` and `max-retries`.
- `HeartbeatService` stays shared — one heartbeat per container regardless of topic count.
- `OrphanReclaimer` and `CleanupService` become topic-aware: iterate over configured topics and apply reclaim/cleanup per topic (with topic filter for shared table, or per-table for dedicated tables).
- `RedriveService` and `QueueHealthService` become topic-aware with the same approach.

## Priority 5: Separate Table Strategy (Opt-In)

Objective: allow users to assign a dedicated `messages` table per topic for full isolation.

- When a topic entry sets `table: orders_messages`, auto-configuration creates a dedicated repository instance using that table name, backed by the same `DataSource` / `ConnectionFactory` — no additional connection pool.
- The SQL dialect for that repository is constructed with the custom table name (dialects already accept table names via constructor).
- Liquibase: provide a reusable Liquibase include fragment (`topic-messages-template.yaml`) that users can include once per custom table, passing the table name as a Liquibase property. Document the pattern in `docs/USAGE.md`.
- The shared `shedlock` and `container_heartbeats` tables are always single-instance — they do not multiply with topics.

## Priority 6: Observability — Topic Dimension

Objective: make metrics and logs sliceable per topic.

- Add a `topic` field to `WorkerObservation`, `ConsumerObservation`, and `MaintenanceObservation`.
- `LoggingSuperduperObserver` includes the topic in structured log fields.
- `MetricsSuperduperObserver` adds a `topic` tag to all meters.
- Update observability tests to verify the topic dimension.

## Priority 7: Documentation and Examples

Objective: show users how to configure and run multi-topic applications.

- Update `docs/USAGE.md` with the multi-topic configuration reference and the separate-table opt-in pattern.
- Update `docs/ARCHITECTURE.md` with the new data flow for multi-topic.
- Update `README.md` data model section to reflect the `topic` column.
- Add or extend an example application (`examples/app-blocking` or a new `examples/app-multi-topic`) demonstrating two topics with different handlers and different worker settings.
