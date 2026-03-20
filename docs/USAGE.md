# Usage Guide

> See [docs/ARCHITECTURE.md](ARCHITECTURE.md) for the module map and internal data flow.

## Library Modules & How to Use

### Maven coordinates

Use the stable `1.0.0` artifacts for the first general-availability release line.

```xml
<dependency>
  <groupId>net.rsworld</groupId>
  <artifactId>superduper-starter-autoselect</artifactId>
  <version>1.0.0</version>
</dependency>

<!-- Select one consumer module -->
<dependency>
  <groupId>net.rsworld</groupId>
  <artifactId>superduper-consumer-kafka-blocking</artifactId>
  <version>1.0.0</version>
</dependency>

<dependency>
  <groupId>net.rsworld</groupId>
  <artifactId>superduper-consumer-kafka-reactive</artifactId>
  <version>1.0.0</version>
</dependency>
```

### Choose stack via properties

```yaml
superduper:
  consumer:
    type: spring   # or reactor
  kafka:
    bootstrap-servers: localhost:9092
    group-id: my-group
    topic: my-topic
    # topics: topic-a,topic-b,topic-c
  db:
    tables:
      messages: messages
      heartbeats: container_heartbeats
  worker:
    shedlock:
      table-name: shedlock
      claim-lock-name: superduper-claim-batch
```

### Multi-topic configuration

Use exactly one of these modes:

- legacy single-topic mode with `superduper.kafka.topic`
- multi-topic mode with `superduper.topics`

Example:

```yaml
superduper:
  consumer:
    type: spring
  topics:
    orders:
      kafka-topic: orders.events
      handler: ordersMessageHandler
      batch-size: 200
      max-retries: 5
    invoices:
      kafka-topic: invoices.events
      handler: invoicesMessageHandler
      table: invoices_messages
```

Rules:

- `handler` must match the Spring bean name of a `MessageHandler` or `ReactiveMessageHandler`
- `table` is optional; leave it empty to use the shared `messages` table
- `batch-size` and `max-retries` fall back to `superduper.worker.*` when omitted
- `superduper.kafka.topic` remains the backward-compatible single-topic path and resolves to logical topic name `default`

Operational behavior:

- consumers persist the Kafka topic into `messages.topic`
- workers create one scheduled claim loop per configured topic
- queue health, cleanup, orphan reclaim, and redrive route per topic and per table
- metrics and logs include the topic dimension for worker and maintenance observations
- dedicated-table mode should point Liquibase at `db.changelog-infra.yaml`, then include the per-topic table changelogs instead of `db.changelog-master.yaml`

Schema split:

- shared mode keeps the standard `messages` table plus `container_heartbeats` and `shedlock`
- dedicated mode creates only `container_heartbeats`, `shedlock`, and the configured per-topic tables such as `orders_messages` and `invoices_messages`

For end-to-end shared and dedicated multi-topic runs, see [docs/EXAMPLES.md#multi-topic-examples](EXAMPLES.md#multi-topic-examples).

### Shared vs. Dedicated Table Comparison

| Concern | Shared Table | Dedicated Table |
|---|---|---|
| Schema overhead | Single `messages` table, no extra DDL | One table per topic, Liquibase changesets needed |
| Schema outcome | `messages` + `container_heartbeats` + `shedlock` | `<topic>_messages` per topic + `container_heartbeats` + `shedlock` (no `messages` table) |
| Operational simplicity | One table to monitor, backup, and index | Per-topic tables to manage independently |
| Query isolation | `WHERE topic = :topic` filter | Full table-level isolation |
| Index contention | All topics share the same indexes | Each table has its own index set |
| Scaling model | Homogeneous topics with similar SLAs | Heterogeneous topics with independent SLAs |
| Maintenance routing | Same repository with a topic predicate | Separate repository instance per table |
| Recommended when | Few topics, similar volume and latency | Varying SLAs, compliance isolation, or very high per-topic volume |

Recommended use-cases:

- Shared table works well for roughly up to ten homogeneous topics with uniform SLAs and simpler operations.
- Dedicated tables fit compliance or audit isolation, independent scaling or retention, and workloads where per-topic index contention matters.

### Provide your business logic

JDBC:

```java
@Bean
MessageHandler superduperWorker() {
  return row -> {
    return ProcessingResult.SUCCESS;
  };
}
```

Reactive:

```java
@Bean
ReactiveMessageHandler superduperWorkerReactive() {
  return row -> Mono.fromCallable(() -> ProcessingResult.SUCCESS);
}
```

The starter auto-configures:

- JDBC worker, ShedLock, heartbeat, and orphan reclaimer when `superduper.consumer.type=spring`
- Reactive worker, ShedLock, reactive heartbeat, and reactive orphan reclaimer when `superduper.consumer.type=reactor`
- Kafka consumers that persist records into `messages`
- In multi-topic mode, a per-topic worker coordinator plus per-topic repository routing for maintenance operations

### Observability configuration

Default behavior:

- logging enabled
- metrics disabled
- all components and signals enabled

```yaml
superduper:
  observability:
    enabled: true
    components:
      consumer: true
      worker: true
      maintenance: true
    signals:
      lifecycle: true
      success: true
      failure: true
      retry: true
      timing: true
    outputs:
      log:
        enabled: true
      metrics:
        enabled: false
    metrics:
      tags:
        topic: true
        exception-tag: true
```

Available knobs:

- `superduper.observability.enabled`: master switch
- `superduper.observability.components.consumer`: consumer events
- `superduper.observability.components.worker`: worker claim/process/retry/stop/failure events
- `superduper.observability.components.maintenance`: heartbeat/orphan reclaim events
- `superduper.observability.signals.lifecycle`: lifecycle events
- `superduper.observability.signals.success`: success events
- `superduper.observability.signals.failure`: failure events
- `superduper.observability.signals.retry`: retry events
- `superduper.observability.signals.timing`: duration tracking
- `superduper.observability.outputs.log.enabled`: structured logs
- `superduper.observability.outputs.metrics.enabled`: Micrometer metrics
- `superduper.observability.metrics.tags.topic`: include Kafka topic metric tag
- `superduper.observability.metrics.tags.exception-tag`: include exception class tag on failures

Backends:

- logging observer: logs only
- metrics observer: logs plus Micrometer metrics
- if metrics are enabled but no `MeterRegistry` is available, the configuration falls back to logging when log output is enabled

Metrics emitted when enabled:

- `superduper.consumer.received.total`
- `superduper.consumer.persisted.total`
- `superduper.consumer.failed.total`
- `superduper.worker.claim.total`
- `superduper.worker.processed.total`
- `superduper.worker.retried.total`
- `superduper.worker.stopped.total`
- `superduper.worker.failed.total`
- `superduper.worker.redriven.total`
- `superduper.maintenance.total`
- `superduper.maintenance.reclaimed.total`
- `superduper.queue.backlog{mode,status}`
- `superduper.consumer.persist.duration`
- `superduper.worker.claim.duration`
- `superduper.worker.process.duration`
- `superduper.maintenance.duration`

## Redrive

Use the administrative redrive services to inspect or requeue terminal failures:

```java
@Autowired
private RedriveService redriveService;

List<ClaimedMessage> failed = redriveService.inspect("FAILED", 100);
boolean redriven = redriveService.redriveOne(42L);
int redrivenBatch = redriveService.redriveBatch("STOPPED", 50);
```

Reactive:

```java
@Autowired
private ReactiveRedriveService reactiveRedriveService;

reactiveRedriveService.inspect("FAILED", 100).collectList();
reactiveRedriveService.redriveOne(42L);
reactiveRedriveService.redriveBatch("STOPPED", 50);
```

Redrive contract:

- only `FAILED` and `STOPPED` rows are inspectable or redrivable
- redrive resets `status='READY'`, `retry_count=0`, `container_id=NULL`, and updates `last_updated`
- a redriven row is still subject to the normal per-key claim ordering rules
- if a sibling row for the same key is already `PROCESSING`, the redriven row remains unclaimable until that sibling completes

## Operator Workflows

### Inspecting failures

Blocking applications can inspect terminal failures directly with `RedriveService`:

```java
List<ClaimedMessage> failed = redriveService.inspect("FAILED", 20);
```

Reactive applications use the same repository contract through `ReactiveRedriveService`:

```java
reactiveRedriveService.inspect("FAILED", 20).collectList();
```

The example apps expose the same workflow over HTTP:

```bash
curl 'http://localhost:8080/ops/messages?status=FAILED&limit=20'
```

### Redriving messages

Single-message redrive:

```java
boolean redriven = redriveService.redriveOne(42L);
```

Batch redrive:

```java
int redriven = redriveService.redriveBatch("STOPPED", 50);
```

Reactive calls return `Mono<Boolean>` and `Mono<Integer>`:

```java
reactiveRedriveService.redriveOne(42L);
reactiveRedriveService.redriveBatch("STOPPED", 50);
```

HTTP equivalents:

```bash
curl -X POST http://localhost:8080/ops/redrive/42
curl -X POST 'http://localhost:8080/ops/redrive?status=STOPPED&limit=50'
```

Redrive resets the row to `READY`, clears `container_id`, resets `retry_count` to `0`, and still respects the normal claim ordering rules for that key.

### Monitoring queue health

Enable periodic queue-health polling:

```yaml
superduper:
  worker:
    queue-health:
      enabled: true
      interval-ms: 30000
```

The example apps also expose an operator view for the same backlog counts:

```bash
curl http://localhost:8080/ops/queue
```

Metrics backends receive `superduper.queue.backlog{mode,status}` gauges for `READY`, `FAILED`, `STOPPED`, and `PROCESSED`. Alert on sustained `READY` backlog growth, any non-zero `STOPPED`, and use `PROCESSED` as a coarse completion counter between cleanup runs.

### Blocking vs. reactive

- Blocking stacks call `RedriveService` and `WorkerMessageRepository`.
- Reactive stacks call `ReactiveRedriveService` and `ReactiveWorkerMessageRepository`.
- Both stacks persist the same schema, use the same status model, and preserve the same per-key claim ordering.

### Safety notes

- Only redrive `FAILED` or `STOPPED` rows.
- Avoid bulk redrive without checking whether a key still has a `PROCESSING` sibling. The claim query will still prevent out-of-order execution, but the row will remain queued until that sibling finishes.
- `FAILED` rows stay available for later redrive; cleanup never deletes them.

### Prometheus and Grafana

Start the local infrastructure plus the observability stack:

```bash
docker compose up -d
```

Then run one example app and verify scraping:

```bash
curl http://localhost:8080/actuator/prometheus | grep superduper
```

Access:

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Dashboard: `http://localhost:3000/d/superduper-overview`

## Tuning & Operational Guidance

- Batch size: `superduper.worker.batch-size`
- Claim interval: `superduper.worker.claim-interval-ms`
- Initial delays:
  - claim loop: `superduper.worker.claim-initial-delay-ms`
  - heartbeat loop: `superduper.worker.heartbeat-initial-delay-ms`
  - orphan reclaimer loop: `superduper.worker.orphan-initial-delay-ms`
- Kafka topics:
  - single topic: `superduper.kafka.topic`
  - multiple topics: `superduper.kafka.topics`
- Repository tables:
  - message queue table: `superduper.db.tables.messages`
  - heartbeat table: `superduper.db.tables.heartbeats`
- Heartbeat interval and orphan timeout: tune to your failover SLO
- ShedLock claim settings:
  - table: `superduper.worker.shedlock.table-name`
  - claim lock name: `superduper.worker.shedlock.claim-lock-name`
  - lock windows: `superduper.worker.shedlock.lock-at-most-for-ms`, `superduper.worker.shedlock.lock-at-least-for-ms`
- Max retries: alert on `STOPPED`, then requeue manually if appropriate
- Queue-health polling: `superduper.worker.queue-health.enabled` (default `false`)
- Queue-health polling interval: `superduper.worker.queue-health.interval-ms` (default `60000`)
- Indexes: keep `messages(message_key, id)` and add claim/fetch/reclaim indexes from the example Liquibase SQL
- Idempotency: `message_id` is deterministic from Kafka `topic:partition:offset` by default

## Security & Consistency Notes

- Claiming is a single SQL statement per dialect to minimize race windows.
- A batch can claim multiple rows for the same key when no row for that key is already `PROCESSING`.
- Workers still process each key in `id` order and release later same-key rows in the batch when an earlier row fails.
- ShedLock ensures one claim section runs at a time across shared infrastructure.
- In Kubernetes, claim coordination remains cluster-wide as long as pods share the same database and `superduper.worker.shedlock.claim-lock-name`.
- Reactive processing still scales horizontally; lock coordination only guards the claim entry point.

## Operational Monitoring

### Metrics

| Metric | Tags | Meaning |
|---|---|---|
| `superduper.consumer.received.total` | `mode`, optional `topic`, optional `exception` | Kafka records received |
| `superduper.consumer.persisted.total` | `mode`, optional `topic`, optional `exception` | Consumer writes persisted |
| `superduper.consumer.failed.total` | `mode`, optional `topic`, optional `exception` | Consumer persistence failures |
| `superduper.worker.claim.total` | `mode`, optional `exception`, `claimed_count` | Claim loop executions |
| `superduper.worker.processed.total` | `mode`, optional `exception` | Messages completed successfully |
| `superduper.worker.retried.total` | `mode`, optional `exception` | Messages moved to `FAILED` |
| `superduper.worker.stopped.total` | `mode`, optional `exception` | Messages moved to `STOPPED` |
| `superduper.worker.failed.total` | `mode`, optional `exception` | Worker execution failures |
| `superduper.worker.redriven.total` | `mode` | Messages redriven to `READY` |
| `superduper.maintenance.total` | `mode`, `operation`, optional `exception`, `result` | Heartbeat and reclaim runs |
| `superduper.maintenance.reclaimed.total` | `mode`, `operation`, optional `exception` | Rows reclaimed during maintenance |
| `superduper.maintenance.cleanup.total` | `mode`, `operation` | Rows deleted by cleanup |
| `superduper.queue.backlog` | `mode`, `status` | Current row counts for `READY`, `FAILED`, `STOPPED`, `PROCESSED` |
| `superduper.consumer.persist.duration` | same as consumer counters | Consumer persistence latency |
| `superduper.worker.claim.duration` | same as worker claim counter | Claim latency |
| `superduper.worker.process.duration` | same as worker outcome counters | Per-message processing latency |
| `superduper.maintenance.duration` | same as maintenance counters | Heartbeat or reclaim latency |

### Key log lines

| Log line | Level | Meaning |
|---|---|---|
| `consumer.received ...` | INFO | Consumer received a record |
| `consumer.persisted ...` | DEBUG | Consumer write succeeded |
| `consumer.failed ...` | ERROR | Consumer write failed |
| `worker.claimed ...` | INFO | Claim loop completed |
| `worker.processed ...` | DEBUG | Single message processed successfully |
| `worker.batch.completed ...` | INFO | Batch summary with processed/failed/stopped counts |
| `worker.retry ...` | WARN | Message moved to `FAILED` |
| `worker.stopped ...` | ERROR | Message moved to `STOPPED` |
| `worker.redriven ...` | INFO | Manual redrive completed |
| `maintenance.ok ...` | INFO | Heartbeat or reclaim run succeeded |
| `maintenance.cleanup ...` | INFO | Cleanup deleted retained rows |
| `maintenance.failed ...` | ERROR | Heartbeat or reclaim run failed |

Example:

```text
worker.batch.completed mode=blocking workerId=worker-a batchSize=100 processed=94 failed=5 stopped=1 durationMs=87
consumer.persisted mode=reactive topic=orders partition=3 offset=991 durationMs=4
app.handler.completed correlation_id=order-42 message_type=OrderCreated message_id=2b6c3d7e-8b34-4f60-a1e6-5c9c18b9f0d1 key=customer-17 outcome=SUCCESS durationMs=12
```

Recommended Prometheus / Grafana queries:

- backlog growth: `sum by (status) (superduper_queue_backlog)`
- claim rate: `sum(rate(superduper_worker_claim_total[5m]))`
- failure rate: `sum(rate(superduper_worker_failed_total[5m])) + sum(rate(superduper_worker_retried_total[5m]))`
- stopped alert: `sum(superduper_queue_backlog{status="STOPPED"}) > 0`
- missing heartbeats: alert when `increase(superduper_maintenance_total{operation="heartbeat",result="success"}[5m]) == 0`
- orphan reclaim activity: `sum(rate(superduper_maintenance_reclaimed_total{operation="orphan-reclaim"}[5m]))`
- cleanup rate: `sum(rate(superduper_maintenance_cleanup_total[5m]))`

Operational checks:

- alert on sustained `READY` backlog growth combined with flat claim rate
- alert on any non-zero `STOPPED` backlog
- use reclaim counters and direct database checks to investigate stale `PROCESSING` rows or missing heartbeats
- include `correlation_id` and `message_type` in your application-level handler logs when you need message-specific diagnostics

## Queue Retention and Cleanup

Enable cleanup with per-status retention:

```yaml
superduper:
  worker:
    retention:
      enabled: true
      processed-retention-days: 14
      stopped-retention-days: 30
      heartbeat-retention-days: 1
      interval-ms: 86400000
      initial-delay-ms: 60000
```

Configuration reference:

- `superduper.worker.retention.enabled`: enable scheduled cleanup, default `false`
- `superduper.worker.retention.processed-retention-days`: delete old `PROCESSED` rows after this many days, default `14`
- `superduper.worker.retention.stopped-retention-days`: delete old `STOPPED` rows after this many days, default `30`
- `superduper.worker.retention.heartbeat-retention-days`: delete old `container_heartbeats` rows after this many days, default `1`
- `superduper.worker.retention.interval-ms`: cleanup schedule interval, default `86400000`
- `superduper.worker.retention.initial-delay-ms`: cleanup startup delay, default `60000`

Retention policy:

- `PROCESSED` rows are deleted after `14` days by default.
- `STOPPED` rows are deleted after `30` days by default.
- `container_heartbeats` rows are deleted after `1` day by default.

What is not cleaned up:

- `READY` rows are never deleted by cleanup.
- `PROCESSING` rows are never deleted by cleanup.
- `FAILED` rows are never deleted by cleanup because they may still be redriven.

Operational tradeoffs:

- shorter retention keeps `messages` and `container_heartbeats` smaller and helps index efficiency
- longer `STOPPED` retention keeps more time for manual investigation and audit
- cleanup uses hard deletes, so deleted rows are not archived by the library

Manual cleanup workflow:

- if you need archive-before-delete, leave `superduper.worker.retention.enabled=false`
- run your own scheduled workflow that first copies eligible `PROCESSED` or `STOPPED` rows into an archive table or external store, then deletes the archived ids from `messages`
- apply the same pattern to `container_heartbeats` if heartbeat history must be retained outside the live table

Large-table considerations:

- cleanup is a plain `DELETE`, so very large tables can still hold row locks briefly
- prefer running cleanup during lower-traffic windows when message volume is high
- if cleanup batches are too large, reduce retention windows or run cleanup more frequently to spread the work

## Claim Query Performance

Expected plan characteristics:

- PostgreSQL claim plan should use `idx_messages_topic_status_key_id`
- PostgreSQL fetch plan should use `idx_messages_processing_worker_key_id` or `idx_messages_processing_last_updated`
- MariaDB claim plan should use `idx_messages_topic_status_key_id`
- MariaDB fetch plan should use `idx_messages_processing_worker_key_id` or `idx_messages_processing_last_updated`
- neither dialect should regress to a full table scan for the core `messages` access paths

Run the explain-plan guardrails locally:

```bash
mvn -q -pl repository-jdbc -Dtest=JdbcWorkerClaimExplainIntegrationTest,JdbcWorkerClaimExplainMariaDbIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -q -pl repository-r2dbc -Dtest=R2dbcWorkerClaimExplainIntegrationTest,R2dbcWorkerClaimExplainMariaDbIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Regression warning signs:

- PostgreSQL plans showing `Seq Scan` on `messages`
- MariaDB `EXPLAIN` rows with `type=ALL` on claim or fetch
- missing claim or processing index names in the plan output
- row estimates or filtered percentages jumping sharply after schema or query changes

Reference the Liquibase changelogs for required indexes:

- `schema-liquibase/src/main/resources/db/changelog/superduper/topic-messages-template-postgres.sql`
- `schema-liquibase/src/main/resources/db/changelog/superduper/topic-messages-template-mariadb.sql`

## Consumer Metadata SPI

`starter-autoselect` registers a default `ConsumerMetadataResolver` bean. It resolves:

- `message_id` from header `message_id`, otherwise deterministic UUID from `topic:partition:offset`
- `occurred_at` from header `occurred_at`, otherwise Kafka record timestamp
- `correlation_id` from header `correlationId`, otherwise a random UUID
- `message_type` from header `message_type`, otherwise `null`

Override it with your own bean if you want custom metadata mapping.
