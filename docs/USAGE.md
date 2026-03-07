# Usage Guide

> See [docs/ARCHITECTURE.md](ARCHITECTURE.md) for the module map and internal data flow.

## Library Modules & How to Use

### Maven coordinates

```xml
<dependency>
  <groupId>net.rsworld</groupId>
  <artifactId>superduper-starter-autoselect</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>

<!-- Select one consumer module -->
<dependency>
  <groupId>net.rsworld</groupId>
  <artifactId>superduper-consumer-kafka-blocking</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>

<dependency>
  <groupId>net.rsworld</groupId>
  <artifactId>superduper-consumer-kafka-reactive</artifactId>
  <version>1.0.0-SNAPSHOT</version>
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
- `superduper.maintenance.total`
- `superduper.consumer.persist.duration`
- `superduper.worker.claim.duration`
- `superduper.worker.process.duration`
- `superduper.maintenance.duration`

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
- Indexes: keep `messages(message_key, id)` and add claim/fetch/reclaim indexes from the example Liquibase SQL
- Idempotency: `message_id` is deterministic from Kafka `topic:partition:offset` by default

## Security & Consistency Notes

- Claiming is a single SQL statement per dialect to minimize race windows.
- A batch can claim multiple rows for the same key when no row for that key is already `PROCESSING`.
- Workers still process each key in `id` order and release later same-key rows in the batch when an earlier row fails.
- ShedLock ensures one claim section runs at a time across shared infrastructure.
- In Kubernetes, claim coordination remains cluster-wide as long as pods share the same database and `superduper.worker.shedlock.claim-lock-name`.
- Reactive processing still scales horizontally; lock coordination only guards the claim entry point.

## Consumer Metadata SPI

`starter-autoselect` registers a default `ConsumerMetadataResolver` bean. It resolves:

- `message_id` from header `message_id`, otherwise deterministic UUID from `topic:partition:offset`
- `occurred_at` from header `occurred_at`, otherwise Kafka record timestamp
- `correlation_id` from header `correlationId`, otherwise a random UUID
- `message_type` from header `message_type`, otherwise `null`

Override it with your own bean if you want custom metadata mapping.
