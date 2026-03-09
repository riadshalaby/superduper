# Plan — v0.4.2

Status: **ready**

Goal: make redrive and queue-health capabilities easy to adopt in real applications, and add a configurable cleanup task with per-status retention so `messages` and `container_heartbeats` tables stay manageable under production traffic.

Design decisions:
- Cleanup uses **hard delete** (no archive table).
- Retention periods are **per-status**: `PROCESSED` and `STOPPED` get independent retention settings.

---

## Priority 1: Operator Entry Points and Examples

### Step 1.1 — Add redrive and queue-health examples to blocking app

**Files to modify:**

- `examples/app-blocking/src/main/java/.../BlockingExampleApplication.java` (or new class below)

**New file:**

- `examples/app-blocking/src/main/java/.../ExampleBlockingOperatorEndpoints.java`

Create a simple `@RestController` that exposes operator endpoints backed by the library's `RedriveService` and `QueueHealthService`:

```java
@RestController
@RequestMapping("/ops")
public class ExampleBlockingOperatorEndpoints {

    private final RedriveService redriveService;
    private final WorkerMessageRepository repository;

    /** GET /ops/queue — returns Map<String, Long> of status counts. */
    @GetMapping("/queue")
    public Map<String, Long> queueHealth();

    /** GET /ops/messages?status=FAILED&limit=20 — inspect messages by status. */
    @GetMapping("/messages")
    public List<ClaimedMessage> inspect(@RequestParam String status,
                                        @RequestParam(defaultValue = "20") int limit);

    /** POST /ops/redrive/{id} — redrive a single message. */
    @PostMapping("/redrive/{id}")
    public Map<String, Object> redriveOne(@PathVariable long id);

    /** POST /ops/redrive?status=FAILED&limit=100 — batch redrive. */
    @PostMapping("/redrive")
    public Map<String, Object> redriveBatch(@RequestParam String status,
                                            @RequestParam(defaultValue = "100") int limit);
}
```

Endpoints return simple JSON maps/lists — no custom DTOs. Error handling via `@ExceptionHandler` for `IllegalArgumentException` (invalid status) returning 400.

### Step 1.2 — Add redrive and queue-health examples to reactive app

**New file:**

- `examples/app-reactive/src/main/java/.../ExampleReactiveOperatorEndpoints.java`

Same contract as Step 1.1 but using `ReactiveRedriveService`, `ReactiveWorkerMessageRepository`, and returning `Mono`/`Flux` from WebFlux `@RestController`.

### Step 1.3 — Enable queue-health and observability in example configs

**Files to modify:**

- `examples/app-blocking/src/main/resources/application.yml`
- `examples/app-reactive/src/main/resources/application.yml`

Add to both:

```yaml
superduper:
  worker:
    queue-health:
      enabled: true
      interval-ms: 30000
  observability:
    enabled: true

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      show-details: always
  prometheus:
    metrics:
      export:
        enabled: true
```

This ensures the example apps demonstrate queue-health polling, full observability, and Prometheus-scrapable metrics out of the box.

### Step 1.4 — Add Actuator and Prometheus dependencies to example apps

**Files to modify:**

- `examples/app-blocking/pom.xml`
- `examples/app-reactive/pom.xml`

Add the following dependencies to both example app POMs:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

Also add `spring-boot-starter-web` to `app-blocking` (it currently only has `spring-boot-starter` and `spring-boot-starter-jdbc`, and needs a web server for the operator endpoints from Step 1.1 and the Actuator endpoints).

`app-reactive` already has `spring-boot-starter-webflux`, which supports Actuator natively.

### Step 1.5 — Add Prometheus and Grafana to Docker Compose

**New files:**

- `examples/observability/prometheus.yml` — Prometheus scrape configuration.
- `examples/observability/grafana/provisioning/datasources/prometheus.yml` — Grafana datasource provisioning (auto-register Prometheus).
- `examples/observability/grafana/provisioning/dashboards/dashboard.yml` — Grafana dashboard provisioning config.
- `examples/observability/grafana/dashboards/superduper.json` — Pre-built Grafana dashboard JSON.

**Files to modify:**

- `docker-compose.yml` — add Prometheus and Grafana services.
- `docker-compose.multi.yml` — add Prometheus and Grafana services with worker scrape targets.

**Prometheus configuration (`examples/observability/prometheus.yml`):**

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: superduper
    metrics_path: /actuator/prometheus
    static_configs:
      - targets:
          - host.docker.internal:8080
    # Multi-container variant adds worker-1:8080 through worker-5:8080
```

For `docker-compose.multi.yml`, use a separate `prometheus-multi.yml` or a file-based service discovery config that covers all worker instances.

**Docker Compose services to add:**

```yaml
prometheus:
  image: prom/prometheus:latest
  ports:
    - "9090:9090"
  volumes:
    - ./examples/observability/prometheus.yml:/etc/prometheus/prometheus.yml

grafana:
  image: grafana/grafana:latest
  ports:
    - "3000:3000"
  environment:
    - GF_SECURITY_ADMIN_USER=admin
    - GF_SECURITY_ADMIN_PASSWORD=admin
    - GF_AUTH_ANONYMOUS_ENABLED=true
    - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
  volumes:
    - ./examples/observability/grafana/provisioning:/etc/grafana/provisioning
    - ./examples/observability/grafana/dashboards:/var/lib/grafana/dashboards
```

### Step 1.6 — Create Grafana dashboard for all SuperDuper metrics

**New file:**

- `examples/observability/grafana/dashboards/superduper.json`

The dashboard should cover all metrics emitted by `MetricsSuperduperObserver` and `QueueHealthService`, organized into panels:

**Row 1 — Queue Health (gauges)**
- `superduper.queue.backlog{status=READY}` — messages waiting to be claimed.
- `superduper.queue.backlog{status=FAILED}` — messages awaiting retry.
- `superduper.queue.backlog{status=STOPPED}` — messages requiring intervention.
- `superduper.queue.backlog{status=PROCESSING}` — messages currently in-flight.
- Panel type: stat panels or gauge panels, one per status.

**Row 2 — Consumer Throughput (counters)**
- `rate(superduper.consumer.received.total[5m])` — ingest rate.
- `rate(superduper.consumer.persisted.total[5m])` — persist rate.
- `rate(superduper.consumer.failed.total[5m])` — consumer failure rate.
- `superduper.consumer.persist.duration` (p50, p95, p99) — ingest latency.
- Panel type: time series graphs.

**Row 3 — Worker Processing (counters + timers)**
- `rate(superduper.worker.processed.total[5m])` — processing rate.
- `rate(superduper.worker.retried.total[5m])` — retry rate.
- `rate(superduper.worker.stopped.total[5m])` — stopped rate.
- `rate(superduper.worker.failed.total[5m])` — error rate.
- `rate(superduper.worker.claim.total[5m])` — claim rate.
- `superduper.worker.process.duration` (p50, p95, p99) — processing latency.
- `superduper.worker.claim.duration` (p50, p95, p99) — claim latency.
- Panel type: time series graphs.

**Row 4 — Redrive (counters)**
- `rate(superduper.worker.redriven.total[5m])` — redrive rate.
- Panel type: time series graph.

**Row 5 — Maintenance (counters + timers)**
- `superduper.maintenance.total{result=success}` vs `{result=failure}` — heartbeat/orphan health.
- `rate(superduper.maintenance.reclaimed.total[5m])` — orphan reclaim rate.
- `superduper.maintenance.duration` — maintenance operation latency.
- Panel type: time series graphs.

**Row 6 — Cleanup (counters, added by Priority 2)**
- `rate(superduper.maintenance.cleanup.total{operation=cleanup-processed}[5m])`
- `rate(superduper.maintenance.cleanup.total{operation=cleanup-stopped}[5m])`
- `rate(superduper.maintenance.cleanup.total{operation=cleanup-heartbeats}[5m])`
- Panel type: time series graph.

Dashboard variables:
- `mode` — filter by `blocking` or `reactive`.
- `instance` — filter by Prometheus target instance.

Use Grafana dashboard JSON model format. Dashboard UID: `superduper-overview`. Auto-provisioned on Grafana startup via the provisioning config.

### Step 1.7 — Document operator workflows in USAGE.md

**Files to modify:**

- `docs/USAGE.md`

Add an "Operator Workflows" section covering:

1. **Inspecting failures** — how to call `RedriveService.inspect("FAILED", limit)` or hit the example REST endpoint.
2. **Redriving messages** — single-message and batch workflows, what happens to retry count, how per-key ordering is preserved after redrive.
3. **Monitoring queue health** — enabling `queue-health.enabled=true`, reading the gauge metrics, and setting up alerts on backlog growth.
4. **Blocking vs. reactive** — call-site differences (`RedriveService` vs. `ReactiveRedriveService`), both use the same repository contract.
5. **Safety notes** — warn against redriving while the key has a `PROCESSING` sibling (the claim query naturally handles this, but document the behavior).
6. **Prometheus and Grafana** — how to start the observability stack (`docker compose up`), access Prometheus at `localhost:9090`, access Grafana at `localhost:3000` (admin/admin), and find the pre-built SuperDuper dashboard.

### Step 1.8 — Update EXAMPLES.md

**Files to modify:**

- `docs/EXAMPLES.md`

Add instructions for:

**Operator endpoints:**
- How to query queue health: `curl localhost:8080/ops/queue`
- How to inspect failed messages: `curl localhost:8080/ops/messages?status=FAILED&limit=10`
- How to redrive a single message: `curl -X POST localhost:8080/ops/redrive/42`
- How to batch-redrive: `curl -X POST 'localhost:8080/ops/redrive?status=STOPPED&limit=50'`
- Expected JSON responses for each.

**Observability stack:**
- Starting Prometheus and Grafana with Docker Compose.
- Verifying metrics scraping: `curl localhost:8080/actuator/prometheus | grep superduper`
- Accessing the Grafana dashboard at `http://localhost:3000/d/superduper-overview`.
- Screenshot or description of expected dashboard panels.

---

## Priority 2: Queue Retention and Cleanup

### Step 2.1 — Repository API: add cleanup methods

**Files to modify:**

- `repository-api/.../WorkerMaintenanceRepository.java`
- `repository-api/.../ReactiveWorkerMaintenanceRepository.java`

**New methods (blocking):**

```java
int deleteProcessedOlderThan(int retentionDays);
int deleteStoppedOlderThan(int retentionDays);
int deleteStaleHeartbeats(int retentionDays);
```

**New methods (reactive):**

```java
Mono<Integer> deleteProcessedOlderThan(int retentionDays);
Mono<Integer> deleteStoppedOlderThan(int retentionDays);
Mono<Integer> deleteStaleHeartbeats(int retentionDays);
```

**Semantics:**

- `deleteProcessedOlderThan` — `DELETE FROM messages WHERE status = 'PROCESSED' AND last_updated < NOW() - INTERVAL :days DAY`. Returns number of rows deleted.
- `deleteStoppedOlderThan` — `DELETE FROM messages WHERE status = 'STOPPED' AND last_updated < NOW() - INTERVAL :days DAY`. Returns number of rows deleted.
- `deleteStaleHeartbeats` — `DELETE FROM container_heartbeats WHERE last_heartbeat < NOW() - INTERVAL :days DAY`. Returns number of rows deleted.

### Step 2.2 — SQL Dialect: add cleanup SQL

**Files to modify:**

- `repository-jdbc/.../JdbcSqlDialect.java` (interface)
- `repository-jdbc/.../PostgresJdbcSqlDialect.java`
- `repository-jdbc/.../MariaDbJdbcSqlDialect.java`
- `repository-r2dbc/.../R2dbcSqlDialect.java` (interface)
- `repository-r2dbc/.../PostgresR2dbcSqlDialect.java`
- `repository-r2dbc/.../MariaDbR2dbcSqlDialect.java`

**New dialect methods:**

```java
String deleteProcessedOlderThanSql();
String deleteStoppedOlderThanSql();
String deleteStaleHeartbeatsSql();
```

**SQL templates (Postgres):**

```sql
-- deleteProcessedOlderThanSql
DELETE FROM messages WHERE status = 'PROCESSED'
  AND last_updated < NOW() - MAKE_INTERVAL(days => :retentionDays);

-- deleteStoppedOlderThanSql
DELETE FROM messages WHERE status = 'STOPPED'
  AND last_updated < NOW() - MAKE_INTERVAL(days => :retentionDays);

-- deleteStaleHeartbeatsSql
DELETE FROM container_heartbeats
  WHERE last_heartbeat < NOW() - MAKE_INTERVAL(days => :retentionDays);
```

**SQL templates (MariaDB):**

```sql
-- deleteProcessedOlderThanSql
DELETE FROM messages WHERE status = 'PROCESSED'
  AND last_updated < TIMESTAMPADD(DAY, -:retentionDays, NOW());

-- deleteStoppedOlderThanSql
DELETE FROM messages WHERE status = 'STOPPED'
  AND last_updated < TIMESTAMPADD(DAY, -:retentionDays, NOW());

-- deleteStaleHeartbeatsSql
DELETE FROM container_heartbeats
  WHERE last_heartbeat < TIMESTAMPADD(DAY, -:retentionDays, NOW());
```

Note: follow the existing interval syntax pattern used in `reclaimStaleProcessingSql()` — Postgres uses `INTERVAL '... seconds'` while MariaDB uses `TIMESTAMPADD`. Adapt the day-based variant consistently.

### Step 2.3 — Repository implementations: wire cleanup SQL

**Files to modify:**

- `repository-jdbc/.../JdbcWorkerMaintenanceRepository.java`
- `repository-r2dbc/.../R2dbcWorkerMaintenanceRepository.java`

Implement the three new interface methods. Bind `:retentionDays` parameter, execute delete, return affected row count. Follow existing patterns in each class.

### Step 2.4 — Observer API: add cleanup signal

**Files to modify:**

- `observability-api/.../SuperduperObserver.java`

**New method:**

```java
default void maintenanceCleanup(MaintenanceObservation observation, int deletedCount) {}
```

### Step 2.5 — Observer implementations: emit cleanup events

**Files to modify:**

- `observability-logging/.../LoggingSuperduperObserver.java` — log at INFO level:

```
maintenance.cleanup mode={} operation={} deletedCount={} durationMs={}
```

Where `operation` is `"cleanup-processed"`, `"cleanup-stopped"`, or `"cleanup-heartbeats"`.

- `observability-metrics/.../MetricsSuperduperObserver.java` — register counter `superduper.maintenance.cleanup.total` with tags `mode` and `operation`.

### Step 2.6 — Configuration: add retention properties

**Files to modify:**

- `starter-autoselect/.../WorkerProperties.java`

**New nested class:**

```java
public static class Retention {
    private boolean enabled = false;
    private int processedRetentionDays = 14;
    private int stoppedRetentionDays = 30;
    private int heartbeatRetentionDays = 1;
    private long intervalMs = 86400000; // 24 hours
    private long initialDelayMs = 60000; // 1 minute
}
```

Accessible via `superduper.worker.retention.*` properties.

### Step 2.7 — Cleanup service classes

**New files:**

- `worker-blocking/.../CleanupService.java`
- `worker-reactive/.../ReactiveCleanupService.java`

**CleanupService (blocking):**

```java
public class CleanupService {

    private final WorkerMaintenanceRepository repository;
    private final SuperduperObserver observer;
    private final int processedRetentionDays;
    private final int stoppedRetentionDays;
    private final int heartbeatRetentionDays;

    @Scheduled(
        fixedDelayString = "${superduper.worker.retention.interval-ms:86400000}",
        initialDelayString = "${superduper.worker.retention.initial-delay-ms:60000}")
    public void cleanup() {
        long start = System.currentTimeMillis();

        int deletedProcessed = repository.deleteProcessedOlderThan(processedRetentionDays);
        int deletedStopped = repository.deleteStoppedOlderThan(stoppedRetentionDays);
        int deletedHeartbeats = repository.deleteStaleHeartbeats(heartbeatRetentionDays);

        long duration = System.currentTimeMillis() - start;

        // Emit one observation per category so metrics/logs distinguish them
        observer.maintenanceCleanup(
            new MaintenanceObservation("blocking", "n/a", "cleanup-processed", duration),
            deletedProcessed);
        observer.maintenanceCleanup(
            new MaintenanceObservation("blocking", "n/a", "cleanup-stopped", duration),
            deletedStopped);
        observer.maintenanceCleanup(
            new MaintenanceObservation("blocking", "n/a", "cleanup-heartbeats", duration),
            deletedHeartbeats);
    }
}
```

**ReactiveCleanupService:** same contract, reactive chain with `Mono.zip()` or sequential `flatMap`, ending with observer emissions. Blocks on `.block()` in the `@Scheduled` method (same pattern as `ReactiveOrphanReclaimer`).

### Step 2.8 — Auto-configuration: wire cleanup services

**Files to modify:**

- `starter-autoselect/.../AutoSelectConfiguration.java`

Register `CleanupService` bean (blocking stack) and `ReactiveCleanupService` bean (reactive stack), conditional on `superduper.worker.retention.enabled=true`. Inject retention properties from `WorkerProperties.Retention`.

### Step 2.9 — Integration tests for cleanup

**New test files:**

- `repository-jdbc/.../JdbcCleanupIntegrationTest.java` (Postgres)
- `repository-jdbc/.../JdbcCleanupMariaDbIntegrationTest.java` (MariaDB)
- `repository-r2dbc/.../R2dbcCleanupIntegrationTest.java` (Postgres)
- `repository-r2dbc/.../R2dbcCleanupMariaDbIntegrationTest.java` (MariaDB)
- `worker-blocking/.../CleanupServiceTest.java` (unit, mocked repo)
- `worker-reactive/.../ReactiveCleanupServiceTest.java` (unit, mocked repo)

**Scenarios to cover:**

1. `deleteProcessedOlderThan(14)` deletes `PROCESSED` rows with `last_updated` older than 14 days, returns correct count.
2. `deleteProcessedOlderThan(14)` does **not** delete `PROCESSED` rows within the retention window.
3. `deleteProcessedOlderThan(14)` does **not** delete rows in other statuses (`READY`, `PROCESSING`, `FAILED`, `STOPPED`).
4. `deleteStoppedOlderThan(30)` deletes only `STOPPED` rows older than 30 days.
5. `deleteStoppedOlderThan(30)` does **not** delete `FAILED` rows (they may still be redriven).
6. `deleteStaleHeartbeats(1)` deletes heartbeat rows older than 1 day, preserves recent ones.
7. **Non-interference with claim:** seed rows with mixed statuses and ages. Run cleanup, then run `claimBatch`. Verify the claim query returns the correct `READY` and retry-eligible `FAILED` rows — cleanup must not affect claimable rows.
8. **Non-interference with redrive:** seed `STOPPED` rows, some within retention, some outside. Run cleanup. Verify `redriveByStatus("STOPPED", N)` only sees the surviving (within-retention) rows.
9. **Non-interference with orphan recovery:** seed `PROCESSING` rows with stale `last_updated`. Run cleanup (should not touch `PROCESSING`). Run `reclaimStaleProcessing`. Verify reclaim still works.
10. **Observer emission:** verify `maintenanceCleanup` is called with correct counts per category.

### Step 2.10 — Enable cleanup in example apps

**Files to modify:**

- `examples/app-blocking/src/main/resources/application.yml`
- `examples/app-reactive/src/main/resources/application.yml`

Add:

```yaml
superduper:
  worker:
    retention:
      enabled: true
      processed-retention-days: 14
      stopped-retention-days: 30
      heartbeat-retention-days: 1
      interval-ms: 86400000    # 24 hours
```

### Step 2.11 — Documentation: retention guidance

**Files to modify:**

- `docs/USAGE.md`

Add a "Queue Retention and Cleanup" section covering:

1. **Configuration reference** — all `superduper.worker.retention.*` properties with defaults.
2. **Retention policy** — explain the per-status retention model: `PROCESSED` (default 14 days), `STOPPED` (default 30 days), `container_heartbeats` (default 1 day).
3. **What is NOT cleaned up** — `READY`, `PROCESSING`, and `FAILED` rows are never deleted by the cleanup task. `FAILED` rows may still be redriven; `PROCESSING` rows are live work; `READY` rows are pending.
4. **Operational tradeoffs** — shorter retention reduces table size and improves query performance; longer retention preserves investigation capability for `STOPPED` messages.
5. **Large-table considerations** — for tables with millions of rows, the `DELETE` statement may lock rows briefly. Recommend running cleanup during low-traffic windows or reducing `interval-ms` with smaller retention to spread the load.
6. **Manual cleanup** — users who need archive-before-delete can disable the built-in task (`retention.enabled=false`) and implement their own `SELECT INTO ... DELETE` workflow.
7. **Monitoring cleanup** — the `superduper.maintenance.cleanup.total` counter and `maintenance.cleanup` log lines confirm execution and deleted counts.

---

## Priority 3: Prepare the 0.5.x Release

### Step 3.1 — Update ROADMAP.md

**Files to modify:**

- `ROADMAP.md`

After all v0.4.2 code is merged, rewrite `ROADMAP.md` to scope the `0.5.x` line. Candidate topics (to be refined based on user feedback):

- Breaking API changes deferred from `0.4.x` (if any).
- Multi-topic support / topic-level isolation.
- Pluggable retry strategies (exponential backoff, jitter).
- Schema versioning and migration tooling improvements.
- Spring Boot 3.x / Jakarta alignment validation.

This step is documentation-only and does not require code changes.

---

## Implementation Order

| Phase | Steps | Rationale |
|---|---|---|
| Phase A | 2.1 → 2.3 | Repository API + dialect SQL + implementations for cleanup. Foundation layer, no service dependencies. |
| Phase B | 2.4 → 2.5 | Observer API + implementations for cleanup signal. Small, self-contained. |
| Phase C | 2.6 → 2.8 | Configuration properties + cleanup services + auto-configuration. Depends on Phase A and B. |
| Phase D | 2.9 | Cleanup integration tests. Run after services are wired. |
| Phase E | 1.1 → 1.4 | Example app operator endpoints + dependencies + config. Independent of cleanup work. Can run in parallel with Phases A–D. |
| Phase F | 1.5 → 1.6 | Prometheus + Grafana Docker Compose services and dashboard. Depends on Phase E (Actuator must be wired). |
| Phase G | 2.10, 1.7, 1.8, 2.11 | Documentation and example config updates. Last, after all code is stable. |
| Phase H | 3.1 | ROADMAP update for 0.5.x. After merge. |

---

## Files Summary

### New files

| File | Module | Purpose |
|---|---|---|
| `ExampleBlockingOperatorEndpoints.java` | `examples/app-blocking` | REST endpoints for redrive and queue inspection |
| `ExampleReactiveOperatorEndpoints.java` | `examples/app-reactive` | WebFlux endpoints for redrive and queue inspection |
| `CleanupService.java` | `worker-blocking` | Scheduled cleanup task (blocking) |
| `ReactiveCleanupService.java` | `worker-reactive` | Scheduled cleanup task (reactive) |
| `JdbcCleanupIntegrationTest.java` | `repository-jdbc` | Postgres cleanup IT |
| `JdbcCleanupMariaDbIntegrationTest.java` | `repository-jdbc` | MariaDB cleanup IT |
| `R2dbcCleanupIntegrationTest.java` | `repository-r2dbc` | Postgres cleanup IT (reactive) |
| `R2dbcCleanupMariaDbIntegrationTest.java` | `repository-r2dbc` | MariaDB cleanup IT (reactive) |
| `CleanupServiceTest.java` | `worker-blocking` | Cleanup unit test |
| `ReactiveCleanupServiceTest.java` | `worker-reactive` | Cleanup unit test |
| `examples/observability/prometheus.yml` | examples | Prometheus scrape config |
| `examples/observability/prometheus-multi.yml` | examples | Prometheus scrape config for multi-container demo |
| `examples/observability/grafana/provisioning/datasources/prometheus.yml` | examples | Grafana datasource auto-provisioning |
| `examples/observability/grafana/provisioning/dashboards/dashboard.yml` | examples | Grafana dashboard provisioning config |
| `examples/observability/grafana/dashboards/superduper.json` | examples | Pre-built Grafana dashboard (all SuperDuper metrics) |

### Modified files

| File | Module | Change |
|---|---|---|
| `WorkerMaintenanceRepository.java` | `repository-api` | Add `deleteProcessedOlderThan`, `deleteStoppedOlderThan`, `deleteStaleHeartbeats` |
| `ReactiveWorkerMaintenanceRepository.java` | `repository-api` | Reactive equivalents |
| `JdbcSqlDialect.java` | `repository-jdbc` | Add dialect methods for cleanup SQL |
| `PostgresJdbcSqlDialect.java` | `repository-jdbc` | Implement cleanup SQL (Postgres interval syntax) |
| `MariaDbJdbcSqlDialect.java` | `repository-jdbc` | Implement cleanup SQL (MariaDB TIMESTAMPADD syntax) |
| `JdbcWorkerMaintenanceRepository.java` | `repository-jdbc` | Implement cleanup interface methods |
| `R2dbcSqlDialect.java` | `repository-r2dbc` | Add dialect methods for cleanup SQL |
| `PostgresR2dbcSqlDialect.java` | `repository-r2dbc` | Implement cleanup SQL |
| `MariaDbR2dbcSqlDialect.java` | `repository-r2dbc` | Implement cleanup SQL |
| `R2dbcWorkerMaintenanceRepository.java` | `repository-r2dbc` | Implement cleanup interface methods |
| `SuperduperObserver.java` | `observability-api` | Add `maintenanceCleanup` signal |
| `LoggingSuperduperObserver.java` | `observability-logging` | Log cleanup events at INFO |
| `MetricsSuperduperObserver.java` | `observability-metrics` | Add `superduper.maintenance.cleanup.total` counter |
| `WorkerProperties.java` | `starter-autoselect` | Add nested `Retention` class with per-status settings |
| `AutoSelectConfiguration.java` | `starter-autoselect` | Wire `CleanupService` / `ReactiveCleanupService` |
| `pom.xml` | `examples/app-blocking` | Add `spring-boot-starter-actuator`, `spring-boot-starter-web`, `micrometer-registry-prometheus` |
| `pom.xml` | `examples/app-reactive` | Add `spring-boot-starter-actuator`, `micrometer-registry-prometheus` |
| `application.yml` | `examples/app-blocking` | Enable queue-health, observability, Actuator, Prometheus endpoint, retention |
| `application.yml` | `examples/app-reactive` | Enable queue-health, observability, Actuator, Prometheus endpoint, retention |
| `docker-compose.yml` | root | Add Prometheus and Grafana services |
| `docker-compose.multi.yml` | root | Add Prometheus and Grafana services with multi-worker scrape targets |
| `docs/USAGE.md` | docs | Operator workflows, Prometheus/Grafana setup, retention guidance |
| `docs/EXAMPLES.md` | docs | Operator endpoint curl examples, observability stack instructions |

---

## Validation Checklist

- [ ] `mvn -q -DskipTests test-compile` — all modules compile.
- [ ] `mvn -T 1C -q test` — all existing + new tests pass.
- [ ] Cleanup integration tests pass on Postgres and MariaDB (JDBC + R2DBC).
- [ ] Non-interference tests confirm cleanup does not affect claim, redrive, or orphan recovery.
- [ ] Example apps start and operator endpoints return expected JSON.
- [ ] `curl localhost:8080/actuator/prometheus | grep superduper` returns all expected metrics.
- [ ] `docker compose up` starts Prometheus (`:9090`) and Grafana (`:3000`) without errors.
- [ ] Grafana dashboard `superduper-overview` loads and displays panels for all metric categories.
- [ ] `mvn -q spotless:apply` — formatting clean.
- [ ] No new public API without Javadoc.
