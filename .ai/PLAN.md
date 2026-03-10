# Plan — 0.5.1-SNAPSHOT

Status: **ready_for_implement**

Goal: create two multi-topic example applications and supporting documentation as
defined in `ROADMAP.md`.

## Design Decision Record

- **Structure:** two new standalone example modules (option B).
  - `examples/app-multitopic-shared` — shared `messages` table, two Kafka topics.
  - `examples/app-multitopic-dedicated` — one table per Kafka topic.
- **Stack coverage:** blocking only (option 2a). Reactive users extrapolate from the pattern.
- **Topics:** `orders.events` and `invoices.events` (two Kafka topics, two handlers).
- **Liquibase:** shared module reuses core `db.changelog-master.yaml`. Dedicated module
  uses an extended changelog that includes the core master and adds per-topic table DDL
  inside its own resources.
- **Docker infrastructure:** reuse existing `docker-compose.yml` (Kafka auto-creates topics).
- Existing `app-blocking` and `app-reactive` examples are **not modified**.

---

## T-001 — Shared-Table Multi-Topic Example (ROADMAP Priority 1)

### Scope

Create the `examples/app-multitopic-shared` Maven module. Two Kafka topics
(`orders.events`, `invoices.events`) share one `messages` table.

### Files to Create

#### 1. `examples/app-multitopic-shared/pom.xml`

Maven module. Parent: `superduper-parent`. Artifact id: `example-app-multitopic-shared`.

Dependencies (mirror `examples/app-blocking/pom.xml`):
- `spring-boot-starter`
- `spring-boot-starter-web`
- `spring-boot-starter-jdbc`
- `spring-boot-starter-actuator`
- `micrometer-registry-prometheus`
- `spring-kafka`
- `postgresql`
- `spring-boot-starter-liquibase`
- `superduper-schema-liquibase`
- `superduper-starter-autoselect`
- `superduper-consumer-kafka-blocking`

Build plugin: `spring-boot-maven-plugin` with `repackage` goal.
Property: `<sonar.skip>true</sonar.skip>`.

#### 2. `examples/app-multitopic-shared/src/main/java/net/rsworld/superduper/example/multitopic/shared/SharedMultitopicApplication.java`

Standard `@SpringBootApplication` main class.

#### 3. `examples/app-multitopic-shared/src/main/java/net/rsworld/superduper/example/multitopic/shared/OrdersMessageHandler.java`

`@Component("ordersMessageHandler")` implementing `MessageHandler`.

Behavior:
- Log prefix `[Orders]`.
- Same failure patterns as the existing blocking handler:
  `always-fail` -> `FAILURE`, `retry-once` first attempt -> `FAILURE`, else `SUCCESS`.
- Register progress with `SeedProgress`.

#### 4. `examples/app-multitopic-shared/src/main/java/net/rsworld/superduper/example/multitopic/shared/InvoicesMessageHandler.java`

`@Component("invoicesMessageHandler")` implementing `MessageHandler`.

Same pattern as `OrdersMessageHandler` but with log prefix `[Invoices]`.

#### 5. `examples/app-multitopic-shared/src/main/java/net/rsworld/superduper/example/multitopic/shared/SeedProgress.java`

Copy from `examples/app-blocking` (same latch-based tracking utility). Shared by both handlers.

#### 6. `examples/app-multitopic-shared/src/main/java/net/rsworld/superduper/example/multitopic/shared/MultitopicSeeder.java`

`@Component` activated by `superduper.example.seed.enabled=true`.

Behavior:
- On `ApplicationReadyEvent`, publish `count` messages to **each** of the two Kafka topics.
- Use `keyCount` distinct keys per topic (e.g., `order-0..order-19`, `invoice-0..invoice-19`).
- Same failure content patterns (`retry-once`, `always-fail`) as the existing seeder.
- Wait for `SeedProgress` latch (expected total = 2 * count).
- Read topic names from `superduper.topics` config keys and bootstrap servers from config.

Config properties consumed:
- `superduper.kafka.bootstrap-servers`
- `superduper.example.seed.enabled` (default `false`)
- `superduper.example.seed.count` (default `500`, per topic)
- `superduper.example.seed.keys` (default `20`, per topic)
- `superduper.example.seed.await-timeout-seconds` (default `180`)
- `superduper.example.seed.readiness-timeout-seconds` (default `30`)

#### 7. `examples/app-multitopic-shared/src/main/java/net/rsworld/superduper/example/multitopic/shared/MultitopicOperatorEndpoints.java`

Copy of `ExampleBlockingOperatorEndpoints` adapted for the new package.
Same `/ops/queue`, `/ops/messages`, `/ops/redrive/{id}`, `/ops/redrive` endpoints.

#### 8. `examples/app-multitopic-shared/src/main/resources/application.yml`

```yaml
spring:
  application:
    name: superduper-example-multitopic-shared
  autoconfigure:
    exclude:
      - org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration
      - org.springframework.boot.r2dbc.autoconfigure.R2dbcInitializationAutoConfiguration
      - org.springframework.boot.r2dbc.autoconfigure.R2dbcTransactionManagerAutoConfiguration
      - org.springframework.boot.data.r2dbc.autoconfigure.DataR2dbcAutoConfiguration
      - org.springframework.boot.data.r2dbc.autoconfigure.DataR2dbcRepositoriesAutoConfiguration
  datasource:
    url: ${SPRING_DATASOURCE_URL:jdbc:postgresql://localhost:5432/superduper}
    username: ${SPRING_DATASOURCE_USERNAME:superduper}
    password: ${SPRING_DATASOURCE_PASSWORD:superduper}
  liquibase:
    change-log: classpath:db/changelog/superduper/db.changelog-master.yaml

superduper:
  db:
    dialect: ${SUPERDUPER_DB_DIALECT:postgres}
  consumer:
    type: spring
  observability:
    enabled: true
    outputs:
      metrics:
        enabled: true
  kafka:
    bootstrap-servers: ${SUPERDUPER_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    group-id: example-multitopic-shared
  topics:
    orders:
      kafka-topic: orders.events
      handler: ordersMessageHandler
      batch-size: 200
      max-retries: 5
    invoices:
      kafka-topic: invoices.events
      handler: invoicesMessageHandler
  worker:
    max-retries: 3
    claim-interval-ms: 2000
    batch-size: 500
    queue-health:
      enabled: true
      interval-ms: 30000
    retention:
      enabled: true
      processed-retention-days: 14
      stopped-retention-days: 30
      heartbeat-retention-days: 1
      interval-ms: 86400000
  example:
    seed:
      enabled: false
      count: 500
      keys: 20
      await-timeout-seconds: 180
      readiness-timeout-seconds: 30

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

logging:
  level:
    root: INFO
```

No `table` field on either topic entry — both use the default shared `messages` table.

### Files to Modify

| File | Change |
|---|---|
| `pom.xml` (root) | Add `<module>examples/app-multitopic-shared</module>` |

### Validation

- `mvn -q -DskipTests test-compile` passes with the new module.
- `mvn -T 1C -q test` passes (no regressions).
- Running with seed enabled produces messages in the shared `messages` table with
  distinct `topic` values (`orders.events`, `invoices.events`).

### Acceptance Criteria

1. `examples/app-multitopic-shared` compiles as part of the reactor build.
2. The app starts successfully.
3. Two independent Kafka topics are consumed and persisted to the shared `messages` table.
4. Worker runs per-topic claim loops (verified by log output showing topic dimension).
5. Seeder publishes to both topics and waits for handler completion.
6. `SELECT topic, COUNT(*) FROM messages GROUP BY topic;` shows rows for both topics.
7. Per-key ordering holds for each topic independently.

---

## T-002 — Dedicated-Table Multi-Topic Example (ROADMAP Priority 2)

### Scope

Create the `examples/app-multitopic-dedicated` Maven module. Each Kafka topic writes
to its own message table (`orders_messages`, `invoices_messages`), with Liquibase
wiring to create those tables.

### Files to Create

#### 1. `examples/app-multitopic-dedicated/pom.xml`

Same dependency set as T-001's module. Artifact id: `example-app-multitopic-dedicated`.

#### 2. `examples/app-multitopic-dedicated/src/main/java/net/rsworld/superduper/example/multitopic/dedicated/DedicatedMultitopicApplication.java`

Standard `@SpringBootApplication` main class.

#### 3. Handler + support classes

Same set of classes as T-001, adapted for the `dedicated` package:
- `OrdersMessageHandler.java`
- `InvoicesMessageHandler.java`
- `SeedProgress.java`
- `MultitopicSeeder.java`
- `MultitopicOperatorEndpoints.java`

These can be near-identical copies. The handler and seeder logic is the same;
only the package name and application name differ.

#### 4. `examples/app-multitopic-dedicated/src/main/resources/application.yml`

Same base config as T-001 except:
- `spring.application.name: superduper-example-multitopic-dedicated`
- `spring.liquibase.change-log` points to the dedicated changelog.
- `superduper.kafka.group-id: example-multitopic-dedicated`
- Each topic entry includes an explicit `table`:

```yaml
superduper:
  topics:
    orders:
      kafka-topic: orders.events
      handler: ordersMessageHandler
      batch-size: 200
      max-retries: 5
      table: orders_messages
    invoices:
      kafka-topic: invoices.events
      handler: invoicesMessageHandler
      table: invoices_messages
```

Liquibase override:

```yaml
spring:
  liquibase:
    change-log: classpath:db/changelog/multitopic-dedicated/db.changelog-dedicated.yaml
```

#### 5. `examples/app-multitopic-dedicated/src/main/resources/db/changelog/multitopic-dedicated/db.changelog-dedicated.yaml`

Extended changelog:

```yaml
databaseChangeLog:
  - include:
      file: db/changelog/superduper/db.changelog-master.yaml
  - changeSet:
      id: multitopic-001-orders-messages-postgres
      author: superduper-example
      dbms: postgresql
      changes:
        - sqlFile:
            path: db/changelog/multitopic-dedicated/orders-messages-postgres.sql
            relativeToChangelogFile: false
  - changeSet:
      id: multitopic-002-invoices-messages-postgres
      author: superduper-example
      dbms: postgresql
      changes:
        - sqlFile:
            path: db/changelog/multitopic-dedicated/invoices-messages-postgres.sql
            relativeToChangelogFile: false
```

Includes the core master first (creates `shedlock`, `container_heartbeats`, and default
`messages` — the default table is harmless), then adds per-topic tables.

#### 6. `examples/app-multitopic-dedicated/src/main/resources/db/changelog/multitopic-dedicated/orders-messages-postgres.sql`

Concrete instantiation of `topic-messages-template-postgres.sql` with table name
`orders_messages` (literal replacement of `${table.name}`).

#### 7. `examples/app-multitopic-dedicated/src/main/resources/db/changelog/multitopic-dedicated/invoices-messages-postgres.sql`

Same template, table name `invoices_messages`.

### Files to Modify

| File | Change |
|---|---|
| `pom.xml` (root) | Add `<module>examples/app-multitopic-dedicated</module>` |

### Validation

- `mvn -q -DskipTests test-compile` passes.
- `mvn -T 1C -q test` passes.
- Running with seed enabled creates `orders_messages` and `invoices_messages` tables
  and persists topic traffic to the correct dedicated tables.

### Acceptance Criteria

1. `examples/app-multitopic-dedicated` compiles as part of the reactor build.
2. The app starts successfully.
3. Liquibase creates `orders_messages` and `invoices_messages` at startup.
4. Orders traffic lands exclusively in `orders_messages`.
5. Invoices traffic lands exclusively in `invoices_messages`.
6. The default `messages` table remains empty (or is ignored).
7. Worker claim, maintenance, and queue-health operations route to the correct
   per-topic table (verified by logs showing topic + table dimension).
8. `SELECT COUNT(*) FROM orders_messages;` and `SELECT COUNT(*) FROM invoices_messages;`
   show the expected row counts.
9. Per-key ordering holds within each dedicated table.

---

## T-003 — Documentation and Comparison (ROADMAP Priority 3)

### Scope

Update project documentation to cover both multi-topic examples, add a
shared-vs-dedicated comparison table, and add a verification script.

### Files to Create

#### 1. `examples/verify-multitopic.sh`

Bash script that:
1. Accepts `--mode shared|dedicated` argument.
2. Connects to Postgres (`localhost:5432`, `superduper`/`superduper`).
3. Runs SQL assertions for the chosen mode:
   - **shared:** counts per topic in `messages`, per-key ordering check.
   - **dedicated:** counts in `orders_messages` and `invoices_messages`, per-key ordering.
4. Prints PASS/FAIL for each assertion.

### Files to Modify

#### 1. `docs/EXAMPLES.md`

Add a new section **"Multi-Topic Examples"** (after the existing content) with subsections:

- **Shared-Table Mode (`app-multitopic-shared`):**
  - Run steps: `docker compose up -d`, then `mvn -pl examples/app-multitopic-shared -am spring-boot:run -Dspring-boot.run.jvmArguments="-Dsuperduper.example.seed.enabled=true"`
  - Config highlights (no `table` field).
  - Expected SQL assertions (per-topic counts in shared `messages`).
- **Dedicated-Table Mode (`app-multitopic-dedicated`):**
  - Run steps: same infrastructure, different module path.
  - Config highlights (`table: orders_messages` / `table: invoices_messages`).
  - Liquibase wiring explanation.
  - Expected SQL assertions (per-table counts).
- **Verification:** how to run `verify-multitopic.sh` for each mode.
- **SQL Assertions:** expected counts and per-key ordering queries for both modes.

Expected SQL assertions per mode (assuming `count=500` per topic, `max-retries=3`):

Shared mode:

| Query | Expected |
|---|---|
| `SELECT COUNT(*) FROM messages;` | `1000` |
| `SELECT topic, COUNT(*) FROM messages GROUP BY topic;` | `orders.events: 500`, `invoices.events: 500` |
| `SELECT COUNT(*) FROM messages WHERE status = 'PROCESSED';` | `975` |
| `SELECT COUNT(*) FROM messages WHERE status = 'STOPPED';` | `25` |

Dedicated mode:

| Query | Expected |
|---|---|
| `SELECT COUNT(*) FROM orders_messages;` | `500` |
| `SELECT COUNT(*) FROM invoices_messages;` | `500` |
| Per-table status distributions match the same ratios | |

#### 2. `docs/USAGE.md`

Add a **"Shared vs. Dedicated Table Comparison"** subsection after the existing
multi-topic configuration section (after line 88):

| Concern | Shared Table | Dedicated Table |
|---|---|---|
| Schema overhead | Single `messages` table, no extra DDL | One table per topic, Liquibase changesets needed |
| Operational simplicity | One table to monitor, backup, index | Per-topic tables to manage independently |
| Query isolation | `WHERE topic = :topic` filter | Full table-level isolation |
| Index contention | All topics share the same indexes | Each table has its own index set |
| Scaling model | Homogeneous topics, similar SLAs | Heterogeneous topics, independent retention/compliance |
| Maintenance routing | Same repository, topic predicate | Separate repository instance per table |
| Recommended when | Few topics, similar volume and latency | Many topics, varying SLAs, regulatory isolation |

Add a **"Recommended Use-Cases"** paragraph:
- Shared table: up to ~10 homogeneous topics, uniform SLAs, simpler ops.
- Dedicated tables: compliance/audit isolation, independent scaling or retention,
  very high per-topic volume where index contention matters.

#### 3. `README.md`

Add bullets under the **Examples** section:
- `examples/app-multitopic-shared` — multi-topic example with a shared `messages` table (blocking).
- `examples/app-multitopic-dedicated` — multi-topic example with per-topic tables (blocking).

Link to `docs/EXAMPLES.md#multi-topic-examples`.

### Validation

- Documentation renders correctly (no broken links).
- `verify-multitopic.sh` runs against a seeded database and reports PASS for all assertions.

### Acceptance Criteria

1. `docs/EXAMPLES.md` has run steps for both examples with SQL assertions.
2. `docs/USAGE.md` has a comparison table: shared vs. dedicated.
3. `README.md` links to both multi-topic examples.
4. `verify-multitopic.sh` passes for both `--mode shared` and `--mode dedicated` after seeding.
5. Both examples run locally with `docker compose up -d` + the app.
6. Both process at least two Kafka topics.
7. Behavior matches documented table strategy.

---

## Implementation Order

T-001 -> T-002 -> T-003 (sequential; each builds on the prior).

## Notes for Implementer

- Mirror the code style and patterns from `examples/app-blocking`.
- Use `@Component("beanName")` for handler bean naming (not `@Bean` methods),
  consistent with the existing example.
- The seeder needs to discover topic names. Simplest approach: inject the resolved
  `TopicRegistry` and iterate its Kafka topic names, or accept a property list.
- For the verification script, use `psql` with `--tuples-only` for parseable output.
- Run `mvn -q spotless:apply` after every code change.
- Run `mvn -q -DskipTests test-compile` for fast validation.
- Run `mvn -T 1C -q test` before finishing.

## Validation

- `mvn -q -DskipTests test-compile`
- `mvn -T 1C -q test`
