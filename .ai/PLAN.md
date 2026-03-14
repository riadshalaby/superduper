# Plan — 0.5.2-SNAPSHOT

Status: **approved**

Goal: deliver robust multi-topic runtime tooling, clean dedicated-mode schema, and updated documentation for `0.5.2-SNAPSHOT`.

---

## Phase 4 — Shared-Mode Schema Template Consolidation (T-004)

### Objective

Eliminate schema duplication between shared-mode SQL files and dedicated-mode templates by making `topic-messages-template-*.sql` the single source of truth for all message table creation (both shared and dedicated modes). Delete the redundant `001-init-messages-*.sql` and `002/003-worker-claim-indexes-*.sql` files.

### Background

The T-002 review identified two duplication issues:

1. **`001-init-messages-postgres.sql`** and **`001-init-messages-mariadb.sql`** are non-parameterized copies of `topic-messages-template-postgres.sql` and `topic-messages-template-mariadb.sql`, with `messages` hardcoded where templates use `${table.name}`.
2. **`002-worker-claim-indexes-postgres.sql`** and **`003-worker-claim-indexes-mariadb.sql`** contain claim indexes that are already present in the templates. In shared mode, `idx_messages_topic_status_key_id` is created twice — once in `001-init-messages-*.sql` and again (with `IF NOT EXISTS`) in `002/003-worker-claim-indexes-*.sql`.

Since there are no existing users, changeset IDs can be freely reassigned.

### Deliverables

#### 1. Rewrite `db.changelog-master.yaml`

Replace the four message/claim-index changesets with a property declaration and two template-based changesets, following the same pattern used by `orders-messages.yaml` and `invoices-messages.yaml` in dedicated mode:

```yaml
databaseChangeLog:
  - include:
      file: db/changelog/superduper/db.changelog-infra.yaml
  - property:
      name: table.name
      value: messages
      global: false
  - changeSet:
      id: 003-init-messages-postgres
      author: superduper
      dbms: postgresql
      changes:
        - sqlFile:
            path: db/changelog/superduper/topic-messages-template-postgres.sql
            relativeToChangelogFile: false
  - changeSet:
      id: 004-init-messages-mariadb
      author: superduper
      dbms: mariadb
      changes:
        - sqlFile:
            path: db/changelog/superduper/topic-messages-template-mariadb.sql
            relativeToChangelogFile: false
```

Key changes from the current version:
- **Property `table.name=messages`** added with `global: false` (scoped to this changelog).
- **Changesets 003/004** now reference `topic-messages-template-*.sql` instead of `001-init-messages-*.sql`. The templates create the table AND all three indexes in one pass.
- **Changesets 005/006** (claim indexes) removed entirely — the templates already include all claim indexes.

#### 2. Delete four redundant SQL files

Delete from `schema-liquibase/src/main/resources/db/changelog/superduper/`:

| File | Reason |
|------|--------|
| `001-init-messages-postgres.sql` | Replaced by `topic-messages-template-postgres.sql` with `table.name=messages` |
| `001-init-messages-mariadb.sql` | Replaced by `topic-messages-template-mariadb.sql` with `table.name=messages` |
| `002-worker-claim-indexes-postgres.sql` | Claim indexes already in `topic-messages-template-postgres.sql` |
| `003-worker-claim-indexes-mariadb.sql` | Claim indexes already in `topic-messages-template-mariadb.sql` |

Note: The `DROP INDEX IF EXISTS idx_messages_message_key_id` legacy migration in the claim index files is no longer needed — there are no existing users with the old index name, and the templates create tables fresh.

#### 3. Update documentation

Three documentation files reference the deleted SQL files and must be updated.

**`README.md`** (line 128):

Replace:
```
**Indexes:** the default schema creates `messages(topic, status, message_key, id)` for topic-aware claim scans plus the processing/reclaim indexes shown in `002-worker-claim-indexes-postgres.sql` and `003-worker-claim-indexes-mariadb.sql`.
```

With:
```
**Indexes:** the default schema creates `messages(topic, status, message_key, id)` for topic-aware claim scans plus processing/reclaim indexes, all defined in `topic-messages-template-postgres.sql` and `topic-messages-template-mariadb.sql`.
```

**`docs/USAGE.md`** (lines 526–529):

Replace the claim index file references:
```
- `schema-liquibase/src/main/resources/db/changelog/superduper/002-worker-claim-indexes-postgres.sql`
- `schema-liquibase/src/main/resources/db/changelog/superduper/003-worker-claim-indexes-mariadb.sql`
```

With:
```
- `schema-liquibase/src/main/resources/db/changelog/superduper/topic-messages-template-postgres.sql`
- `schema-liquibase/src/main/resources/db/changelog/superduper/topic-messages-template-mariadb.sql`
```

**`docs/ARCHITECTURE.md`** (lines 20, 125, 128):

Update the module table (line 20) — remove `001-init-messages-postgres.sql`, `001-init-messages-mariadb.sql`, `002-worker-claim-indexes-postgres.sql`, `003-worker-claim-indexes-mariadb.sql` and add `topic-messages-template-postgres.sql`, `topic-messages-template-mariadb.sql`.

Update the Database Dialect Support table:
- "shared messages schema" row: change from `001-init-messages-*.sql` to `topic-messages-template-*.sql` (with note: `table.name=messages`)
- "claim indexes" row: change from `002/003-worker-claim-indexes-*.sql` to `topic-messages-template-*.sql` (bundled with table creation)

#### 4. No changes to templates or dedicated-mode files

The following files are **not modified**:
- `topic-messages-template-postgres.sql` — already correct
- `topic-messages-template-mariadb.sql` — already correct
- `db.changelog-infra.yaml` — unchanged
- `db.changelog-dedicated.yaml` — unchanged
- `orders-messages.yaml` — unchanged
- `invoices-messages.yaml` — unchanged

#### 5. No changes to integration tests

The existing integration tests should pass without modification:

- **`SharedMultitopicLiquibaseIntegrationTest`** — asserts `messages` table, `container_heartbeats`, `shedlock`, and all three indexes (`idx_messages_topic_status_key_id`, `idx_messages_processing_worker_key_id`, `idx_messages_processing_last_updated`). The templates create exactly these indexes when `table.name=messages`.
- **`DedicatedMultitopicLiquibaseIntegrationTest`** — unchanged (uses `db.changelog-dedicated.yaml` which is not modified).

### Implementation Notes

- The Liquibase `property` with `global: false` scopes `table.name` to the declaring changelog. This is the same mechanism used by the dedicated-mode topic YAML files.
- After consolidation, the schema-liquibase SQL file inventory is:
  - `001-init-infra-postgres.sql` — infrastructure tables (postgres)
  - `001-init-infra-mariadb.sql` — infrastructure tables (mariadb)
  - `topic-messages-template-postgres.sql` — parameterized messages table + indexes (postgres)
  - `topic-messages-template-mariadb.sql` — parameterized messages table + indexes (mariadb)
- Both shared mode (`db.changelog-master.yaml` with `table.name=messages`) and dedicated mode (per-topic YAML files with `table.name=<topic>_messages`) now use the same templates. One definition, many tables.

### Validation

- `mvn -q spotless:apply` — formatting
- `mvn -q -DskipTests test-compile` — compile check
- `mvn -T 1C -q test` — all tests including shared and dedicated Liquibase integration tests
- Verify that exactly 4 SQL files remain in `schema-liquibase/src/main/resources/db/changelog/superduper/` (2 infra + 2 templates)
- Verify `db.changelog-master.yaml` has no references to deleted files

---

## Phase 1 — Runtime Script for Multi-Topic Modes (T-001)

### Objective

Replace `examples/verify-multitopic.sh` with `examples/run-multitopic-modes.sh`, a `run-multi.sh`-style operator script that starts seeder + worker containers for shared-table or dedicated-table multi-topic modes.

### Deliverables

#### 1. `docker-compose.multitopic.yml` (new file, project root)

A compose file modelled on `docker-compose.multi.yml` with these differences:

- **kafka-init** creates topics `orders.events` and `invoices.events` (not `superduper.example`).
- **No separate seeder services.** The multi-topic examples embed their seeders (`MultitopicSeeder`) in the worker app, triggered by `SUPERDUPER_EXAMPLE_SEED_ENABLED=true`.
- **Workers 1–5** build from `${SUPERDUPER_WORKER_CONTEXT}` (shared or dedicated app).
  - `worker-1`: `SPRING_LIQUIBASE_ENABLED=true`, `SUPERDUPER_EXAMPLE_SEED_ENABLED=true`, `SUPERDUPER_EXAMPLE_SEED_COUNT=${SUPERDUPER_SEEDER_COUNT:-500}`.
  - `worker-2..5`: `SPRING_LIQUIBASE_ENABLED=false`, `SUPERDUPER_EXAMPLE_SEED_ENABLED=false`.
- **All workers** receive the same DB and Kafka env vars as `docker-compose.multi.yml`.
- **Prometheus + Grafana** reuse the same observability config.
- **kafka-ui + adminer** included as before.

#### 2. `examples/app-multitopic-shared/Dockerfile` (new file)

Identical to `examples/app-blocking/Dockerfile`:

```dockerfile
FROM eclipse-temurin:25-jre-alpine
COPY target/*.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

#### 3. `examples/app-multitopic-dedicated/Dockerfile` (new file)

Same content as above.

#### 4. `examples/run-multitopic-modes.sh` (new file)

Shell script following `run-multi.sh` patterns:

- **Commands:** `start`, `stop`, `down [--volumes]`
- **Required flag:** `--mode shared|dedicated`
- **Optional flags:**
  - `--count N` (1–5, default `1`) — number of worker instances
  - `--seeder-count N` (default `500`) — messages per topic from the embedded seeder
- **Mode mapping:**
  - `shared` → `SUPERDUPER_WORKER_CONTEXT=./examples/app-multitopic-shared`
  - `dedicated` → `SUPERDUPER_WORKER_CONTEXT=./examples/app-multitopic-dedicated`
- **Pre-start:** `mvn -DskipTests -q package`
- **Dynamic services:** enable/disable workers based on `--count`, clean up disabled services.
- **Runtime guidance:** print URLs (Kafka UI, Adminer, Prometheus, Grafana), log-follow commands, and mode-specific SQL queries for verifying outcomes.

#### 5. Delete `examples/verify-multitopic.sh`

The new run script replaces it. The SQL assertions remain documented in `docs/EXAMPLES.md`.

### Implementation Notes

- The script must use `docker-compose.multitopic.yml` (not the existing multi file).
- Worker-1 runs both Liquibase migration and the embedded seeder. Other workers wait for Postgres health before starting.
- The seeder produces `count` messages per topic (2 topics), so `--seeder-count 500` = 1000 total messages.
- Follow the same argument parsing, validation, and error handling style as `run-multi.sh`.

---

## Phase 2 — Dedicated-Mode Schema Cleanup (T-002)

### Objective

Split the Liquibase changelogs so that dedicated mode creates only infrastructure tables + per-topic tables, without an unused `messages` table.

### Approach: Split Changelogs (Option A)

Since there are no existing users, changeset IDs can be freely reassigned.

### Deliverables

#### 1. New SQL files in `schema-liquibase/src/main/resources/db/changelog/superduper/`

**`001-init-infra-postgres.sql`** — extracted from `001-init-schema-postgres.sql`:

```sql
CREATE TABLE IF NOT EXISTS container_heartbeats (
  container_id VARCHAR(255) PRIMARY KEY,
  last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shedlock (
  name VARCHAR(64) PRIMARY KEY,
  lock_until TIMESTAMP(3) NOT NULL,
  locked_at TIMESTAMP(3) NOT NULL,
  locked_by VARCHAR(255) NOT NULL
);
```

**`001-init-infra-mariadb.sql`** — extracted from `001-init-schema-mariadb.sql`:

```sql
CREATE TABLE IF NOT EXISTS container_heartbeats (
  container_id VARCHAR(255) PRIMARY KEY,
  last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shedlock (
  name VARCHAR(64) PRIMARY KEY,
  lock_until TIMESTAMP(3) NOT NULL,
  locked_at TIMESTAMP(3) NOT NULL,
  locked_by VARCHAR(255) NOT NULL
);
```

**`001-init-messages-postgres.sql`** — messages table extracted from `001-init-schema-postgres.sql`:

```sql
CREATE TABLE IF NOT EXISTS messages (
  id BIGSERIAL PRIMARY KEY,
  topic VARCHAR(255) NOT NULL DEFAULT 'default',
  message_id VARCHAR(36) UNIQUE NOT NULL,
  message_key VARCHAR(255) NOT NULL,
  content TEXT,
  status VARCHAR(32) NOT NULL CHECK (status IN ('READY','PROCESSING','PROCESSED','FAILED','STOPPED')),
  retry_count INT DEFAULT 0,
  container_id VARCHAR(255),
  correlation_id VARCHAR(36) NULL,
  message_type VARCHAR(255) NULL,
  occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  processed_at TIMESTAMP NULL,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_topic_status_key_id ON messages(topic, status, message_key, id);
```

**`001-init-messages-mariadb.sql`** — messages table extracted from `001-init-schema-mariadb.sql`:

```sql
CREATE TABLE IF NOT EXISTS messages (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  topic VARCHAR(255) NOT NULL DEFAULT 'default',
  message_id VARCHAR(36) UNIQUE NOT NULL,
  message_key VARCHAR(255) NOT NULL,
  content TEXT,
  status VARCHAR(32) NOT NULL,
  retry_count INT DEFAULT 0,
  container_id VARCHAR(255),
  correlation_id VARCHAR(36) NULL,
  message_type VARCHAR(255) NULL,
  occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  processed_at TIMESTAMP NULL,
  last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE INDEX idx_messages_topic_status_key_id ON messages(topic, status, message_key, id);
```

#### 2. Delete old combined init files

- Delete `001-init-schema-postgres.sql`
- Delete `001-init-schema-mariadb.sql`

#### 3. New YAML changelog: `db.changelog-infra.yaml`

In `schema-liquibase/src/main/resources/db/changelog/superduper/`:

```yaml
databaseChangeLog:
  - changeSet:
      id: 001-init-infra-postgres
      author: superduper
      dbms: postgresql
      changes:
        - sqlFile:
            path: db/changelog/superduper/001-init-infra-postgres.sql
            relativeToChangelogFile: false
  - changeSet:
      id: 002-init-infra-mariadb
      author: superduper
      dbms: mariadb
      changes:
        - sqlFile:
            path: db/changelog/superduper/001-init-infra-mariadb.sql
            relativeToChangelogFile: false
```

#### 4. Restructure `db.changelog-master.yaml`

Used by single-topic and shared multi-topic modes. Now includes infra + messages:

```yaml
databaseChangeLog:
  - include:
      file: db/changelog/superduper/db.changelog-infra.yaml
  - changeSet:
      id: 003-init-messages-postgres
      author: superduper
      dbms: postgresql
      changes:
        - sqlFile:
            path: db/changelog/superduper/001-init-messages-postgres.sql
            relativeToChangelogFile: false
  - changeSet:
      id: 004-init-messages-mariadb
      author: superduper
      dbms: mariadb
      changes:
        - sqlFile:
            path: db/changelog/superduper/001-init-messages-mariadb.sql
            relativeToChangelogFile: false
  - changeSet:
      id: 005-worker-claim-indexes-postgres
      author: superduper
      dbms: postgresql
      changes:
        - sqlFile:
            path: db/changelog/superduper/002-worker-claim-indexes-postgres.sql
            relativeToChangelogFile: false
  - changeSet:
      id: 006-worker-claim-indexes-mariadb
      author: superduper
      dbms: mariadb
      changes:
        - sqlFile:
            path: db/changelog/superduper/003-worker-claim-indexes-mariadb.sql
            relativeToChangelogFile: false
```

#### 5. Update `db.changelog-dedicated.yaml` (in `examples/app-multitopic-dedicated/`)

Switch from including `db.changelog-master.yaml` to including only `db.changelog-infra.yaml`:

```yaml
databaseChangeLog:
  - include:
      file: db/changelog/superduper/db.changelog-infra.yaml
  - include:
      file: db/changelog/multitopic-dedicated/orders-messages.yaml
      relativeToChangelogFile: false
  - include:
      file: db/changelog/multitopic-dedicated/invoices-messages.yaml
      relativeToChangelogFile: false
```

#### 6. Validation

- Shared mode: Liquibase creates `messages`, `container_heartbeats`, `shedlock` + claim indexes.
- Dedicated mode: Liquibase creates `orders_messages`, `invoices_messages`, `container_heartbeats`, `shedlock`. No `messages` table.
- Single-topic mode (blocking/reactive examples): unchanged — uses `db.changelog-master.yaml`.
- All existing tests must pass: `mvn -T 1C -q test`.

---

## Phase 3 — Documentation and Release Readiness (T-003)

### Objective

Update docs to reflect the new run script, the split schema strategy, and define release acceptance criteria.

### Deliverables

#### 1. Update `docs/EXAMPLES.md`

- Replace the **Verification** section (lines 320–329) that references `verify-multitopic.sh` with instructions for the new `run-multitopic-modes.sh` script.
- Add a **Running Multi-Topic Modes (Containers)** section after the existing shared/dedicated sections, showing:
  ```bash
  ./examples/run-multitopic-modes.sh start --mode shared
  ./examples/run-multitopic-modes.sh start --mode dedicated --count 2 --seeder-count 1000
  ./examples/run-multitopic-modes.sh stop
  ./examples/run-multitopic-modes.sh down --volumes
  ```
- Update the dedicated-mode assertion `SELECT COUNT(*) FROM messages WHERE topic IN (...) = 0` to note that in dedicated mode the `messages` table is not created at all.

#### 2. Update `docs/USAGE.md`

- In the **Shared vs. Dedicated Table Comparison** table, add a row for "Schema outcome":
  - Shared: `messages` + `container_heartbeats` + `shedlock`
  - Dedicated: `<topic>_messages` per topic + `container_heartbeats` + `shedlock` (no `messages` table)
- In the multi-topic configuration section, document that dedicated mode uses `db.changelog-infra.yaml` instead of `db.changelog-master.yaml`.

#### 3. Update `README.md`

- Add a quickstart snippet for multi-topic container mode referencing `run-multitopic-modes.sh`.
- Briefly note the schema difference between shared and dedicated modes.

#### 4. Release acceptance criteria

Document in this plan (not in code):

1. `./examples/run-multitopic-modes.sh start --mode shared` starts and the embedded seeder completes. SQL assertions from `docs/EXAMPLES.md` pass manually.
2. `./examples/run-multitopic-modes.sh start --mode dedicated` starts and the embedded seeder completes. Dedicated SQL assertions pass. `messages` table does NOT exist.
3. `mvn -T 1C -q test` passes.
4. `mvn -q -DskipTests test-compile` passes.
5. Docs in `README.md`, `docs/USAGE.md`, and `docs/EXAMPLES.md` are updated and accurate.

---

## Task Dependency Order

```
T-002 (schema cleanup)  ──┐
                           ├──> T-003 (docs)
T-001 (runtime script)  ──┘
```

T-001 and T-002 are independent and can be implemented in parallel. T-003 depends on both.

---

## Validation

- `mvn -q -DskipTests test-compile`
- `mvn -T 1C -q test`
- Manual: `./examples/run-multitopic-modes.sh start --mode shared` → verify SQL assertions
- Manual: `./examples/run-multitopic-modes.sh start --mode dedicated` → verify SQL assertions + no `messages` table
