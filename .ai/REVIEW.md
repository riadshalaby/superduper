# Review — 0.5.2-SNAPSHOT

Status: **complete**

Review Round: **3**

Reviewed: 2026-03-14

---

## Verdict

`PASS`

All five tasks (T-001, T-002, T-003, T-004, T-005) meet their acceptance criteria. No required fixes.

---

## T-001 — Runtime Script for Multi-Topic Modes

**Verdict: PASS** (Round 1)

Commit: `08f48fb` — `feat: add multitopic runtime tooling and schema split`

### Acceptance Criteria Validation

| # | Criterion | Status |
|---|-----------|--------|
| 1 | `run-multitopic-modes.sh start --mode shared` starts workers + embedded seeder, seeder completes | PASS — confirmed in handoff evidence |
| 2 | `run-multitopic-modes.sh start --mode dedicated` starts workers + embedded seeder, seeder completes | PASS — confirmed in handoff evidence |
| 3 | `--count N` scales workers 1–5 | PASS — script validates 1–5 range, dynamically enables/disables workers |
| 4 | `stop` and `down --volumes` work | PASS — confirmed in handoff evidence |
| 5 | `verify-multitopic.sh` deleted | PASS — file removed in commit |

### Findings (informational)

1. **docker-compose.multitopic.yml** — Correctly modelled on `docker-compose.multi.yml`. Kafka-init creates `orders.events` and `invoices.events` with 3 partitions. No separate seeder services; worker-1 embeds the seeder via `SUPERDUPER_EXAMPLE_SEED_ENABLED`. Workers 2–5 have Liquibase and seeding disabled. Observability (Prometheus, Grafana) reused from multi config.
2. **run-multitopic-modes.sh** — Implements `start`, `stop`, `down [--volumes]` commands. `--mode shared|dedicated` correctly maps to the right worker context and group-id. `--count N` (1–5) and `--seeder-count N` optional flags present with validation. Pre-start runs `mvn -DskipTests -q package`. Mode-specific SQL verification queries printed on start.
3. **Dockerfiles** — Both `app-multitopic-shared/Dockerfile` and `app-multitopic-dedicated/Dockerfile` are identical to `app-blocking/Dockerfile` (`eclipse-temurin:25-jre-alpine`).
4. **pom.xml files** — Both example apps have correct dependencies: `superduper-schema-liquibase`, `superduper-starter-autoselect`, `superduper-consumer-kafka-blocking`, Testcontainers, and schema-liquibase test-jar. Spring Boot repackage plugin configured.

---

## T-002 — Dedicated-Mode Schema Cleanup

**Verdict: PASS** (Round 1)

Commit: `08f48fb` — `feat: add multitopic runtime tooling and schema split`

### Acceptance Criteria Validation

| # | Criterion | Status |
|---|-----------|--------|
| 1 | Shared mode creates messages + container_heartbeats + shedlock + claim indexes | PASS — `db.changelog-master.yaml` includes infra + messages + claim indexes |
| 2 | Dedicated mode creates orders_messages + invoices_messages + container_heartbeats + shedlock only — no messages table | PASS — `db.changelog-dedicated.yaml` includes only infra + per-topic changelogs |
| 3 | Single-topic examples unchanged | PASS — `db.changelog-master.yaml` still serves as the default |
| 4 | `mvn -T 1C -q test` passes | PASS — confirmed in handoff evidence |

### Findings (informational)

1. **Schema split executed correctly.** Old combined `001-init-schema-postgres.sql` / `001-init-schema-mariadb.sql` renamed to `001-init-messages-*.sql` (git detects as rename with ~66–69% similarity). New `001-init-infra-*.sql` files extracted with only `container_heartbeats` and `shedlock` tables.
2. **db.changelog-infra.yaml** — New file with changeset IDs `001` (postgres) and `002` (mariadb), exactly matching the plan.
3. **db.changelog-master.yaml** — Restructured to include `db.changelog-infra.yaml` first, then messages changesets (`003`/`004`) and claim index changesets (`005`/`006`). Changeset IDs match the plan exactly.
4. **db.changelog-dedicated.yaml** — Correctly switched from including `db.changelog-master.yaml` to including only `db.changelog-infra.yaml`, then the per-topic changelogs. This ensures no `messages` table in dedicated mode.
5. **Integration tests added (bonus, not in plan):**
   - `DedicatedMultitopicLiquibaseIntegrationTest` — asserts `container_heartbeats`, `shedlock`, `orders_messages`, `invoices_messages` exist and `messages` does NOT exist.
   - `SharedMultitopicLiquibaseIntegrationTest` — asserts `messages`, `container_heartbeats`, `shedlock` exist and all three claim indexes are present.
   - Both use Testcontainers with PostgreSQL 16 Alpine.
6. **LiquibaseTestSupport** — Overloaded `migrate()` method added to accept a custom changelog path. Clean separation of default and dedicated changelog testing.

---

## T-003 — Documentation and Release Readiness

**Verdict: PASS** (Round 1)

Commit: `08f48fb` — `feat: add multitopic runtime tooling and schema split`

### Acceptance Criteria Validation

| # | Criterion | Status |
|---|-----------|--------|
| 1 | EXAMPLES.md references run-multitopic-modes.sh instead of verify-multitopic.sh | PASS |
| 2 | USAGE.md documents schema outcomes for shared vs dedicated | PASS — "Schema outcome" row added to comparison table |
| 3 | README.md has multi-topic quickstart snippet | PASS — container quickstart with run-multitopic-modes.sh commands |
| 4 | Dedicated-mode SQL assertions updated to reflect no messages table | PASS — `SELECT to_regclass('public.messages')` returns null |

### Findings (informational)

1. **EXAMPLES.md** — "Running Multi-Topic Modes (Containers)" section added with script usage examples. Dedicated-mode verification updated to check that `messages` table does not exist at all (via `to_regclass`).
2. **USAGE.md** — Comparison table includes "Schema outcome" row. Dedicated mode section documents use of `db.changelog-infra.yaml`.
3. **README.md** — Multi-topic configuration section added with YAML config example, schema split explanation, and container quickstart snippet. Examples section updated to list all four apps.
4. **ARCHITECTURE.md** — Updated schema-liquibase module description and dialect support table to reference the new split file names. This was not explicitly required by T-003 but is a welcome consistency improvement.

---

## T-004 — Schema Template Consolidation

**Verdict: PASS** (Round 2)

Commit: `72a0338` — `fix: consolidate shared schema templates`

### Acceptance Criteria Validation

| # | Criterion | Status |
|---|-----------|--------|
| 1 | `001-init-messages-postgres.sql` and `001-init-messages-mariadb.sql` deleted | PASS — both files removed |
| 2 | `db.changelog-master.yaml` uses `topic-messages-template-*.sql` with `table.name=messages` | PASS — property set with `global: false`, changesets 003/004 reference templates |
| 3 | `002-worker-claim-indexes-postgres.sql` and `003-worker-claim-indexes-mariadb.sql` deleted | PASS — both files removed |
| 4 | Claim indexes folded into `topic-messages-template-*.sql` | PASS — already bundled in templates, no separate files needed |
| 5 | No redundant `idx_messages_topic_status_key_id` double-creation | PASS — index created once per template invocation only |
| 6 | `mvn -T 1C -q test` passes | PASS — confirmed in handoff evidence |
| 7 | Both shared and dedicated integration tests pass | PASS — confirmed in handoff evidence |

### Findings (informational)

1. **db.changelog-master.yaml** — Reduced from 35 lines (6 changesets) to 24 lines (infra include + property + 2 changesets). Sets `table.name=messages` with `global: false` so it doesn't leak into included changelogs. Changesets 003/004 reference the same templates used by dedicated mode — single source of truth achieved.
2. **MariaDB template fix** — The processing worker index was adjusted from `(topic, status, container_id, message_key, id)` to `(status, container_id(191), topic(191), message_key(191), id)` with column prefix lengths. This matches the index shape already exercised by the MariaDB repository tests and avoids key-length violations. Good catch during validation.
3. **File inventory clean** — `schema-liquibase/.../superduper/` now contains exactly 4 SQL files (`001-init-infra-postgres.sql`, `001-init-infra-mariadb.sql`, `topic-messages-template-postgres.sql`, `topic-messages-template-mariadb.sql`) + 2 YAML changelogs (`db.changelog-infra.yaml`, `db.changelog-master.yaml`). No redundancy.
4. **Documentation updated** — README.md indexes reference, ARCHITECTURE.md module table and dialect support table, and USAGE.md index reference all point to the template files now. No stale references to deleted files in production code or documentation.
5. **Dedicated mode untouched** — `db.changelog-dedicated.yaml` and per-topic changelogs (`orders-messages.yaml`, `invoices-messages.yaml`) were not modified, confirming no regression.

---

## T-005 — Narrow `message_key` and Remove MariaDB Index Prefix Limits

**Verdict: PASS** (Round 3)

Commit: `f678f72` — `fix: narrow message key columns`

### Acceptance Criteria Validation

| # | Criterion | Status |
|---|-----------|--------|
| 1 | `message_key` changed from VARCHAR(255) to VARCHAR(36) in both SQL templates | PASS — both `topic-messages-template-postgres.sql` and `topic-messages-template-mariadb.sql` updated |
| 2 | All `(191)` prefix limits removed from `topic-messages-template-mariadb.sql` | PASS — `grep "(191)"` returns zero matches across all `.sql` and `.java` files |
| 3 | MariaDB index key total remains under 3072 bytes | PASS — calculated 2,320 bytes (128 + 1020 + 1020 + 144 + 8) |
| 4 | README.md and docs updated if they reference `message_key` sizing | PASS — README schema diagram updated to `VARCHAR(36)` |
| 5 | `mvn -T 1C -q test` passes | PASS — confirmed in handoff evidence |
| 6 | Both shared and dedicated integration tests pass | PASS — confirmed in handoff evidence |

### Findings (informational)

1. **PostgreSQL template** — `message_key` narrowed to `VARCHAR(36)`. No other changes needed since PostgreSQL uses partial indexes with no key-length pressure.
2. **MariaDB template** — `message_key` narrowed to `VARCHAR(36)` and all three `(191)` prefix limits removed. The processing worker index is now `(status, container_id, topic, message_key, id)` — full-column indexing with no truncation risk for any column.
3. **Repository integration tests (4 files)** — All inline schema DDL updated consistently: `message_key VARCHAR(36)` in all four test classes, and `(191)` prefixes removed from both MariaDB test classes.
4. **No stale references** — `grep` confirms zero `message_key VARCHAR(255)` in production code, SQL templates, tests, or documentation. Only `.ai/PLAN.md` historical text contains the old width (expected).
5. **Index key math verified:**
   - `status` VARCHAR(32) × 4 = 128 bytes
   - `container_id` VARCHAR(255) × 4 = 1,020 bytes
   - `topic` VARCHAR(255) × 4 = 1,020 bytes
   - `message_key` VARCHAR(36) × 4 = 144 bytes
   - `id` BIGINT = 8 bytes
   - **Total: 2,320 bytes** — 752 bytes of headroom under the 3,072-byte InnoDB limit.

---

## Architecture Compliance (CLAUDE.md)

| Rule | Status |
|------|--------|
| Schema migrations centralized in `schema-liquibase` | PASS — all SQL and changelog YAML in schema-liquibase |
| Workers/consumers use repository ports (no direct SQL in service classes) | PASS — no service-layer changes |
| Documentation in `docs/` except README, ROADMAP, CLAUDE | PASS |
| English for code comments, log/output messages | PASS |
| Conventional Commit messages | PASS — `feat:` (08f48fb), `fix:` (72a0338), `fix:` (f678f72) |
| Spotless formatting applied | PASS — confirmed in all handoffs |
| Tests pass | PASS — `mvn -T 1C -q test` confirmed for all three commits |

---

## Open Questions

None.
