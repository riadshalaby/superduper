# Review — T-001 / T-002 / T-003 (v0.5.1-SNAPSHOT multitopic examples)

Status: **complete**

Review Round: **2** (updated after user-prompted re-examination)

Reviewed: 2026-03-11

Commit reviewed: `dac89e0` `feat(examples): add multitopic example apps`

## Verdict

`PASS_WITH_NOTES`

All acceptance criteria are functionally met. Three design-quality issues were identified
(two missed in round 1, one carried over). None are runtime-blocking, but two
(DDL duplication, unused table) affect the quality of the example as a reference pattern
for users.

---

## Findings (ordered by severity)

### [NOTE-1] Dedicated example DDL duplicates the template instead of using it (severity: design)

**Files:**
- `examples/app-multitopic-dedicated/src/main/resources/db/changelog/multitopic-dedicated/orders-messages-postgres.sql`
- `examples/app-multitopic-dedicated/src/main/resources/db/changelog/multitopic-dedicated/invoices-messages-postgres.sql`
- `schema-liquibase/src/main/resources/db/changelog/superduper/topic-messages-template-postgres.sql` (unused)

**Finding:** `schema-liquibase` ships `topic-messages-template-postgres.sql` with a
`${table.name}` Liquibase property-substitution placeholder, intended for reuse by
applications that need per-topic tables. The dedicated example ignores this template and
instead ships two verbatim copies of the DDL with table names manually baked in.

The plan said _"Concrete instantiation of `topic-messages-template-postgres.sql` with table
name `orders_messages` (literal replacement of `${table.name}`)"_ — but the intended
mechanism is Liquibase property substitution referencing the single template file, not
forking the DDL into separate files.

**Consequences:**
- The template is on the classpath (via the `superduper-schema-liquibase` dependency) but
  never referenced — users following this example will not learn the reuse pattern.
- Any future schema change to `topic-messages-template-postgres.sql` won't propagate to
  the dedicated example's tables without a manual sync.
- MariaDB support is silently missing: there is no `orders-messages-mariadb.sql` /
  `invoices-messages-mariadb.sql`, and the dedicated changelog has no `dbms: mariadb`
  changeset at all, whereas the template provides a MariaDB variant.

**Required fix:** No — acceptance criteria are met. Recommended: replace the two
verbatim SQL files with Liquibase property-substitution changesets that reference the
shared template file.

---

### [NOTE-2] Dedicated example creates an unused `messages` table (severity: design)

**File:**
- `examples/app-multitopic-dedicated/src/main/resources/db/changelog/multitopic-dedicated/db.changelog-dedicated.yaml`
- `schema-liquibase/src/main/resources/db/changelog/superduper/001-init-schema-postgres.sql`

**Finding:** `001-init-schema-postgres.sql` bundles `messages`, `container_heartbeats`,
and `shedlock` into a single SQL file. Because the dedicated changelog includes the core
master first, all three tables are created. `container_heartbeats` and `shedlock` are
necessary, but `messages` is never written to in dedicated mode — yet it is created,
indexed, and left empty.

The plan acknowledged this with _"the default table is harmless"_, and AC-6 only requires
it to _remain empty (or be ignored)_, which is met. However it is misleading as a reference
example: users may wonder why the unused table exists.

**Root cause:** The `001-init-schema-postgres.sql` file conflates three independent
concerns (message queue, heartbeats, locking) into one changeset, making selective
inclusion impossible without modifying the core schema module.

**Required fix:** No — acceptance criteria are met. Worth noting as a future schema
refactor opportunity (split `001-init` into separate changesets per table).

---

### [NOTE-3] README.md stale copy text (severity: cosmetic, carried from round 1)

**File:** `README.md`, line 257
**Finding:** `"Two runnable apps are included:"` says "Two" but the list now has four items.
**Required fix:** No — cosmetic only.

---

## Per-Task Acceptance Criteria

### T-001 — Shared-Table Multi-Topic Example

| # | Criterion | Status |
|---|---|---|
| 1 | `examples/app-multitopic-shared` compiles in reactor build | PASS |
| 2 | `SharedMultitopicApplication` `@SpringBootApplication` | PASS |
| 3 | `OrdersMessageHandler` + `InvoicesMessageHandler` — correct bean names, correct failure patterns | PASS |
| 4 | `SeedProgress` latch-based tracking | PASS |
| 5 | `MultitopicSeeder` seeds both topics, waits for handler completion | PASS |
| 6 | `MultitopicOperatorEndpoints` — all four endpoints present | PASS |
| 7 | `application.yml` — no `table` field on either topic entry | PASS |
| 8 | Root `pom.xml` includes module | PASS |

### T-002 — Dedicated-Table Multi-Topic Example

| # | Criterion | Status |
|---|---|---|
| 1 | `examples/app-multitopic-dedicated` compiles in reactor build | PASS |
| 2 | `DedicatedMultitopicApplication` `@SpringBootApplication` | PASS |
| 3 | Handler + support class set correct, package `dedicated` | PASS |
| 4 | `application.yml` — dedicated changelog, correct `table:` entries, correct `group-id` | PASS |
| 5 | `db.changelog-dedicated.yaml` — includes core master, adds per-topic changesets | PASS |
| 6 | `orders-messages-postgres.sql` / `invoices-messages-postgres.sql` — correct tables and indexes | PASS |
| 7 | Root `pom.xml` includes module | PASS |
| AC-6 | Default `messages` table remains empty (or is ignored) | PASS — but see NOTE-2: table is created unused |
| template reuse | Template `topic-messages-template-postgres.sql` used via Liquibase substitution | NOTE-1 — DDL duplicated instead |

### T-003 — Documentation and Comparison

| # | Criterion | Status |
|---|---|---|
| 1 | `docs/EXAMPLES.md` has Multi-Topic Examples section with run steps, assertions, ordering queries | PASS |
| 2 | `docs/USAGE.md` has comparison table and use-case paragraph | PASS |
| 3 | `README.md` bullets for both examples, link to multi-topic docs | PASS |
| 4 | `verify-multitopic.sh` — both modes, `psql --tuples-only`, row counts and ordering asserted | PASS |

---

## Required Fixes

None. All findings are notes/recommendations, not blocking defects.
