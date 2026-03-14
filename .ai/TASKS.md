# TASKS

Use this board to coordinate manual handoff between planner, implementer, and reviewer.

Status values:
- `todo`
- `in_planning`
- `ready_for_implement`
- `in_implementation`
- `ready_for_review`
- `in_review`
- `changes_requested`
- `done`
- `blocked`

| Task ID | Scope | Planner Agent | Implementer Agent | Reviewer Agent | Status | Acceptance Criteria | Evidence | Next Role |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| T-001 | Runtime script for multi-topic modes: docker-compose.multitopic.yml, Dockerfiles, run-multitopic-modes.sh, delete verify-multitopic.sh | claude | codex | claude | done | (1) `run-multitopic-modes.sh start --mode shared` starts workers+embedded seeder, seeder completes (2) `run-multitopic-modes.sh start --mode dedicated` starts workers+embedded seeder, seeder completes (3) `--count N` scales workers 1–5 (4) `stop` and `down --volumes` work (5) verify-multitopic.sh deleted | Shared and dedicated `run-multitopic-modes.sh start` manual validations passed; `down --volumes` exercised after each run; helper script deleted. | — |
| T-002 | Dedicated-mode schema cleanup: split Liquibase changelogs so dedicated mode has no unused messages table | claude | codex | claude | done | (1) shared mode creates messages + container_heartbeats + shedlock + claim indexes (2) dedicated mode creates orders_messages + invoices_messages + container_heartbeats + shedlock only — no messages table (3) single-topic examples unchanged (4) `mvn -T 1C -q test` passes | `mvn -q -DskipTests test-compile` and `mvn -T 1C -q test` passed; shared SQL counts reached `1000 / 975 / 25 / 0`; dedicated SQL counts reached `500 / 500 / null / 975 / 25 / 0`. | — |
| T-003 | Documentation and release readiness: update EXAMPLES.md, USAGE.md, README.md for new run script and schema strategy | claude | codex | claude | done | (1) EXAMPLES.md references run-multitopic-modes.sh instead of verify-multitopic.sh (2) USAGE.md documents schema outcomes for shared vs dedicated (3) README.md has multi-topic quickstart snippet (4) dedicated-mode SQL assertions updated to reflect no messages table | README, EXAMPLES, USAGE updated to match the new runtime script and infra-only dedicated schema; validation evidence captured in implementer handoff. | — |
| T-004 | Consolidate shared-mode schema: reuse per-topic SQL templates for the shared `messages` table and fold claim indexes into the template | claude | codex | — | ready_for_review | (1) `001-init-messages-postgres.sql` and `001-init-messages-mariadb.sql` deleted (2) `db.changelog-master.yaml` uses `topic-messages-template-*.sql` with `table.name=messages` for the shared messages table (3) `002-worker-claim-indexes-postgres.sql` and `003-worker-claim-indexes-mariadb.sql` deleted (4) claim indexes folded into `topic-messages-template-*.sql` (already the case for dedicated mode) (5) no redundant `idx_messages_topic_status_key_id` double-creation (6) `mvn -T 1C -q test` passes (7) both shared and dedicated integration tests pass | `mvn -q spotless:apply`, `mvn -q -DskipTests test-compile`, targeted MariaDB repository tests, and `mvn -T 1C -q test` passed; `db.changelog-master.yaml` no longer references deleted files; only 4 SQL files remain in `schema-liquibase/.../superduper`. | review |
