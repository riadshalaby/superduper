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
| T-001 | Runtime script for multi-topic modes: docker-compose.multitopic.yml, Dockerfiles, run-multitopic-modes.sh, delete verify-multitopic.sh | claude | codex | claude | ready_for_implement | (1) `run-multitopic-modes.sh start --mode shared` starts workers+embedded seeder, seeder completes (2) `run-multitopic-modes.sh start --mode dedicated` starts workers+embedded seeder, seeder completes (3) `--count N` scales workers 1–5 (4) `stop` and `down --volumes` work (5) verify-multitopic.sh deleted | n/a | implement |
| T-002 | Dedicated-mode schema cleanup: split Liquibase changelogs so dedicated mode has no unused messages table | claude | codex | claude | ready_for_implement | (1) shared mode creates messages + container_heartbeats + shedlock + claim indexes (2) dedicated mode creates orders_messages + invoices_messages + container_heartbeats + shedlock only — no messages table (3) single-topic examples unchanged (4) `mvn -T 1C -q test` passes | n/a | implement |
| T-003 | Documentation and release readiness: update EXAMPLES.md, USAGE.md, README.md for new run script and schema strategy | claude | codex | claude | ready_for_implement | (1) EXAMPLES.md references run-multitopic-modes.sh instead of verify-multitopic.sh (2) USAGE.md documents schema outcomes for shared vs dedicated (3) README.md has multi-topic quickstart snippet (4) dedicated-mode SQL assertions updated to reflect no messages table | n/a | implement |
