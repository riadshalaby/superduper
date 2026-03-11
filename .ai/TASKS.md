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
| T-001 | Shared-table multi-topic example (`examples/app-multitopic-shared`) | claude | codex | claude | ready_for_review | App compiles, starts, consumes 2 Kafka topics into shared `messages` table, per-topic claim loops run, seeder seeds both topics, SQL assertions pass | `mvn -q -DskipTests test-compile` PASS; `mvn -T 1C -q test` PASS; follow-up fixes applied for review notes | review |
| T-002 | Dedicated-table multi-topic example (`examples/app-multitopic-dedicated`) | claude | codex | claude | ready_for_review | App compiles, starts, Liquibase creates `orders_messages` + `invoices_messages`, traffic lands in correct tables, SQL assertions pass | `mvn -q -DskipTests test-compile` PASS; `mvn -T 1C -q test` PASS; follow-up fixes applied for review notes | review |
| T-003 | Documentation and comparison (USAGE.md, EXAMPLES.md, README.md, verify script) | claude | codex | claude | ready_for_review | docs/EXAMPLES.md has run steps for both, docs/USAGE.md has comparison table, README links to examples, verify script passes for both modes | `mvn -q -DskipTests test-compile` PASS; `mvn -T 1C -q test` PASS; follow-up fixes applied for review notes | review |
