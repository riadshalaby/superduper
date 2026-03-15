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
| T-001 | Per-Topic Claim Locks — Verification | claude | codex | claude | ready_for_review | All per-topic lock tests pass; at least one test proves per-topic lock independence | `mvn -T 1C -q test` passed; `TopicWorkerCoordinatorTest.topicWorkerInstance_usesTopicScopedLockName` still verifies per-topic lock independence | review |
| T-002 | Batch Inserts on Ingest | claude | codex | claude | ready_for_review | Batch upsert in both blocking+reactive consumers; single-record fallback on failure; configurable max-poll-records; E2E dedup verified; offsets ack'd only after persist | `mvn -q -DskipTests test-compile` passed; `mvn -T 1C -q test` passed | review |
