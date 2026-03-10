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
| T-500 | v0.5.0 multi-topic implementation follow-up after review round 2 | claude | codex | claude | done | Review notes from round 2 are triaged and addressed (or explicitly accepted) by implementer | `.ai/REVIEW.md` shows Review Round 3 completed on 2026-03-10 with `PASS`; all 5 Round-2 open items resolved; full Maven test passes confirmed by implementer | — |
