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
| T-001 | Unified CI Pipeline Consolidation: non-blocking Sonar, merge release.yml into ci.yml, strict job execution order, delete release.yml | claude | codex | TBD | ready_for_review | Sonar non-blocking on main; ci.yml has build+sonar+tag-version+release jobs; release.yml deleted; tag-version skips snapshots; release gated by is_release; ai-release.sh + CLAUDE.md updated | `mvn -q spotless:apply`; `mvn -q -DskipTests test-compile`; `mvn -T 1C -q test`; `bash -n scripts/ai-release.sh`; `git diff --check` | review |
