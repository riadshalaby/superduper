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
| T-001 | Release Trigger Consistency + Release Notes: CI sole tag creator, remove tag push from finalize, add GitHub Release with auto-generated notes | claude | codex | claude | done | finalize verifies tag (no create/push); CI release job creates GitHub Release with notes; release permissions contents:write; PR body reflects automated flow; CLAUDE.md updated; deterministic sequence enforced | `mvn -q spotless:apply`; `mvn -q -DskipTests test-compile`; `mvn -T 1C -q test`; `bash -n scripts/ai-release.sh`; `git diff --check`; no Surefire failure markers found; all 20 plan compliance items verified PASS | — |
