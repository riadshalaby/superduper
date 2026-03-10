# HANDOFF

Append-only role handoff log. Each role adds one entry when its step is complete.

## Entry Template

- Timestamp (UTC):
- Task ID:
- Role: planner | implementer | reviewer
- Agent: claude | codex
- User start confirmation: yes
- Summary:
- Files changed:
- Validation:
- Commit: hash + Conventional Commit message (implementer only)
- Verdict: PASS | PASS_WITH_NOTES | FAIL (reviewer only)
- Next role:
