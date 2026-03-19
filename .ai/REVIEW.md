# Review — T-011, T-012, T-013, T-014: Development Cycle Bootstrap

Reviewer: claude
Date: 2026-03-19T16:15Z
Commit: `322c1aa` — `chore(cycle): add bootstrap workflow`

## Verdict: PASS_WITH_NOTES

## Task Review Summary

| Task | Scope | Verdict |
|---|---|---|
| T-011 | Create `scripts/ai-start-cycle.sh` | PASS |
| T-012 | Remove `{{VERSION}}` from templates | PASS |
| T-013 | Rewrite `docs/RELEASE.md` | PASS |
| T-014 | Update `CLAUDE.md` for cycle bootstrap | PASS |

---

## T-012: Remove `{{VERSION}}` from templates

### Checklist

| # | Acceptance Criterion | Result | Evidence |
|---|---|---|---|
| 1 | `.ai/PLAN.template.md` heading is version-agnostic | PASS | `# Plan` (was `# Plan — {{VERSION}}`) |
| 2 | `.ai/PLAN.template.md` goal text is version-agnostic | PASS | `Goal: implement the scope defined in \`ROADMAP.md\`.` (was `Goal: define and implement the scope for \`{{VERSION}}\`.`) |
| 3 | `.ai/REVIEW.template.md` heading is version-agnostic | PASS | `# Review` (was `# Review — {{VERSION}}`) |
| 4 | `ROADMAP.template.md` goal text is version-agnostic | PASS | `Goal: define and deliver the scope for this cycle.` (was `Goal: define and deliver the \`{{VERSION}}\` scope.`) |
| 5 | No `{{VERSION}}` in any template | PASS | `rg` confirms 0 matches across all 3 changed templates. |

### Plan Compliance

- All 3 template changes match the plan specification exactly (Phase 1).
- `.ai/TASKS.template.md` and `.ai/HANDOFF.template.md` correctly left unchanged (no `{{VERSION}}`).

---

## T-011: Create `scripts/ai-start-cycle.sh`

### Checklist

| # | Acceptance Criterion | Result | Evidence |
|---|---|---|---|
| 1 | Script exists and is executable | PASS | `scripts/ai-start-cycle.sh` present, `chmod +x` confirmed. |
| 2 | Shebang `#!/usr/bin/env bash`, `set -euo pipefail` | PASS | Lines 1-2. |
| 3 | `REPO_ROOT` resolved via `SCRIPT_DIR` pattern | PASS | Lines 4-6, consistent with other scripts. |
| 4 | Validates prefix: `feature/`, `fix/`, `chore/` | PASS | Lines 37-44, `case` statement. |
| 5 | Rejects bare prefix (e.g., `feature/`) | PASS | Lines 46-49, explicit check. |
| 6 | Rejects invalid git branch names | PASS | Lines 51-54, `git check-ref-format --branch`. |
| 7 | Prints usage on invalid input | PASS | `usage` called before `die` in all validation paths. |
| 8 | Checks branch existence locally | PASS | Lines 69-71, `git rev-parse --verify --quiet`. |
| 9 | Checks branch existence on remote | PASS | Lines 73-75, `git ls-remote --exit-code --heads`. |
| 10 | Checks out `main` and fast-forward pulls | PASS | Lines 77-78. |
| 11 | Creates new branch | PASS | Line 79, `git checkout -b`. |
| 12 | Copies 4 template files to cycle files | PASS | Lines 81-84. |
| 13 | Removes `.ai/HANDOFF.md` | PASS | Line 85, `rm -f`. |
| 14 | Stages cycle files | PASS | Line 87. |
| 15 | `git rm --cached .ai/HANDOFF.md` if tracked | PASS | Lines 89-91, conditional check. |
| 16 | Commits with `chore: start cycle <suffix>` | PASS | Line 93. |
| 17 | Pushes with `-u` tracking | PASS | Line 94. |
| 18 | No pom.xml modification or version logic | PASS | No `pom.xml`, `mvn`, or version references in script. |
| 19 | `bash -n` passes | PASS | Syntax check confirmed. |

### Plan Compliance

- Script implements all 9 steps from Phase 2 of the plan.
- Error handling covers all 3 specified abort scenarios (main checkout fails, branch exists, pull diverged).
- Helper functions (`usage`, `die`, `require_cmd`, `validate_branch_name`, `branch_exists_remotely`) are clean and idiomatic.

---

## T-013: Rewrite `docs/RELEASE.md`

### Checklist

| # | Acceptance Criterion | Result | Evidence |
|---|---|---|---|
| 1 | Overview paragraph present | PASS | Lines 6-7: one-paragraph summary with release-please and `ai-start-cycle.sh`. |
| 2 | Release Flow section with 5 steps | PASS | Lines 9-15: covers branch workflow, PR sync, release-please detection, Release PR review, and publish trigger. |
| 3 | Starting a New Development Cycle section | PASS | Lines 17-33: documents script usage, what it does, and next steps. |
| 4 | Required GitHub Secrets section retained | PASS | Lines 35-40: 4 secrets listed. |
| 5 | Local Dry Run section retained | PASS | Lines 42-55: both commands preserved. |
| 6 | No stale references | PASS | `rg` confirms 0 matches for `ai-release.sh`, `tag-version`, `compose-release-notes.sh`, `central.skipPublishing`, `finalize`. |
| 7 | Feature PR note about `ai-pr.sh sync` retained | PASS | Line 12: mentioned in Release Flow step 2. |

### Plan Compliance

- All 5 plan-specified sections present in correct order.
- All 5 categories of stale content removed.
- Clean rewrite — no remnants of old prepare/finalize model.

---

## T-014: Update `CLAUDE.md` for cycle bootstrap

### Checklist

| # | Acceptance Criterion | Result | Evidence |
|---|---|---|---|
| 1 | `mvn versions:set` bullet removed from Session Workflow | PASS | Diff shows removal of 2-line block. Section now flows directly from `spotless:apply` to `git add`. |
| 2 | `ai-start-cycle.sh` in AI Operating Mode | PASS | Lines 95-96: listed as "Cycle bootstrap" before the generic launcher. |
| 3 | `ai-start-cycle.sh` in Mixed Team Manual Workflow | PASS | Line 111: "Start a new development cycle with `scripts/ai-start-cycle.sh <branch-name>` before running `ai-plan.sh`." |
| 4 | No `mvn versions:set` references | PASS | `rg` confirms 0 matches. |
| 5 | No stale `finalize` references | PASS | `rg` confirms 0 matches. |

### Plan Compliance

- All 3 section updates match the plan specification exactly (Phase 4).
- Guardrail references to `ai-release.sh` and `compose-release-notes.sh` in Release Rules ("must not be reintroduced") are intentional and were approved in T-009 — not stale references.

---

## Commit Review

- Commit: `322c1aa` — `chore(cycle): add bootstrap workflow`
- Conventional Commit format: yes (`chore` type with `cycle` scope).
- Files in commit: 8 files changed (+242/−219).
- Plan specified separate commits per task; implementation bundled all 4 into a single commit.

---

## Findings

Ordered by severity:

### 1. [NOTE] Single commit for 4 tasks

**Severity:** Low (non-blocking)
**Description:** The plan states "Each task should be a separate Conventional Commit" but all 4 tasks were bundled into commit `322c1aa`. This is acceptable because the tasks are tightly coupled (T-012 feeds T-011 which feeds T-013/T-014) and the single commit is clean and atomic.

### 2. [NOTE] Typo in `.ai/PLAN.md` line 1

**Severity:** Cosmetic (non-blocking)
**Description:** Line 1 reads `cre# Plan — Development Cycle Bootstrap` — stray `cre` prefix before the heading. This is a process tracking file, not a deliverable, so it does not affect any acceptance criteria. Can be cleaned up in the next cycle reset.

---

## Required Fixes

None. All acceptance criteria pass for all 4 tasks.
