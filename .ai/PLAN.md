cre# Plan — Development Cycle Bootstrap

Status: **approved**

Goal: create `scripts/ai-start-cycle.sh` to automate the repeatable steps of starting a new development cycle (branch creation, cycle file reset) and update all related documentation and templates.

## Context

The old `ai-release.sh finalize` handled post-release cycle transitions (branch, version bump, file reset). With release-please owning versioning, the version bump is gone but the cycle bookkeeping remains manual. This plan fills that gap with a lightweight script.

## Scope

1. New `scripts/ai-start-cycle.sh` script for bootstrapping a development cycle.
2. Remove `{{VERSION}}` placeholders from `.ai/` and `ROADMAP` templates (release-please owns versions).
3. Rewrite `docs/RELEASE.md` to reflect the release-please model.
4. Update `CLAUDE.md` to document the new script and remove stale version-management references.

## Acceptance Criteria

1. `scripts/ai-start-cycle.sh feature/my-scope` creates a branch from latest `main`, resets cycle files, commits, and pushes.
2. The script validates branch names against allowed prefixes: `feature/`, `fix/`, `chore/`.
3. The script does NOT perform any version bump or pom.xml modification.
4. Templates no longer contain `{{VERSION}}` placeholders.
5. `docs/RELEASE.md` accurately describes the release-please flow and the new cycle bootstrap.
6. `CLAUDE.md` documents `ai-start-cycle.sh` and has no stale `mvn versions:set` or `finalize` references.
7. `mvn -q -DskipTests test-compile` passes.
8. `mvn -T 1C -q test` passes.

## Implementation Phases

### Phase 1 — Update Templates (T-012)

Remove `{{VERSION}}` from all templates. release-please owns versions; cycle files no longer carry a version label.

**`.ai/PLAN.template.md`:**
- `# Plan — {{VERSION}}` → `# Plan`
- `Goal: define and implement the scope for \`{{VERSION}}\`.` → `Goal: implement the scope defined in \`ROADMAP.md\`.`

**`.ai/REVIEW.template.md`:**
- `# Review — {{VERSION}}` → `# Review`

**`ROADMAP.template.md`:**
- `Goal: define and deliver the \`{{VERSION}}\` scope.` → `Goal: define and deliver the scope for this cycle.`

**`.ai/TASKS.template.md`** and **`.ai/HANDOFF.template.md`:** no `{{VERSION}}` — unchanged.

### Phase 2 — Create Bootstrap Script (T-011)

Create `scripts/ai-start-cycle.sh` (executable, `#!/usr/bin/env bash`, `set -euo pipefail`).

**Usage:**
```
scripts/ai-start-cycle.sh <branch-name>
```

**Branch name validation:**
- Must start with `feature/`, `fix/`, or `chore/`.
- Must have at least one character after the prefix (e.g., `feature/` alone is rejected).
- Reject names containing spaces or characters invalid in git branch names.
- Print usage and exit 1 on invalid input.

**Steps the script performs:**
1. Resolve `REPO_ROOT` (same pattern as other scripts in `scripts/`).
2. Validate branch name argument.
3. `git checkout main && git pull --ff-only origin main` — ensure latest main.
4. `git checkout -b <branch-name>` — create the new branch.
5. Copy templates to cycle files:
   - `cp .ai/PLAN.template.md .ai/PLAN.md`
   - `cp .ai/REVIEW.template.md .ai/REVIEW.md`
   - `cp .ai/TASKS.template.md .ai/TASKS.md`
   - `cp ROADMAP.template.md ROADMAP.md`
6. Remove `.ai/HANDOFF.md` if it exists (`rm -f .ai/HANDOFF.md`).
7. `git add .ai/PLAN.md .ai/REVIEW.md .ai/TASKS.md ROADMAP.md` — stage reset files. Also `git rm --cached .ai/HANDOFF.md` if it was tracked.
8. `git commit -m "chore: start cycle $(basename <branch-name>)"` — commit the reset.
9. `git push -u origin <branch-name>` — push the new branch with tracking.

**Error handling:**
- If `main` checkout fails, abort with message.
- If branch already exists locally or on remote, abort with message.
- If `git pull --ff-only` fails (diverged), abort with message.

**No version logic:** the script must not read, write, or modify any `pom.xml`.

### Phase 3 — Rewrite Release Documentation (T-013)

Rewrite `docs/RELEASE.md` to replace the stale prepare/finalize documentation.

**New structure:**

1. **Overview** — one-paragraph summary: release-please automates releases from Conventional Commits; `ai-start-cycle.sh` bootstraps new development cycles.

2. **Release Flow** (replaces old "Flow" section):
   - Feature work happens on branches (`feature/`, `fix/`, `chore/`) and merges to `main` via PR.
   - release-please on `main` detects releasable commits and creates/updates a Release PR with version bumps and CHANGELOG entries.
   - Merging the Release PR triggers: tag creation, GitHub Release, Maven Central publish (via `publish` job in `ci.yml`).

3. **Starting a New Development Cycle** (replaces old "Finalize" section):
   - Run `scripts/ai-start-cycle.sh <branch-name>`.
   - Describe what the script does (branch, reset, commit, push).
   - After running: write `ROADMAP.md`, then start the plan/implement/review workflow.

4. **Required GitHub Secrets** — keep as-is (MAVEN_CENTRAL_USERNAME, MAVEN_CENTRAL_TOKEN, MAVEN_GPG_KEY, MAVEN_GPG_PASSPHRASE).

5. **Local Dry Run** — keep the existing `mvn -Prelease -DskipTests verify` section as-is.

**Remove entirely:**
- Old "Prepare" section (references `ai-release.sh prepare`).
- Old "Build and tag" section (references `tag-version` job).
- Old "Finalize" section (references `ai-release.sh finalize`).
- Old "GitHub Release Notes Generation" section — rewrite as part of "Release Flow" above. Keep the note that feature PRs use `ai-pr.sh sync`.
- All references to `central.skipPublishing`, `--skip-central`, `compose-release-notes.sh`.

### Phase 4 — Update CLAUDE.md (T-014)

**"Session Workflow":**
- Remove the `mvn versions:set` bullet entirely. release-please owns version management; there is no manual version bump step.

**"AI Operating Mode":**
- Add `scripts/ai-start-cycle.sh <branch-name>` as the cycle bootstrap command, listed alongside the existing convenience wrappers.

**"Mixed Team Manual Workflow":**
- Add a note that `scripts/ai-start-cycle.sh` is the starting point for a new cycle, before `ai-plan.sh`.

**Verify no stale references remain** to:
- `mvn versions:set`
- `ai-release.sh`
- `compose-release-notes.sh`
- `finalize`
- `{{VERSION}}`

## Task Dependency Graph

```
T-012 ──> T-011 ──> T-013
                ──> T-014
```

- T-012 (templates): first, because T-011's `cp` must copy the updated templates.
- T-011 (script): depends on T-012.
- T-013 (docs/RELEASE.md): depends on T-011 (documents the script).
- T-014 (CLAUDE.md): depends on T-011 (references the script).
- T-013 and T-014 are independent of each other.

## Implementation Notes

- All tasks are implemented on the current branch.
- Each task should be a separate Conventional Commit.
- The script itself is pure bash/git — no Maven changes, so `test-compile` and `test` should pass trivially.
- Make the script executable: `chmod +x scripts/ai-start-cycle.sh`.

## Validation

- `mvn -q -DskipTests test-compile`
- `mvn -T 1C -q test`
- `bash -n scripts/ai-start-cycle.sh` (syntax check)
- Verify no `{{VERSION}}` remains in templates.
- Verify `docs/RELEASE.md` has no references to `ai-release.sh`, `tag-version`, `compose-release-notes.sh`, or `central.skipPublishing`.
- Verify `CLAUDE.md` has no `mvn versions:set` or `finalize` references.
