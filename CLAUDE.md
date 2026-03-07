# CLAUDE

## Scope
- This file is the single source of truth for agent working rules and project context.

## Session Workflow
- Keep entries concise and timestamped in UTC.
- Run formatting after every code change:
  - `mvn -q spotless:apply`
- Use Maven version bumps for release and post-release transitions:
  - `mvn versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false`
  - `mvn versions:set -DnewVersion=X.Y.(Z+1)-SNAPSHOT -DgenerateBackupPoms=false`
- Stage newly created files explicitly:
  - `git add <new-file>`
- When changes are complete, do not commit automatically:
  - Print a short, meaningful Conventional Commit message proposal.
  - Create the commit only after explicit user confirmation.
- Prefer targeted validation while iterating; run broader validation before finishing:
  - Fast compile: `mvn -q -DskipTests test-compile`
  - Full tests: `mvn -T 1C -q test`

## Language Rules
- Use English for code comments, log/output messages, `README.md`.

## Project Goal
Build and maintain the library described in `README.md`:
- Kafka ingest persists records as `READY` in `messages`.
- Worker claims eligible rows atomically with strict per-key ordering.
- Processing result transitions:
  - `SUCCESS` -> `PROCESSED`
  - `FAILURE` -> increment retry count, then `FAILED` or `STOPPED` at max retries.
- Heartbeats are written to `container_heartbeats`.
- Orphan reclaimer resets stale/dead-worker `PROCESSING` rows to `READY`.

## Current Architecture Baseline
- Documentation lives in `docs/` for all project docs except `README.md`, `ROADMAP.md`, and `CLAUDE.md`.
- Repository split:
  - `repository-api`
  - `repository-jdbc`
  - `repository-r2dbc`
- Workers and consumers use repository ports (no direct SQL in service classes).
- Observability split:
  - `observability-api`
  - `observability-logging`
  - `observability-metrics`
- Schema migrations are centralized in `schema-liquibase`.
- Architecture overview:
  - `docs/ARCHITECTURE.md`
- Examples:
  - `examples/app-blocking`
  - `examples/app-reactive`

## Quick Resume Checklist
- Read `README.md`.
- Read `docs/ARCHITECTURE.md`.
- Read `starter-autoselect/src/main/java/net/rsworld/superduper/starter/AutoSelectConfiguration.java`.
- Read worker services:
  - `worker-blocking/src/main/java/net/rsworld/superduper/worker/blocking/SuperDuperWorkerService.java`
  - `worker-reactive/src/main/java/net/rsworld/superduper/worker/reactive/SuperDuperWorkerReactiveService.java`
- Run: `mvn -T 1C -q test`

## AI Workflow Rules
- Plan Mode:
  - writes `.ai/PLAN.md`
  - never edits code
- Review Mode:
  - writes `.ai/REVIEW.md`
  - never edits code
- Implement Mode:
  - implements `.ai/PLAN.md`
  - updates tests
  - must not invent requirements

## AI Operating Mode
- Mode is selected by the launcher prompt/context:
  - Generic launcher: `scripts/ai-launch.sh <role> <agent> [agent-options...]`
    - roles: `plan`, `implement`, `review`
    - agents: `claude`, `codex`
  - Convenience wrappers:
    - `scripts/ai-plan.sh [agent] [agent-options...]` (default agent: `claude`)
    - `scripts/ai-implement.sh [agent] [agent-options...]` (default agent: `codex`)
    - `scripts/ai-review.sh [agent] [agent-options...]` (default agent: `claude`)
- No `.ai/MODE` file is used.

## Release Rules
- Never release directly from a feature branch.
- A feature is releasable only after it is merged into `main` via PR and required checks/tests pass.
- Set the release version before opening the release PR:
  - `mvn versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false`
- Use Conventional Commit message for the version bump:
  - `chore: release vX.Y.Z`
- Create tag `vX.Y.Z` on the corresponding merge commit in `main` (no unrelated extra commit between merge and tag).
- After the tagged release, bump the branch for the next cycle:
  - `mvn versions:set -DnewVersion=X.Y.(Z+1)-SNAPSHOT -DgenerateBackupPoms=false`
- Use Conventional Commit message for the post-release bump:
  - `chore: start vX.Y.(Z+1)-SNAPSHOT`
- After the release is done:
  - reset `.ai/PLAN.md` for the next cycle,
  - reset `.ai/REVIEW.md` for the next cycle,
  - rework `ROADMAP.md` to prepare scope and priorities for the next version.

## Strict PR Flow (Agent-Executable)
- The agent may execute the full release sequence only after explicit user approval in that session.
- Required order:
  1. On the feature branch, run `mvn versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false`.
  2. Run validation (`mvn -q -DskipTests test-compile` and `mvn -T 1C -q test` unless the session explicitly narrows it).
  3. Commit approved changes on the feature branch with `chore: release vX.Y.Z`.
  4. Push the feature branch to remote.
  5. Open or update a PR targeting `main`.
  6. Use PR title `chore: release vX.Y.Z`.
  7. Use this PR body template:
     ```text
     ## Summary
     - release vX.Y.Z

     ## Validation
     - [ ] mvn -q -DskipTests test-compile
     - [ ] mvn -T 1C -q test

     ## Release Checklist
     - [ ] version bumped with mvn versions:set
     - [ ] docs and roadmap updated
     - [ ] release tag will be created from the merge commit on main
     ```
  8. Ensure required checks pass.
  9. Merge PR into `main` (merge commit preferred unless repository policy enforces another method).
  10. Switch to `main` and fast-forward/pull latest remote.
  11. Verify the merge commit on `main` contains version `X.Y.Z`.
  12. Create tag `vX.Y.Z` on that merge commit in `main`.
  13. Push the tag to remote.
  14. create a new branch for the next release
  14. Perform post-release housekeeping on the branch for the next release cycle:
      - `mvn versions:set -DnewVersion=X.Y.(Z+1)-SNAPSHOT -DgenerateBackupPoms=false`
      - commit `chore: start vX.Y.(Z+1)-SNAPSHOT`
      - reset `.ai/PLAN.md`
      - reset `.ai/REVIEW.md`
      - update `ROADMAP.md`
- Tagging constraints:
  - Tag must point to the merge commit that introduced the released feature.
  - Do not add unrelated commits between merge and release tag.
  - Never tag from a feature branch.
- Safety constraints:
  - Never force-push `main`.
  - Never bypass PR checks.
  - Never amend published release commits or tags unless explicitly requested.

## Git Rules
- Work in the current branch.
