# CLAUDE

## Scope
- This file is the single source of truth for agent working rules and project context.

## Session Workflow
- Keep entries concise and timestamped in UTC.
- Run formatting after every code change:
  - `mvn -q spotless:apply`
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
  - `RETRY` -> increment retry count, then `READY` or `STOPPED` at max retries.
- Heartbeats are written to `container_heartbeats`.
- Orphan reclaimer resets stale/dead-worker `PROCESSING` rows to `READY`.

## Current Architecture Baseline
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
- Examples:
  - `examples/app-blocking`
  - `examples/app-reactive`

## Quick Resume Checklist
- Read `README.md`.
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
- Create tag `vX.Y.Z` on the corresponding merge commit in `main` (no unrelated extra commit between merge and tag).
- After the release is done:
  - reset `.ai/PLAN.md` for the next cycle,
  - reset `.ai/REVIEW.md` for the next cycle,
  - rework `ROADMAP.md` to prepare scope and priorities for the next version.

## Git Rules
- Work in the current branch.
- Never auto commit.
- Human reviews diffs before commit.
