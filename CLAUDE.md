# CLAUDE

## Scope
- This file is the single source of truth for agent working rules and project context.

## Session Workflow
- Keep entries concise and timestamped in UTC.
- Run formatting after every code change:
  - `mvn -q spotless:apply`
- Use Maven version bumps only for development version management when explicitly needed:
  - `mvn versions:set -DnewVersion=NEXT_VERSION -DgenerateBackupPoms=false`
- Stage newly created files explicitly:
  - `git add <new-file>`
- Commit behavior by role:
  - `plan` and `review` roles never commit.
  - `implement` role must stage all changes and create a Conventional Commit after validations pass.
- Prefer targeted validation while iterating; run broader validation before finishing:
  - Fast compile: `mvn -q -DskipTests test-compile`
  - Full tests: `mvn -T 1C -q test`

## CI Pipeline
- GitHub Actions CI runs on every push and on pull requests targeting `main`.
- All CI/CD workflow logic lives in `.github/workflows/ci.yml`.
- CI is the authoritative gate for formatting, compile, tests, and coverage artifacts.
- `build` runs on all pushes and pull requests.
- `sonar` runs on `main` after `build`; its quality gate is visible in logs but non-blocking.
- `release-please` runs on `main` after `build`; it maintains the Release PR with version bumps and `CHANGELOG.md` updates.
- `publish` runs only when `release-please` reports `release_created == 'true'`, which happens when the Release PR is merged into `main`.
- release-please creates the release tag and GitHub Release automatically.
- `.github/workflows/release.yml` was removed and must not be reintroduced.
- `scripts/build-all.sh` was removed and must not be reintroduced as a project build entrypoint.

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

## AI Workflow Rules
- Plan Mode:
  - waits for explicit user start signal
  - writes `.ai/PLAN.md`
  - updates `.ai/TASKS.md` status to `ready_for_implement`
  - appends a handoff entry to `.ai/HANDOFF.md`
  - never edits code
- Review Mode:
  - waits for explicit user start signal
  - writes `.ai/REVIEW.md`
  - updates `.ai/TASKS.md` status to `done` or `changes_requested`
  - appends a handoff entry to `.ai/HANDOFF.md`
  - never edits code
- Implement Mode:
  - waits for explicit user start signal
  - implements `.ai/PLAN.md`
  - updates tests
  - stages files with `git add -A`
  - commits with a Conventional Commit message
  - updates `.ai/TASKS.md` status to `ready_for_review`
  - appends a handoff entry to `.ai/HANDOFF.md` including commit hash
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
- Optional manual gate checks:
  - `scripts/ai-check-plan.sh <TASK_ID>` before starting implement mode
  - `scripts/ai-check-review.sh <TASK_ID>` before starting review mode
- No `.ai/MODE` file is used.

## Mixed Team Manual Workflow
- No role autostarts another role.
- Every role waits in `WAIT_FOR_USER_START` state until you explicitly tell it to begin.
- Agent choice is manual per run (`claude` or `codex`) and can vary by role and task.
- Handoff log policy:
  - runtime log: `.ai/HANDOFF.md` (gitignored)
  - tracked template: `.ai/HANDOFF.template.md`
- Handoffs are file-based:
  - planner -> implementer uses `.ai/PLAN.md` + `.ai/TASKS.md` + `.ai/HANDOFF.md`
  - implementer -> reviewer uses commit + `.ai/TASKS.md` + `.ai/HANDOFF.md`
- Recommended status flow in `.ai/TASKS.md`:
  - `todo` -> `in_planning` -> `ready_for_implement` -> `in_implementation` -> `ready_for_review` -> `in_review` -> `done`

## Release Rules
- Release execution role: `implement`.
- Release actions require explicit user command in-session.
- Conventional Commits merged to `main` are the source of truth for release automation.
- release-please maintains a Release PR with the version bump and `CHANGELOG.md` updates.
- Merging the Release PR triggers the automated release path on `main`: tag creation, GitHub Release creation, and Maven Central publishing.
- Do not perform manual release version bumps in the repository for normal releases.
- `scripts/ai-release.sh` and `scripts/compose-release-notes.sh` must not be reintroduced.

## PR Policy
- Release PRs are managed by release-please and are auto-created and auto-updated on `main`.
- Feature PRs still use `scripts/ai-pr.sh sync`.
- `scripts/ai-pr.sh sync` writes the Summary, Breaking Changes, Included Commits, and Test Plan sections for feature PRs.
- A PR to `main` remains mandatory for user-reviewed changes.

## Release Safety
- Never force-push `main`.
- Never bypass PR checks.
- Never create release tags manually; release-please is the sole tag creator.
- Never amend published release commits or tags unless explicitly requested.
- Feature PRs merged to `main` do not publish by themselves; only merging the Release PR triggers publish.

## Git Rules
- Work in the current branch.
