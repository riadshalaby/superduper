# CLAUDE

## Scope
- This file is the single source of truth for agent working rules and project context.

## Session Workflow
- Keep entries concise and timestamped in UTC.
- Run formatting after every code change:
  - `mvn -q spotless:apply`
- Use Maven version bumps for release and post-release transitions:
  - `mvn versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false`
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
- Sonar runs on `main` after `build`; its quality gate is visible in logs but non-blocking.
- Release flow is `build` -> `tag-version` -> `release` inside `ci.yml`, and only runs on `main` pushes.
- CI on `main` is the sole creator of release tags and GitHub Releases.
- `finalize` no longer pushes tags; it verifies that CI created the release tag before starting the next development cycle.
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
- Two-phase release workflow:
  1. Prepare release on feature branch:
     - `scripts/ai-release.sh prepare X.Y.Z`
     - Automatically stashes and restores unrelated worktree changes when needed.
     - Performs version bump to `X.Y.Z`, runs required validations, commits `chore(release): vX.Y.Z`, pushes the branch, and opens/updates a PR to `main`.
  2. Finalize release after user confirms PR merge:
     - Ask the user what the next version will be.
     - `scripts/ai-release.sh finalize X.Y.Z [NEXT_VERSION]`
     - Automatically stashes and restores unrelated worktree changes when needed.
     - Switches to `main`, verifies merged release version and that CI has created/pushed tag `vX.Y.Z`, prompts for `NEXT_VERSION` when omitted, creates branch `feature/vNEXT_VERSION`, bumps to next version, resets cycle files from templates, updates `ROADMAP.md`, and commits `chore: start vNEXT_VERSION`.
  3. CI post-merge release actions on `main`:
     - CI creates the release tag, publishes to Maven Central, and creates a GitHub Release with generated release notes.
- PR policy:
  - A PR to `main` is mandatory for release.
  - The agent opens or updates the PR; the user reviews and merges it on GitHub.
  - `finalize` must not run before user merge confirmation.

## Release Safety
- Never force-push `main`.
- Never bypass PR checks.
- Never create release tags manually; CI on `main` is the sole tag creator.
- Never tag from a feature branch.
- Never amend published release commits or tags unless explicitly requested.
- Do not run release operations without explicit user approval.

## Git Rules
- Work in the current branch.
