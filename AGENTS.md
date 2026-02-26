# SUPERDUPER Agent Instructions

## Scope
- This file is the single source of truth for agent working rules and project context.
- `RESTART_CONTEXT.txt` is progress history only (no duplicated permanent instructions).

## Session Workflow
- Update `RESTART_CONTEXT.txt` whenever progress is made (code changes, decisions, test results, blockers, next steps).
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
- Use English for code comments, log/output messages, `README.md`, and `RESTART_CONTEXT.txt` entries.

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
