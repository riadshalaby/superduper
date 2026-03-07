# ROADMAP

Goal: harden the `0.4.x` line for production-style operations after the metadata and batch-throughput changes landed in `0.4.0`.

## Priority 1: Redrive and Failure Operations

- Add an operator-facing path to inspect and redrive `FAILED` and `STOPPED` messages without manual SQL.
- Define the supported retry and redrive contract for both blocking and reactive stacks.
- Add integration coverage for redrive flows, including same-key batches that were previously released back to `READY`.

## Priority 2: Observability and Runtime Diagnostics

- Expand metrics and logging around claim/release/retry behavior so hot-key and failure scenarios are visible without database inspection.
- Document how to monitor worker heartbeats, stale processing rows, and backlog growth.
- Add examples or docs for the metadata fields introduced in `0.4.0`, especially how they should appear in logs and dashboards.

## Priority 3: Claim-Path Performance Regression Guardrails

- Add repeatable benchmark or explain-plan validation for Postgres and MariaDB claim queries under mixed-key and hot-key loads.
- Track the cost of the new batch claim strategy so index or SQL regressions are caught before release.
- Keep the performance checks lightweight enough to run in CI or as a targeted pre-release validation step.
