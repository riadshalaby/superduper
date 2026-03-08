# ROADMAP

Goal: stabilize the `0.4.x` line for production operations after the metadata and batch-throughput changes introduced in `0.4.0`.

## Priority 1: Redrive and Failure Operations

Objective: make failure handling operable without ad hoc database access.

- Provide an operator-facing workflow to inspect and redrive `FAILED` and `STOPPED` messages without manual SQL.
- Define and document the retry/redrive contract for both blocking and reactive stacks.
- Add integration tests for redrive behavior, including same-key batches previously released back to `READY`.

## Priority 2: Observability and Runtime Diagnostics

Objective: make queue health and failure patterns visible through logs and metrics.

- Expand metrics and logs for claim/release/retry behavior so hot-key and failure scenarios are diagnosable without direct database inspection.
- Document operational monitoring for worker heartbeats, stale `PROCESSING` rows, and backlog growth.
- Emit clear per-batch summaries: total processed, total failed, and total stopped.
- Log per-message processing details at `DEBUG` level not at info level.
- Add examples for metadata fields introduced in `0.4.0`, including expected representation in logs and dashboards.

## Priority 3: Claim-Path Performance Regression Guardrails

Objective: prevent claim-query regressions before release.

- Add repeatable benchmarks or explain-plan validation for Postgres and MariaDB claim queries under mixed-key and hot-key workloads.
- Track the cost of the batch-claim strategy so SQL or index regressions are detected early.
- Keep checks lightweight enough for CI, or runnable as targeted pre-release validation.
