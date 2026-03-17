# ROADMAP

Goal: deliver `0.6.2-SNAPSHOT` with a unified CI pipeline where Sonar runs on `main` without blocking merges, and release runs from `ci.yml` only after successful build and tag on `main`.

## Priority 1: Sonar on Main (Non-Blocking Quality Gate)

Objective: execute Sonar analysis on `main` while preventing quality gate failures from breaking the pipeline.

- Run Sonar only on `main` after core build/test steps.
- Keep Sonar execution visible in CI output and artifacts.
- Configure quality gate handling as non-blocking (`continue-on-error` or equivalent guarded step logic).
- Add explicit log messaging when quality gate is red so failures are visible but do not fail the workflow.

## Priority 2: Merge Release Workflow into `ci.yml`

Objective: consolidate release automation into `ci.yml` and remove standalone release workflow duplication.

- Move logic from `release.yml` into a dedicated release job in `ci.yml`.
- Ensure release job runs only on `main`.
- Ensure release job is gated by successful main build job completion.
- Keep existing release semantics intact (version/tag handling and release steps).
- Decommission `release.yml` after parity is confirmed.

## Priority 3: Strict Execution Order and Safety Gates

Objective: enforce correct release sequencing and avoid accidental release execution.

- Enforce dependency chain: `build/test` -> `tag version` -> `release`.
- Run release only when version tagging is successful.
- Block release job on pull requests and non-main branches.
- Add workflow-level conditions and job `needs` checks for deterministic behavior.

## Done Criteria

- Sonar runs on `main` and reports quality gate status without failing the pipeline.
- `ci.yml` contains the full release flow previously handled by `release.yml`.
- Release runs only on `main` and only after successful build and successful version tagging.
- Standalone `release.yml` is removed or disabled after migration validation.
