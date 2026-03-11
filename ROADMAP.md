# ROADMAP

Goal: deliver `0.5.2-SNAPSHOT` with robust multi-topic examples and a runtime script that works like `./examples/run-multi.sh` for the new modes.

## Priority 1: Runtime Script for New Modes

Objective: replace `./examples/verify-multitopic.sh` with a `run-multi.sh`-style operator script for the new multi-topic modes.

- Replace `./examples/verify-multitopic.sh` with a new run script (for example `./examples/run-multitopic-modes.sh`).
- Run seeder and worker containers in parallel, following the behavior pattern of `./examples/run-multi.sh`.
- Support both shared-table mode and dedicated-table-per-topic mode from one script entrypoint.
- Print clear runtime guidance for inspecting logs and database state while containers are running.
- Keep the script focused on starting/running flows for the new modes, not test-style verification output.

## Priority 2: Dedicated-Mode Schema Cleanup

Objective: remove unused schema artifacts when running in dedicated-table mode.

- Ensure the standard Liquibase path does not leave an unused default `messages` table in dedicated mode.
- Define and implement the dedicated-mode schema strategy explicitly in migrations/docs.
- Validate that shared mode and dedicated mode each create only the expected tables.

## Priority 3: Documentation and Release Readiness

Objective: make the new run workflow discoverable and release-safe.

- Update `README.md` and `docs/USAGE.md` with quickstart instructions for both modes.
- Document trade-offs and expected database outcomes for shared vs dedicated strategies.
- Add CI coverage for building and starting the new mode-specific run flow.
- Define release acceptance criteria: run script works for both modes, schema outcome verified, docs updated.
