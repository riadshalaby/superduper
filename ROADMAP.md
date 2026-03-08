# ROADMAP

Goal: turn the `0.4.x` operational foundation into a smoother adoption and day-2 operations story after the `0.4.1` redrive and observability release.

## Priority 1: Operator Entry Points and Examples

Objective: make the new redrive and queue-health capabilities easy to use in real applications.

- Add example application flows for inspecting `FAILED` and `STOPPED` messages and redriving them without direct SQL.
- Show how queue-health polling and observability outputs should be enabled in starter-based applications.
- Document safe operational usage patterns for admin workflows in blocking and reactive deployments.

## Priority 2: Queue Retention and Cleanup

Objective: define how queue tables stay manageable once production traffic accumulates.

- Add a supported cleanup/archive story for old `PROCESSED` rows and terminal failures.
- Document retention guidance and operational tradeoffs for `messages` and `container_heartbeats`.
- Cover cleanup behavior with integration tests so retention tasks do not interfere with claim, redrive, or orphan recovery.

## Priority 3: CI and Release Hardening

Objective: make the new guardrails and release flow repeatable with less manual effort.

- Promote claim-plan regression checks into a reliable pre-release or CI workflow.
- Tighten release automation and documentation around version bumps, tagging, and post-release branch setup.
- Keep container-heavy validation reproducible and scoped so release verification remains practical.
