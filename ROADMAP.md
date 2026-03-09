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
- define a retention policy for `messages` and `container_heartbeats` tables. 14 days for `messages` and 1 day for `container_heartbeats`.
- create a cleanup task and make its schedule configurable.

## Priority 3:
- prepare the `0.5.x` release.
