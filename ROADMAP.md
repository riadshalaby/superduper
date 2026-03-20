# ROADMAP

Goal: define and deliver the scope for this cycle.

## Priority 1

Objective: bugfix for MariaDB payload truncation risk.

- Change MariaDB schema for `content` from `TEXT` to `LONGTEXT`.
- Add migration/update notes for existing installations.

## Priority 2

Objective: Outbox Pattern support via a new Outbox Service.

- Provide an Outbox Service interface that lets users write messages directly to a DB table, bypassing Kafka ingest.
- Reuse the existing worker infrastructure to claim and process outbox messages with the same ordering and retry guarantees.
- Enable users to implement the transactional Outbox Pattern: persist domain events in the same transaction as their business data, then let superduper reliably deliver them.
- Kafka consumers and outbox services are fully composable: users can run any mix in the same application (e.g. 2 Kafka consumers + 3 outbox services).
- Table isolation is configurable per consumer/outbox service individually: each can write to its own dedicated table or share a table with others.
- The only global choice is the execution model (blocking vs. reactive); it applies uniformly across all consumers and outbox services.

## Priority 3

Objective: promote to 1.0.0 stable release.

- All planned features (outbox pattern, MariaDB bugfix) are complete.
- Mark the library as feature-complete and production-ready.
- Use `release-as: 1.0.0` to trigger the major version bump via release-please.
- Update README.md and documentation to reflect 1.0.0 stable status.
