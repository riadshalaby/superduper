# ROADMAP

Goal: create two multi-topic examples:
- one example with a shared `messages` table for all topics,
- one example with dedicated message tables per topic.

## Priority 1: Example A — Shared Table Multi-Topic

Objective: provide a runnable example where multiple Kafka topics share one `messages` table.

- Add or extend an example app with at least two topics and different handlers.
- Configure `superduper.topics` entries without `table` overrides.
- Show topic-specific worker tuning (`batch-size`, `max-retries`) in config.
- Add a short run guide and expected behavior (topic-aware claim and processing).
- Add a smoke test or verification script for end-to-end ingest and processing.

## Priority 2: Example B — Dedicated Table Per Topic

Objective: provide a runnable example where each topic uses its own message table.

- Add or extend an example app with at least two topics and explicit `table` per topic.
- Add Liquibase example wiring for creating per-topic message tables.
- Keep a single shared connection pool (`DataSource`/`ConnectionFactory`) and document it.
- Add a run guide with verification steps showing table-level isolation.
- Add a smoke test or verification script validating records land in the expected topic tables.

## Priority 3: Documentation and Comparison

Objective: make both examples easy to understand, run, and compare.

- Update `docs/USAGE.md` with a dedicated section for both example modes.
- Add a comparison table: shared vs dedicated tables (ops model, isolation, schema overhead).
- Document recommended use-cases and trade-offs.
- Ensure README links directly to both examples and their startup steps.
- Define acceptance criteria:
  - both examples run locally,
  - both process at least two topics,
  - behavior matches documented table strategy.
