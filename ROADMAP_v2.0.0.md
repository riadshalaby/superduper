# ROADMAP v2.0.0

Goal: introduce event-driven claiming to eliminate poll-based latency, marking a breaking change in the worker scheduling model.

## Priority 1: Event-Driven Claim Trigger (Spring ApplicationEvent)

Objective: eliminate poll-based latency for the common case.

- Publish a Spring `ApplicationEvent` from the consumer after each successful ingest.
- The worker claim loop subscribes to that event and triggers an immediate claim cycle.
- Keep the poll interval as a fallback so the worker never stalls if no local ingest occurs (e.g., messages arriving via other containers).
- Works on both PostgreSQL and MariaDB since the mechanism is JVM-local, not database-specific.
- Limitation: only wakes the worker in the same container. Cross-container pickup still relies on the poll interval.
- Expected impact: P50 claim latency drops from `interval/2` to near-zero for messages ingested by the local consumer.
