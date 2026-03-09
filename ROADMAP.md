# ROADMAP

Goal: shape the `0.5.x` line around larger API and operability improvements after the `0.4.x` operator and retention work.

## Priority 1: Delivery Controls

Objective: make retry and scheduling behavior more adaptable across workloads.

- Evaluate pluggable retry strategies such as exponential backoff and jitter.
- Clarify where retry policy should live: worker service, repository contract, or starter configuration.
- Define how future retry controls interact with ordering guarantees and redrive workflows.

## Priority 2: Topic and Tenant Isolation

Objective: improve support for more complex Kafka topologies.

- Explore multi-topic support and topic-level isolation rules.
- Document whether per-topic worker groups, retention, and observability need separate controls.
- Validate how current metadata resolution and ordering assumptions scale beyond a single topic.

## Priority 3: Platform Alignment

Objective: reduce future upgrade friction.

- Review deferred breaking API changes for a `0.5.x` boundary.
- Revisit schema versioning and migration tooling ergonomics.
- Validate Spring Boot and Jakarta compatibility expectations for the next major line.
