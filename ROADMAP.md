# ROADMAP v0.6.0

Goal: push the transactional inbox algorithm closer to its theoretical limits — reduce latency, increase claim parallelism, and extend the scale ceiling before a fundamentally different architecture becomes necessary.

## Priority 1: Event-Driven Claim Trigger (LISTEN/NOTIFY)

Objective: eliminate poll-based latency for the common case.

- Use PostgreSQL `LISTEN/NOTIFY` to wake the worker claim loop immediately when new rows are inserted.
- Keep the poll interval as a fallback (heartbeat-style) so the worker never stalls if a notification is missed.
- Expected impact: P50 claim latency drops from `interval/2` to near-zero for fresh messages.
- Scope: PostgreSQL only; MariaDB continues with poll-based claiming.

## Priority 2: Per-Topic Claim Locks

Objective: allow topics to claim in parallel instead of serializing all claim work behind a single ShedLock entry.

- Each topic entry in `TopicRegistry` already has a dedicated `claimLockName`; verify that claim loops for different topics actually acquire independent locks.
- If not, wire each `TopicWorkerInstance` / `ReactiveTopicWorkerInstance` to its own ShedLock name so multi-topic deployments scale linearly with topic count.
- Expected impact: N topics can claim concurrently instead of queuing behind one lock.

## Priority 3: Batch Inserts on Ingest

Objective: amortize database round-trips on the consumer ingest path.

- Buffer N consumed Kafka records (or up to a time window) before issuing a single batch INSERT.
- Ensure `message_id` deduplication still works correctly with batch upserts.
- Preserve at-least-once semantics: Kafka offsets must not be acknowledged before the batch is persisted.
- Expected impact: ingest throughput improves proportionally to batch size, reducing per-record INSERT overhead.

## Priority 4: Partition-Aware Worker Affinity

Objective: reduce claim lock contention by giving workers a natural work-sharding hint.

- Workers advertise which Kafka partitions their co-located consumer owns.
- The claim query adds a soft preference for rows ingested from the worker's own partitions.
- Fallback: if no preferred rows are available, claim from the full candidate set (no starvation).
- Expected impact: under normal load, workers mostly claim "their" rows without competing for the global lock.

## Priority 5: Table Partitioning Guide

Objective: keep claim scans fast at millions of rows without aggressive cleanup windows.

- Document native PostgreSQL table partitioning strategies for `messages` (by status or by `received_at` range).
- Provide example Liquibase changesets for partitioned table creation.
- Validate that claim, fetch, and maintenance queries use partition pruning correctly.
- Expected impact: claim index scan cost stays constant regardless of total row count.

## Priority 6: Hybrid Fast-Path (Exploratory)

Objective: remove the database from the hot path for the common case (no failures, no ordering conflict).

- For messages where no earlier same-key message is in flight or failed, process directly from Kafka and write the outcome to the DB as a journal entry.
- Fall back to the full claim-based path when ordering conflicts or retries are detected.
- This is the highest-complexity item and should be prototyped and benchmarked before committing.
- Expected impact: 90%+ of messages skip the INSERT-then-claim round-trip; DB write volume drops to ~1 write per message instead of 3.

## Non-Goals for v0.6.0

- Replacing the relational database with a different backing store.
- Supporting databases other than PostgreSQL and MariaDB.
- Changing the public handler API (`MessageHandler` / `ReactiveMessageHandler`).
