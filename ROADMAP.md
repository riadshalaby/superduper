# ROADMAP v0.6.0

Goal: push the transactional inbox algorithm closer to its theoretical limits — reduce latency, increase claim parallelism, and extend the scale ceiling before a fundamentally different architecture becomes necessary.

## Priority 1: Per-Topic Claim Locks

Objective: allow topics to claim in parallel instead of serializing all claim work behind a single ShedLock entry.

- Each topic entry in `TopicRegistry` already has a dedicated `claimLockName`; verify that claim loops for different topics actually acquire independent locks.
- If not, wire each `TopicWorkerInstance` / `ReactiveTopicWorkerInstance` to its own ShedLock name so multi-topic deployments scale linearly with topic count.
- Expected impact: N topics can claim concurrently instead of queuing behind one lock.

## Priority 2: Batch Inserts on Ingest

Objective: amortize database round-trips on the consumer ingest path.

- Buffer N consumed Kafka records (or up to a time window) before issuing a single batch INSERT.
- Ensure `message_id` deduplication still works correctly with batch upserts.
- Preserve at-least-once semantics: Kafka offsets must not be acknowledged before the batch is persisted.
- Error handling: a DB error rolls back the entire batch, delaying all N records instead of one. Define a fallback strategy (retry full batch, split-and-retry to isolate the failing record, or fall back to single-record inserts on error). Batch size must be tunable so operators can balance throughput gain against blast radius.
- Expected impact: ingest throughput improves proportionally to batch size, reducing per-record INSERT overhead.


