# Review: T-002 — TopicConfigView evolution + worker/consumer alignment

- Reviewer: claude
- Date (UTC): 2026-03-20
- Commit reviewed: `cf180bf6ca29e0b707e07ea8743b63758a610c8b`

Review Round: **1**

## Acceptance Criteria Checklist

| Criterion | Status | Notes |
|---|---|---|
| `TopicConfigView` has `topicColumnValue()` default method | ✅ | Returns `kafkaTopic()` if non-blank, else `name()`; with Javadoc |
| `TopicRegistry.kafkaTopics()` filters out blank `kafkaTopic` entries | ✅ | Stream filter: `kafkaTopic != null && !kafkaTopic.isBlank()` |
| `TopicWorkerInstance` passes `topicColumnValue()` as the topic | ✅ | Line 47: `topicConfig.topicColumnValue()` |
| `ReactiveTopicWorkerInstance` passes `topicColumnValue()` as the topic | ✅ | Line 43: `topicConfig.topicColumnValue()` |
| `KafkaConsumerService` writes `topicColumnValue()` in `MessageIngestData.topic` | ✅ | Line 140: `topicConfig.topicColumnValue()` |
| `KafkaReactiveR2dbcConsumerService` writes `topicColumnValue()` in `MessageIngestData.topic` | ✅ | Line 141: `topicRegistry.getByKafkaTopic(kafkaTopic).topicColumnValue()` |
| All existing tests pass unchanged | ✅ | Per implementer validation |
| `mvn -q -DskipTests test-compile` passes | ✅ | Per implementer validation |
| `mvn -T 1C -q test` passes | ✅ | Per implementer validation |

## Findings

### SEV-3 (Info) — Bonus duplicate Kafka topic detection in `TopicRegistry` constructor

The constructor now throws `IllegalArgumentException` on duplicate `kafkaTopic` values when building the lookup map. This is not in the plan but is a correct defensive guard and is covered by the new `rejectsDuplicateKafkaTopics` test.

**Impact:** None — additive, correct behavior.
**Required fix:** None.

### SEV-3 (Info) — Minor style inconsistency in reactive consumer

`KafkaReactiveR2dbcConsumerService` calls `topicRegistry.getByKafkaTopic(kafkaTopic).topicColumnValue()` inline, while `KafkaConsumerService` stores the config in a local variable `topicConfig` first. Functionally equivalent.

**Impact:** None.
**Required fix:** None.

### SEV-3 (Info) — Handoff package path typo for reactive consumer

The implementer handoff lists the reactive consumer at `consumer-kafka-reactive/src/main/java/net/rsworld/superduper/consumer/kafka/reactive/...` but the actual path is `consumer-kafka-reactive/src/main/java/net/rsworld/superduper/consumer/reactive/...`. Documentation-only discrepancy.

**Impact:** None.
**Required fix:** None.

## Open Questions

None.

## Verdict

`PASS`

All acceptance criteria are satisfied. `topicColumnValue()` default method is correctly implemented and well-documented. Both workers and both consumers use it. `TopicRegistry.kafkaTopics()` and its constructor both filter blank `kafkaTopic` entries, preventing outbox topics from reaching `@KafkaListener`. Tests cover the blank-filtering, custom column value routing, and duplicate detection. No required fixes.
