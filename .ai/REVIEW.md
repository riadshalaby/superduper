# Review: T-005 — Outbox configuration + TopicRegistry merge + starter wiring

- Reviewer: claude
- Date (UTC): 2026-03-20
- Commit reviewed: `6c84d23cbb414959b91468c5fb0190f3568f158b`

Review Round: **1**

## Acceptance Criteria Checklist

| Criterion | Status | Notes |
|---|---|---|
| `OutboxProperties` parses `superduper.outbox.<name>` with `handler`, `batchSize`, `maxRetries`, `table` | ✅ | `@ConfigurationProperties(prefix = "superduper")`; null-safe setters |
| `handler` required — missing handler throws `IllegalArgumentException` at registry build | ✅ | `resolveConfiguredOutboxes()` line 210-212; test `topicRegistryRequiresOutboxHandler` |
| Outbox entries in `TopicRegistry` with `kafkaTopic = ""` and `claimLockName = "superduper-claim-<name>"` | ✅ | `resolveConfiguredOutboxes()` lines 215, 220 |
| `batchSize`/`maxRetries` fall back to `WorkerProperties` when zero | ✅ | Consistent with Kafka topic resolution pattern |
| Workers pick up outbox topics automatically | ✅ | All topics iterate via `TopicRegistry.topics()` in coordinators |
| `@KafkaListener` does NOT subscribe to outbox topic names | ✅ | `kafkaTopics()` filters blank entries (T-002); test asserts only `orders.events` |
| Outbox-only configuration allowed (no Kafka topics) | ✅ | Test `topicRegistryAllowsOutboxOnlyConfiguration` |
| Error message updated to include outbox hint | ✅ | Line 163: "…or configure superduper.outbox." |
| `OutboxProperties` enabled in `@EnableConfigurationProperties` | ✅ | Line 70 |
| `OutboxService` bean created for `consumer.type=spring`, conditional on outboxes present | ✅ | `jdbcOutboxService()` with `@Conditional(OutboxConfiguredCondition.class)` |
| `ReactiveOutboxService` bean created for `consumer.type=reactor`, conditional on outboxes present | ✅ | `reactiveOutboxService()` with `@Conditional(OutboxConfiguredCondition.class)` |
| Shared-table outbox uses default repository | ✅ | `table.isBlank()` → shared repo in `resolveOutboxRepositories()` |
| Dedicated-table outbox uses `RepositoryFactory.createIngestRepository(table)` | ✅ | Non-blank table → factory |
| No `OutboxService`/`ReactiveOutboxService` bean when no outboxes configured | ✅ | `OutboxConfiguredCondition.matches()` returns false; both service tests verify absence |
| `outbox-blocking` and `outbox-reactive` added to `starter-autoselect/pom.xml` | ✅ | Lines 47-54 |
| `outbox-blocking` and `outbox-reactive` added to `coverage-report/pom.xml` | ✅ | Lines 63-72 |
| `mvn -q -DskipTests test-compile` passes | ✅ | Per implementer validation |
| `mvn -T 1C -q test` passes | ✅ | Per implementer validation |

## Findings

### SEV-3 (Info) — Outbox modules are mandatory transitive dependencies of the starter

`outbox-blocking` and `outbox-reactive` are added as regular compile-scope dependencies (not `optional`) in `starter-autoselect/pom.xml`. This mirrors the existing pattern where both `worker-blocking` and `worker-reactive` are always on the classpath regardless of which stack the app uses. Beans are guarded by `@ConditionalOnProperty` + `OutboxConfiguredCondition`, so no outbox beans are created unless configured. No functional impact; acceptable given the project's existing all-in-one starter design.

**Required fix:** None.

## Open Questions

None.

## Verdict

`PASS`

All acceptance criteria are satisfied. `OutboxProperties` is correctly bound, null-safe, and wired. Outbox entries are merged into `TopicRegistry` with blank `kafkaTopic` and correctly scoped `claimLockName`. `OutboxConfiguredCondition` correctly reads the environment at condition-evaluation time (before beans are created). Both blocking and reactive outbox service beans are conditional and are absent when no outboxes are configured. Tests are comprehensive: they cover outbox-only mode, mixed Kafka+outbox mode, handler validation, shared vs. dedicated repository routing, and bean absence. Coverage aggregation is correctly updated.
