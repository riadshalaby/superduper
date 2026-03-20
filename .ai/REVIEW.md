# Review: T-006 — Outbox examples in multitopic apps (shared + dedicated)

- Reviewer: claude
- Date (UTC): 2026-03-20
- Commit reviewed: `087aa18c204260e740cbcdc069e07bb16b652206`

Review Round: **1**

## Acceptance Criteria Checklist

| Criterion | Status | Notes |
|---|---|---|
| `app-multitopic-shared` has 2 Kafka topics + notifications outbox sharing the default table | ✅ | YAML: no `table` on notifications outbox → shared `messages` table |
| `app-multitopic-dedicated` has 2 Kafka topics + notifications outbox with dedicated `outbox_notifications` table | ✅ | YAML: `table: outbox_notifications`; Liquibase changeset creates table |
| `superduper-outbox-blocking` dependency in both example POMs | ✅ | Lines 70-73 in each POM |
| `NotificationsMessageHandler` implements `MessageHandler`; registered as `notificationsMessageHandler` | ✅ | `@Component("notificationsMessageHandler")` in both apps |
| Handlers log processing for observability | ✅ | `[Notifications] id=... key=... attempt=... -> SUCCESS/FAILURE` logs |
| `OutboxSeedService.seedNotifications()` annotated `@Transactional` | ✅ | Demonstrates transactional outbox pattern |
| `MultitopicSeeder` calls `outboxSeedService.seedNotifications()` and includes outbox in expected count | ✅ | `expected = configuredTopics.size() * count` (3 topics × count) |
| Kafka producer loop skips outbox topics (blank kafkaTopic) | ✅ | Line 155: `if (topic.kafkaTopic().isBlank()) continue` |
| `application.yml` in each app has `superduper.outbox.notifications.*` | ✅ | Both YAMLs have handler; dedicated adds `table: outbox_notifications` |
| Liquibase changeset creates `outbox_notifications` (postgres + mariadb) | ✅ | `outbox-notifications.yaml` uses shared templates; both DBMS variants |
| `db.changelog-dedicated.yaml` includes new changeset | ✅ | Line 11-12 |
| `DedicatedMultitopicLiquibaseIntegrationTest` asserts `outbox_notifications` exists, `messages` absent | ✅ | Line 49-50 |
| `mvn -q -DskipTests test-compile` passes | ✅ | Per implementer validation |
| `mvn -T 1C -q test` passes | ✅ | Per implementer validation |

## Findings

### SEV-3 (Info) — `keyFor("notifications", ...)` case in `MultitopicSeeder` is unreachable in the Kafka producer path

`MultitopicSeeder.keyFor()` has a `case "notifications"` branch, but `publishKafkaSeedMessages()` skips all topics with a blank `kafkaTopic`, which includes the outbox. The `keyFor()` method is only called inside that loop, so the `notifications` case is never invoked. It is harmless and may serve as documentation intent.

**Required fix:** None.

### SEV-3 (Info) — `NotificationsMessageHandler` is identical in both apps (no shared abstraction)

Both `app-multitopic-shared` and `app-multitopic-dedicated` have identical `NotificationsMessageHandler` implementations. Duplication is expected in example apps (they are intentionally self-contained), and the plan specifies this pattern. No production code is affected.

**Required fix:** None.

## Open Questions

None.

## Verdict

`PASS`

All acceptance criteria are satisfied. Both apps demonstrate the transactional outbox pattern correctly: `@Transactional` seeder, dedicated handler, shared vs. dedicated table configuration, Liquibase schema provisioning, and integration test coverage. The Kafka producer loop correctly skips outbox topics. The expected count formula correctly includes all three topic slots. Two SEV-3 info notes — neither requires a fix.
