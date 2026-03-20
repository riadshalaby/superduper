# Review: T-003 — outbox-blocking module (OutboxService + JdbcOutboxService)

- Reviewer: claude
- Date (UTC): 2026-03-20
- Commit reviewed: `37d6539acf2fb25cce0b25b78ba1a427544f0699`

Review Round: **1**

## Acceptance Criteria Checklist

| Criterion | Status | Notes |
|---|---|---|
| Module `outbox-blocking` in parent POM `<modules>` (after `consumer-kafka-reactive`) | ✅ | Line 46 in `pom.xml` |
| `superduper-outbox-blocking` in parent POM `<dependencyManagement>` | ✅ | Line 156 in `pom.xml` |
| `OutboxService` interface with both `send()` overloads | ✅ | Correct signatures + Javadoc |
| `JdbcOutboxService` validates outbox name, throws `IllegalArgumentException` | ✅ | Line 36-38 |
| `JdbcOutboxService` generates UUID `message_id` | ✅ | `UUID.randomUUID().toString()` |
| `JdbcOutboxService` routes to correct repository by outbox name | ✅ | `repositories.get(outboxName)` |
| `JdbcOutboxService` calls `upsertReadyMessage()` with `outboxName` as topic column | ✅ | Line 41 |
| Simple overload delegates with `occurredAt = Instant.now()`, null optional fields | ✅ | Line 24 |
| Unit test: routing + UUID is valid UUID | ✅ | `send_routesToNamedRepositoryWithGeneratedUuid` |
| Unit test: simple overload `occurredAt` in range, null metadata | ✅ | `send_simpleOverloadUsesCurrentInstantAndNullOptionalMetadata` |
| Unit test: unknown outbox name throws `IllegalArgumentException` | ✅ | `send_throwsForUnknownOutboxName` |
| Unit test: multi-outbox routing, non-target repo never called | ✅ | `send_routesEachOutboxToItsMatchingRepository` |
| `mvn -q -DskipTests test-compile` passes | ✅ | Per implementer validation |
| `mvn -T 1C -q test` passes | ✅ | Per implementer validation |

## Findings

### SEV-3 (Info) — `observer` field stored but never used; suppressed with `@SuppressWarnings`

`JdbcOutboxService` accepts a `SuperduperObserver` in its constructor and null-coalesces it, but never calls any observer method in `send()`. The `@SuppressWarnings("unused")` annotation silences the compiler warning. This is forward-compatible (a placeholder for future observability), but the annotation is a mild code smell that signals the field has no runtime effect in this cycle.

**Impact:** No functional issue. Observer is harmlessly no-op.
**Required fix:** None. Observability hooks are not required by T-003 acceptance criteria.

### SEV-3 (Info) — `superduper-repository-jdbc` listed as a compile dependency

`outbox-blocking/pom.xml` depends on `superduper-repository-jdbc` at compile scope, but `JdbcOutboxService` only uses `MessageIngestRepository` from `superduper-repository-api`. No classes from `repository-jdbc` are directly imported. This matches the plan's specified dependencies, but could be tightened in a future cleanup.

**Impact:** None — the dependency is harmless but slightly broader than what the module actually uses.
**Required fix:** None.

### SEV-3 (Info) — Null `outboxName` throws `NullPointerException`, not `IllegalArgumentException`

`Objects.requireNonNull(outboxName, "outboxName")` throws `NullPointerException` for a null outbox name, while an unknown-but-non-null name throws `IllegalArgumentException`. Both are appropriate runtime exceptions, and the plan's acceptance criterion only specifies the `IllegalArgumentException` for an unknown name (not the null case). No test for null outbox name is present, which is consistent with the plan.

**Impact:** None. Behavior is correct and consistent with standard Java contracts.
**Required fix:** None.

## Open Questions

None.

## Verdict

`PASS`

All acceptance criteria are met. The module, interface, implementation, and four unit tests precisely match the plan. `JdbcOutboxService` correctly validates the outbox name, generates UUID message IDs, routes to the pre-resolved repository, and passes `outboxName` as the topic column value. Three SEV-3 info notes — none require fixes.
