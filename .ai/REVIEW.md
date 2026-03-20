# Review: T-004 — outbox-reactive module (ReactiveOutboxService + R2dbcOutboxService)

- Reviewer: claude
- Date (UTC): 2026-03-20
- Commit reviewed: `9cf765a6a90cae0a85f9b3664fb45570d8ccc40f`

Review Round: **1**

## Acceptance Criteria Checklist

| Criterion | Status | Notes |
|---|---|---|
| Module `outbox-reactive` in parent POM `<modules>` (after `outbox-blocking`) | ✅ | Line 47 in `pom.xml` |
| `superduper-outbox-reactive` in parent POM `<dependencyManagement>` | ✅ | Line 162 in `pom.xml` |
| `ReactiveOutboxService` interface with both `Mono<Void> send()` overloads | ✅ | Correct signatures + Javadoc |
| `R2dbcOutboxService` mirrors blocking impl with reactive semantics | ✅ | `Map.copyOf`, null-coalesced observer, `UUID.randomUUID()` |
| Unknown outbox returns `Mono.error(IllegalArgumentException)` — not a synchronous throw | ✅ | Line 38-40: correct reactive error propagation |
| `outboxName` passed as topic column value | ✅ | Line 43: first arg to `upsertReadyMessage` |
| Simple overload delegates with `Instant.now()`, null optional fields | ✅ | Line 25 |
| Unit test: routing + UUID valid + full overload verified via StepVerifier | ✅ | `send_routesToNamedRepositoryWithGeneratedUuid` |
| Unit test: simple overload `occurredAt` in range, null metadata | ✅ | `send_simpleOverloadUsesCurrentInstantAndNullOptionalMetadata` |
| Unit test: unknown outbox name propagates as reactive error | ✅ | `send_throwsForUnknownOutboxName` with `expectErrorSatisfies` |
| Unit test: multi-outbox routing, non-target repo never called | ✅ | `send_routesEachOutboxToItsMatchingRepository` |
| `mvn -q -DskipTests test-compile` passes | ✅ | Per implementer validation |
| `mvn -T 1C -q test` passes | ✅ | Per implementer validation |

## Findings

### SEV-3 (Info) — `observer` field stored but never used; same pattern as T-003

Identical to the T-003 finding. `@SuppressWarnings("unused")` silences the compiler for a placeholder field. Forward-compatible, no functional impact.

**Required fix:** None.

### SEV-3 (Info) — `superduper-repository-r2dbc` and `r2dbc-postgresql` dependencies broader than strictly needed

`R2dbcOutboxService` uses only `ReactiveMessageIngestRepository` from `superduper-repository-api`. The two additional compile-scope dependencies match the plan specification and are consistent with `outbox-blocking`'s same pattern. Can be tightened in a future cleanup cycle.

**Required fix:** None.

### SEV-3 (Info) — `Instant.now()` is eagerly evaluated in simple `send()` overload

`Mono.defer` is not used; `Instant.now()` runs when the `Mono` is assembled (at call time), not at subscription time. This is functionally correct for the outbox use case (caller expects the timestamp to be captured at send-call time, not at database commit time) and is consistent with the blocking variant. The test validates the time-range constraint correctly.

**Required fix:** None.

### SEV-3 (Info) — Timestamp assertion in test wraps value in `Mono.just()` + StepVerifier unnecessarily

`send_simpleOverloadUsesCurrentInstantAndNullOptionalMetadata` wraps the `occurredAt` assertion inside `StepVerifier.create(Mono.just(...))` when a direct AssertJ assertion would be simpler and clearer. Functionally equivalent.

**Required fix:** None.

## Open Questions

None.

## Verdict

`PASS`

All acceptance criteria are met. The module is a faithful reactive mirror of `outbox-blocking`. The key reactive correctness requirement — returning `Mono.error()` instead of throwing synchronously for an unknown outbox name — is implemented correctly and verified by `StepVerifier.expectErrorSatisfies`. Four SEV-3 info notes; none require fixes.
