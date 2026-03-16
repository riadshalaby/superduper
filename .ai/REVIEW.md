# Review — T-004: Public Interface JavaDoc

- **Verdict:** PASS_WITH_NOTES
- **Reviewer:** claude
- **Reviewed at (UTC):** 2026-03-16T20:40Z
- **Commit reviewed:** 8de8fde — `docs: add public API javadocs`

---

## Findings (ordered by severity)

### NOTE — `TopicRegistryView.getByKafkaTopic` missing `@throws` for not-found case

**File:** `repository-api/.../TopicRegistryView.java` (line 27)

Implementations throw `IllegalArgumentException` when the Kafka topic is not registered (visible in `TopicRegistry` and verified by `TopicRegistryTest`). The interface JavaDoc does not document this. Callers have no way to know the method throws rather than returning `null` on a miss.

**Recommended fix:**
```java
* @throws IllegalArgumentException if no topic is registered for the given Kafka topic name
```

---

### NOTE — `ReactiveMessageHandler.handle` should document `Mono.error` error signalling contract

**File:** `worker-reactive/.../ReactiveMessageHandler.java` (line 13)

The current `@return` says *"a publisher that emits the processing outcome"*, but reactive callers need to know how errors are signalled. In a reactive pipeline, the handler must **not** throw a synchronous exception — it must return `Mono.error(...)` instead. Without this documented, implementors may mistakenly throw from `handle()`, bypassing the reactive error-handling chain.

**Recommended fix — extend `@return` or add `@throws`-equivalent note:**
```java
* @return a publisher that emits the processing outcome, or signals an error via
*     {@link reactor.core.publisher.Mono#error Mono.error} when processing cannot complete
```

---

## Plan Compliance Checklist — All 15 Interfaces

| Interface | Class Doc | Methods | All @param | All @return | @throws where needed | Status |
|---|---|---|---|---|---|---|
| `SuperduperObserver` | ✅ | 13/13 | ✅ | ✅ | ✅ (`consumerFailed`, `workerFailed`, `maintenanceFailed`) | PASS |
| `ConsumerMetadataResolver` | ✅ | 4/4 | ✅ | ✅ | n/a | PASS |
| `MessageIngestRepository` | ✅ | 3/3 | ✅ | ✅ | ✅ (`batchUpsertReadyMessages` NullPointerException) | PASS |
| `ReactiveMessageIngestRepository` | ✅ | 3/3 | ✅ | ✅ | ✅ (`batchUpsertReadyMessages` NullPointerException) | PASS |
| `ReactiveWorkerMaintenanceRepository` | ✅ | 9/9 | ✅ | ✅ | n/a | PASS |
| `ReactiveWorkerMessageRepository` | ✅ | 12/12 | ✅ | ✅ | n/a | PASS |
| `TopicConfigView` | ✅ | 7/7 | n/a (getters) | ✅ | n/a | PASS |
| `TopicRegistryView` | ✅ | 3/3 | ✅ | ✅ | NOTE (missing not-found throws) | NOTE |
| `TopicRepositoryFactory` | ✅ | 6/6 | ✅ | ✅ | n/a | PASS |
| `WorkerMaintenanceRepository` | ✅ | 9/9 | ✅ | ✅ | n/a | PASS |
| `WorkerMessageRepository` | ✅ | 12/12 | ✅ | ✅ | n/a | PASS |
| `JdbcSqlDialect` | ✅ | 17/17 | n/a (no params) | ✅ | n/a | PASS |
| `R2dbcSqlDialect` | ✅ | 17/17 | n/a (no params) | ✅ | n/a | PASS |
| `MessageHandler` | ✅ | 1/1 | ✅ | ✅ | ✅ (`MessageHandlingException`) | PASS |
| `ReactiveMessageHandler` | ✅ | 1/1 | ✅ | ✅ | NOTE (Mono.error contract undocumented) | NOTE |
| **Total** | **15/15** | **~117/~117** | ✅ | ✅ | 13/15 fully ✅ | **PASS_WITH_NOTES** |

---

## Quality Assessment

**Accuracy:** All descriptions correctly reflect the method's behaviour as implemented. No copy-paste errors found between blocking/reactive pairs — each has appropriately adapted wording (e.g., "returns" vs. "emits", "completion when" for `Mono<Void>` returns).

**Terminology consistency:** Consistent use of project vocabulary throughout — `claim`, `topic`, `READY`, `PROCESSING`, `STOPPED`, `FAILED`, `PROCESSED`, `containerId`, `workerId`, `orphanTimeoutSec`. No mixing of "lock"/"claim" or "queue"/"topic".

**`{@code}` usage:** Status names, boolean literals, and `null` are correctly wrapped: `{@code READY}`, `{@code true}`, `{@code null}`. Clean and readable in generated Javadoc.

**Class-level docs:** All 15 interfaces have meaningful one-to-two sentence class docs that establish the port's role in the system.

**Default-method defaults:** Default methods that delegate to the topic-scoped overload correctly document the "default topic" behaviour, giving callers a clear picture of what the no-topic overload does.

**Language:** All JavaDoc is in English. No German or other non-English text introduced.

**Spotless:** Passed (`mvn -q spotless:apply` reported no diff).

---

## Required Fixes

Two small JavaDoc additions, addressable in a single follow-up commit:

1. `TopicRegistryView.getByKafkaTopic` — add `@throws IllegalArgumentException if no topic is registered for the given Kafka topic name`.
2. `ReactiveMessageHandler.handle` — extend `@return` to document `Mono.error` as the error-signalling contract (no synchronous throws).

Neither fix requires a new review cycle; they can be applied and verified by the implementer.

---

## Summary

All 15 public interfaces have class-level and method-level JavaDoc with accurate summaries, consistent terminology, and proper `@param`/`@return`/`@throws` tags. Two small omissions noted: a missing `@throws` for the not-found case on `TopicRegistryView.getByKafkaTopic`, and an undocumented `Mono.error` error-signalling contract on `ReactiveMessageHandler.handle`. Verdict **PASS_WITH_NOTES**.
