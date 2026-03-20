# TASKS

Use this board to coordinate manual handoff between planner, implementer, and reviewer.

Status values:
- `todo`
- `in_planning`
- `ready_for_implement`
- `in_implementation`
- `ready_for_review`
- `in_review`
- `changes_requested`
- `done`
- `blocked`

| Task ID | Scope | Planner Agent | Implementer Agent | Reviewer Agent | Status | Acceptance Criteria | Evidence | Next Role |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| T-001 | MariaDB content column TEXT to LONGTEXT | claude | codex | claude | done | Template uses LONGTEXT; migration changeset alters existing table; compile and tests pass | `mvn -q -DskipTests test-compile` ✅, `mvn -T 1C -q test` ✅ | — |
| T-002 | TopicConfigView evolution + worker/consumer alignment | claude | codex | claude | done | topicColumnValue() default method; kafkaTopics() filters blanks; workers and consumers use topicColumnValue(); all existing tests pass unchanged | `mvn -q -DskipTests test-compile` ✅, `mvn -T 1C -q test` ✅ | — |
| T-003 | outbox-blocking module (OutboxService + JdbcOutboxService) | claude | codex | claude | ready_for_review | New module in parent POM; OutboxService interface; JdbcOutboxService impl; unit tests for routing, UUID gen, validation; compile and tests pass | `mvn -q -DskipTests test-compile` ✅, `mvn -T 1C -q test` ✅ | review |
| T-004 | outbox-reactive module (ReactiveOutboxService + R2dbcOutboxService) | claude | codex | claude | ready_for_implement | New module in parent POM; ReactiveOutboxService interface; R2dbcOutboxService impl; unit tests with StepVerifier; compile and tests pass | n/a | implement |
| T-005 | Outbox configuration + TopicRegistry merge + starter wiring | claude | codex | claude | ready_for_implement | OutboxProperties config; outbox entries in TopicRegistry; OutboxService/ReactiveOutboxService beans wired; coverage aggregation; compile and tests pass | n/a | implement |
| T-006 | Outbox examples in multitopic apps (shared + dedicated) | claude | codex | claude | ready_for_implement | Both multitopic examples run with Kafka consumers + outbox services; outbox handler + seeder in each; @Transactional demo; shared and dedicated table variants; compile and tests pass | n/a | implement |
| T-007 | Documentation (ARCHITECTURE.md + README.md) | claude | codex | claude | ready_for_implement | ARCHITECTURE.md has outbox modules in Module Map, Dependency Graph, Data Flow, Extension Points; README.md has outbox description, usage snippet, and example references | n/a | implement |
| T-008 | 1.0.0 stable release promotion | claude | codex | claude | ready_for_implement | release-as 1.0.0 configured; README.md reflects stable status; all previous tasks done; compile and tests pass | n/a | implement |
