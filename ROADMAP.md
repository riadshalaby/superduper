# ROADMAP

Goal: improve worker throughput, keep ordering guarantees, and clean up message metadata extensibility.

## Priority 1: Worker Throughput and Batch Semantics

- Current behavior: the current SQL claim strategy (strict per-key oldest-pending filter) effectively claims only one message per key per worker run. If 100 messages share the same key, only 1 message is processed per schedule cycle.
- Target behavior: when at least `batchSize` messages are available, the worker should claim and process up to `batchSize` messages per run, including large same-key backlogs.
- Design rule: if one message for a key fails and later messages of the same key are already inside the current batch, handling of those later messages must be done inside worker batch logic, not through SQL-level key blocking.
- Required validation: add integration tests for same-key batches with partial failure, and add performance tests to verify throughput improvements under hot-key load.

## Priority 2: Message Schema and Naming Cleanup

- Rename `uuid` to `message_id`.
- Rename `key` to `message_key`
- Add `correlation_id` to the message.
- Add `message_type` (string) to the message.

## Priority 3: Consumer Metadata SPI

- Add a consumer SPI so library users can provide `occurredAt`, `message_id`, `correlationId`, and `message_type`.
- Provide a default SPI implementation with this behavior:
- `occurredAt`: `now()`.
- `message_id`: header `"message_id"` if present; otherwise deterministic UUID from `topic:partition:offset`.
- `correlationId`: header `"correlationId"` if present; otherwise new UUID.
- `message_type`: header `"message_type"` if present; otherwise `null`.
