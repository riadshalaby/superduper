# Examples

> See [README.md](../README.md) for the library overview and [docs/USAGE.md](USAGE.md) for integration details.

## Running Locally (docker-compose)

1. Start infrastructure:
   ```bash
   docker compose up -d
   ```
2. Run the JDBC example app:
   ```bash
   mvn -pl examples/app-blocking -am spring-boot:run
   ```
3. Or run the reactive example app:
   ```bash
   mvn -pl examples/app-reactive -am spring-boot:run
   ```
4. Send a message to Kafka through Kafka UI at `http://localhost:8089`:
   - Topic: `superduper.example`
   - Key: `k1`
   - Value: `{ "hello": "world" }`
5. Inspect the database:
   ```sql
   SELECT id, message_key, content, status, retry_count
   FROM messages
   ORDER BY message_key, id;
   ```

Operator endpoints:

```bash
curl http://localhost:8080/ops/queue
curl 'http://localhost:8080/ops/messages?status=FAILED&limit=10'
curl -X POST http://localhost:8080/ops/redrive/42
curl -X POST 'http://localhost:8080/ops/redrive?status=STOPPED&limit=50'
```

Expected response shapes:

- `GET /ops/queue`: `{"READY":12,"FAILED":1,"STOPPED":0,"PROCESSED":975}`
- `GET /ops/messages`: JSON array of `ClaimedMessage` rows
- `POST /ops/redrive/{id}`: `{"id":42,"redriven":true}`
- `POST /ops/redrive`: `{"status":"STOPPED","limit":50,"redriven":3}`

Observability stack:

```bash
curl http://localhost:8080/actuator/prometheus | grep superduper
```

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Dashboard: `http://localhost:3000/d/superduper-overview`

## Running Locally (Multi-Container Demo)

This mode runs:

- Up to 5 one-shot seeder containers (`seeder-1..5`) producing messages.
- Up to 5 worker containers (`worker-1..5`) consuming and processing cooperatively.
- Kafka UI and Adminer for manual verification.

Prerequisite:

```bash
mvn -DskipTests -q package
```

Start everything:

```bash
./examples/run-multi.sh start
./examples/run-multi.sh start --count 2 --mode blocking --seeder-count 5000
./examples/run-multi.sh start --count 2 --mode reactive --seeder-count 5000
```

Inspect services:

- Kafka UI: `http://localhost:8089`
- Adminer: `http://localhost:8090`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
  - System: `PostgreSQL`
  - Server: `postgres`
  - Username: `superduper`
  - Password: `superduper`
  - Database: `superduper`

Follow logs:

```bash
docker compose -f docker-compose.multi.yml logs -f seeder-1 seeder-2
docker compose -f docker-compose.multi.yml logs -f worker-1 worker-2
```

Stop and clean up:

```bash
./examples/run-multi.sh stop
./examples/run-multi.sh down
./examples/run-multi.sh down --volumes
```

The multi-container compose file provisions Prometheus against `worker-1` through `worker-5` and auto-loads the `Superduper Overview` dashboard in Grafana.

Seeder behavior in the multi-container demo:

- the seeder uses at least `count / 10` distinct keys, so `--seeder-count 5000` produces at least `500` keys
- configured `superduper.seeder.keys` is treated as a floor, not a hard cap
- only the dedicated key `order-0` receives failure payloads
- failure traffic is intentionally sparse: `retry-once` every `200th` message and `always-fail` every `1000th` message

## Simple Use Case (1000 Messages Demo)

Use this to observe consumer ingest and worker processing behavior.

1. Start infrastructure:
   ```bash
   docker compose up -d
   ```
2. Run one example app with built-in load seeding enabled:
   ```bash
   mvn -pl examples/app-blocking -am spring-boot:run \
     -Dspring-boot.run.jvmArguments="-Dsuperduper.example.seed.enabled=true"
   ```
   Or:
   ```bash
   mvn -pl examples/app-reactive -am spring-boot:run \
     -Dspring-boot.run.jvmArguments="-Dsuperduper.example.seed.enabled=true"
   ```
3. The example publishes `1000` records to `superduper.example` across `20` keys:
   - every 10th message: `retry-once #N`
   - every 40th message: `always-fail #N`
   - all others: `ok #N`
4. In the reactive example, startup also waits until the `ReactiveMessageHandler` has seen all seeded message ids, up to `superduper.example.seed.await-timeout-seconds`.

## Reproducible SQL Assertions

Use these assertions after the seeder completion log appears, or once this query returns `0`:

```sql
SELECT COUNT(*) AS still_in_flight
FROM messages
WHERE status IN ('READY', 'PROCESSING', 'FAILED');
```

### 1000-message demo (blocking and reactive)

The blocking and reactive example apps use the same seed pattern and the same `superduper.worker.max-retries=3`, so the final counts are identical.

Expected totals:

| Query | Expected result |
|---|---|
| `SELECT COUNT(*) FROM messages;` | `1000` |
| `SELECT COUNT(*) FROM messages WHERE status = 'PROCESSED';` | `975` |
| `SELECT COUNT(*) FROM messages WHERE status = 'STOPPED';` | `25` |
| `SELECT COUNT(*) FROM messages WHERE status IN ('READY', 'PROCESSING', 'FAILED');` | `0` |

Expected status distribution:

```sql
SELECT status, COUNT(*) AS c
FROM messages
GROUP BY status
ORDER BY status;
```

| status | c |
|---|---:|
| `PROCESSED` | `975` |
| `STOPPED` | `25` |

Expected retry distribution:

```sql
SELECT retry_count, COUNT(*) AS c
FROM messages
GROUP BY retry_count
ORDER BY retry_count;
```

| retry_count | c |
|---|---:|
| `0` | `900` |
| `1` | `75` |
| `3` | `25` |

Why those numbers:

- `25` messages match `i % 40 == 0` and always end in `STOPPED`.
- `75` messages match `i % 10 == 0` but not `i % 40 == 0`; they fail once, then succeed.
- The remaining `900` messages succeed on the first attempt.

### Multi-container demo (`instance_count = N`, `seeder_count = M`)

Each seeder publishes the same deterministic pattern. For `N` seeder instances and `M` messages per seeder:

| Metric | Formula |
|---|---|
| total ingested | `N * M` |
| `STOPPED` rows | `N * floor(M / 40)` |
| retry-once rows | `N * (floor(M / 10) - floor(M / 40))` |
| first-attempt success rows | `N * (M - floor(M / 10))` |
| `PROCESSED` rows | `N * (M - floor(M / 40))` |

Example for `./examples/run-multi.sh start --count 2 --seeder-count 5000`:

| Query | Expected result |
|---|---|
| `SELECT COUNT(*) FROM messages;` | `10000` |
| `SELECT COUNT(*) FROM messages WHERE status = 'PROCESSED';` | `9750` |
| `SELECT COUNT(*) FROM messages WHERE status = 'STOPPED';` | `250` |
| `SELECT COUNT(*) FROM messages WHERE status IN ('READY', 'PROCESSING', 'FAILED');` | `0` |

Expected status distribution for that example:

| status | c |
|---|---:|
| `PROCESSED` | `9750` |
| `STOPPED` | `250` |

Expected retry distribution for that example:

| retry_count | c |
|---|---:|
| `0` | `9000` |
| `1` | `750` |
| `3` | `250` |

### Per-key ordering assertion

This query must return no rows. If it returns any row, ordering for that key has been violated.

```sql
WITH ordered AS (
  SELECT
    id,
    message_key,
    status,
    retry_count,
    LAG(id) OVER (PARTITION BY message_key ORDER BY id) AS prev_id
  FROM messages
  WHERE message_key = 'order-7'
)
SELECT id, message_key, status, retry_count, prev_id
FROM ordered
WHERE prev_id IS NOT NULL
  AND id <= prev_id;
```

Expected output example:

```text
 id | message_key | status | retry_count | prev_id
----+-------------+--------+-------------+---------
(0 rows)
```

## Multi-Topic Examples

These examples use the blocking stack and start the same local infrastructure with two Kafka topics:

- `orders.events`
- `invoices.events`

### Shared-Table Mode (`app-multitopic-shared`)

Run it:

```bash
docker compose up -d
mvn -pl examples/app-multitopic-shared -am spring-boot:run \
  -Dspring-boot.run.jvmArguments="-Dsuperduper.example.seed.enabled=true"
```

Config highlights:

- both topics use the shared `messages` table
- no `table` field is configured under `superduper.topics`
- `/ops/queue?topic=orders.events` and `/ops/queue?topic=invoices.events` expose topic-scoped backlog snapshots

Expected shared-table SQL assertions:

| Query | Expected |
|---|---|
| `SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events');` | `1000` |
| `SELECT topic, COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') GROUP BY topic ORDER BY topic;` | `invoices.events: 500`, `orders.events: 500` |
| `SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') AND status = 'PROCESSED';` | `975` |
| `SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') AND status = 'STOPPED';` | `25` |

### Dedicated-Table Mode (`app-multitopic-dedicated`)

Run it:

```bash
docker compose up -d
mvn -pl examples/app-multitopic-dedicated -am spring-boot:run \
  -Dspring-boot.run.jvmArguments="-Dsuperduper.example.seed.enabled=true"
```

Config highlights:

- `orders.events` writes to `orders_messages`
- `invoices.events` writes to `invoices_messages`
- Liquibase loads `db.changelog-master.yaml` first, then the dedicated-table DDL in `db/changelog/multitopic-dedicated/db.changelog-dedicated.yaml`
- `/ops/queue?topic=orders.events` and `/ops/queue?topic=invoices.events` query the correct per-topic repository/table

Expected dedicated-table SQL assertions:

| Query | Expected |
|---|---|
| `SELECT COUNT(*) FROM orders_messages WHERE topic = 'orders.events';` | `500` |
| `SELECT COUNT(*) FROM invoices_messages WHERE topic = 'invoices.events';` | `500` |
| `SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events');` | `0` |
| `SELECT (SELECT COUNT(*) FROM orders_messages WHERE status = 'PROCESSED') + (SELECT COUNT(*) FROM invoices_messages WHERE status = 'PROCESSED');` | `975` |
| `SELECT (SELECT COUNT(*) FROM orders_messages WHERE status = 'STOPPED') + (SELECT COUNT(*) FROM invoices_messages WHERE status = 'STOPPED');` | `25` |

### Verification

Run the helper script after the seeder completion log appears:

```bash
./examples/verify-multitopic.sh --mode shared
./examples/verify-multitopic.sh --mode dedicated
```

The script connects to the local Postgres instance (`localhost:5432`, `superduper` / `superduper` by default), runs the documented assertions, and prints PASS or FAIL for each one. Override connection details with `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, or `PGDATABASE` when needed.

### SQL Assertions

Shared-table per-key ordering check:

```sql
WITH ordered AS (
  SELECT
    topic,
    message_key,
    id,
    LAG(id) OVER (PARTITION BY topic, message_key ORDER BY id) AS prev_id
  FROM messages
  WHERE topic IN ('orders.events', 'invoices.events')
)
SELECT topic, message_key, id, prev_id
FROM ordered
WHERE prev_id IS NOT NULL
  AND id <= prev_id;
```

Dedicated-table per-key ordering checks:

```sql
WITH ordered AS (
  SELECT
    message_key,
    id,
    LAG(id) OVER (PARTITION BY message_key ORDER BY id) AS prev_id
  FROM orders_messages
  WHERE topic = 'orders.events'
)
SELECT message_key, id, prev_id
FROM ordered
WHERE prev_id IS NOT NULL
  AND id <= prev_id;
```

```sql
WITH ordered AS (
  SELECT
    message_key,
    id,
    LAG(id) OVER (PARTITION BY message_key ORDER BY id) AS prev_id
  FROM invoices_messages
  WHERE topic = 'invoices.events'
)
SELECT message_key, id, prev_id
FROM ordered
WHERE prev_id IS NOT NULL
  AND id <= prev_id;
```

For a positive spot-check of the ordered sequence:

```sql
SELECT id, message_key, content, status, retry_count
FROM messages
WHERE message_key = 'order-7'
ORDER BY id
LIMIT 5;
```

Expected shape:

- `id` is strictly increasing.
- `content` follows `ok ... #7`, `ok ... #27`, `ok ... #47`, `ok ... #67`, `ok ... #87`.
- `status` is `PROCESSED` for every row.
- `retry_count` is `0` for every row.
