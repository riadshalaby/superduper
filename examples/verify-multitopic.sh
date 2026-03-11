#!/usr/bin/env bash

set -euo pipefail

MODE=""
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-superduper}"
PGPASSWORD="${PGPASSWORD:-superduper}"
PGDATABASE="${PGDATABASE:-superduper}"

usage() {
  cat <<'EOF'
Usage: ./examples/verify-multitopic.sh --mode shared|dedicated

Environment overrides:
  PGHOST
  PGPORT
  PGUSER
  PGPASSWORD
  PGDATABASE
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$MODE" != "shared" && "$MODE" != "dedicated" ]]; then
  echo "Expected --mode shared or --mode dedicated" >&2
  usage >&2
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required but was not found in PATH" >&2
  exit 1
fi

export PGPASSWORD

run_sql() {
  psql \
    --host "$PGHOST" \
    --port "$PGPORT" \
    --username "$PGUSER" \
    --dbname "$PGDATABASE" \
    --no-align \
    --tuples-only \
    --quiet \
    --set ON_ERROR_STOP=1 \
    --command "$1"
}

pass_count=0
fail_count=0

assert_eq() {
  local label="$1"
  local expected="$2"
  local sql="$3"
  local actual
  actual="$(run_sql "$sql" | tr -d '[:space:]')"
  if [[ "$actual" == "$expected" ]]; then
    echo "PASS ${label}: ${actual}"
    pass_count=$((pass_count + 1))
  else
    echo "FAIL ${label}: expected=${expected} actual=${actual}" >&2
    fail_count=$((fail_count + 1))
  fi
}

assert_zero_rows() {
  local label="$1"
  local sql="$2"
  assert_eq "$label" "0" "$sql"
}

if [[ "$MODE" == "shared" ]]; then
  assert_eq "shared total rows" "1000" \
    "SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events');"
  assert_eq "shared orders rows" "500" \
    "SELECT COUNT(*) FROM messages WHERE topic = 'orders.events';"
  assert_eq "shared invoices rows" "500" \
    "SELECT COUNT(*) FROM messages WHERE topic = 'invoices.events';"
  assert_eq "shared processed rows" "975" \
    "SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') AND status = 'PROCESSED';"
  assert_eq "shared stopped rows" "25" \
    "SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') AND status = 'STOPPED';"
  assert_zero_rows "shared in-flight rows" \
    "SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') AND status IN ('READY', 'PROCESSING', 'FAILED');"
  assert_zero_rows "shared ordering check" \
    "WITH ordered AS (
       SELECT topic, message_key, id,
              LAG(id) OVER (PARTITION BY topic, message_key ORDER BY id) AS prev_id
       FROM messages
       WHERE topic IN ('orders.events', 'invoices.events')
     )
     SELECT COUNT(*) FROM ordered WHERE prev_id IS NOT NULL AND id <= prev_id;"
else
  assert_eq "dedicated orders rows" "500" \
    "SELECT COUNT(*) FROM orders_messages WHERE topic = 'orders.events';"
  assert_eq "dedicated invoices rows" "500" \
    "SELECT COUNT(*) FROM invoices_messages WHERE topic = 'invoices.events';"
  assert_eq "dedicated processed rows" "975" \
    "SELECT
       (SELECT COUNT(*) FROM orders_messages WHERE topic = 'orders.events' AND status = 'PROCESSED')
       +
       (SELECT COUNT(*) FROM invoices_messages WHERE topic = 'invoices.events' AND status = 'PROCESSED');"
  assert_eq "dedicated stopped rows" "25" \
    "SELECT
       (SELECT COUNT(*) FROM orders_messages WHERE topic = 'orders.events' AND status = 'STOPPED')
       +
       (SELECT COUNT(*) FROM invoices_messages WHERE topic = 'invoices.events' AND status = 'STOPPED');"
  assert_eq "dedicated shared table unused" "0" \
    "SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events');"
  assert_zero_rows "dedicated orders in-flight rows" \
    "SELECT COUNT(*) FROM orders_messages WHERE topic = 'orders.events' AND status IN ('READY', 'PROCESSING', 'FAILED');"
  assert_zero_rows "dedicated invoices in-flight rows" \
    "SELECT COUNT(*) FROM invoices_messages WHERE topic = 'invoices.events' AND status IN ('READY', 'PROCESSING', 'FAILED');"
  assert_zero_rows "dedicated orders ordering check" \
    "WITH ordered AS (
       SELECT message_key, id,
              LAG(id) OVER (PARTITION BY message_key ORDER BY id) AS prev_id
       FROM orders_messages
       WHERE topic = 'orders.events'
     )
     SELECT COUNT(*) FROM ordered WHERE prev_id IS NOT NULL AND id <= prev_id;"
  assert_zero_rows "dedicated invoices ordering check" \
    "WITH ordered AS (
       SELECT message_key, id,
              LAG(id) OVER (PARTITION BY message_key ORDER BY id) AS prev_id
       FROM invoices_messages
       WHERE topic = 'invoices.events'
     )
     SELECT COUNT(*) FROM ordered WHERE prev_id IS NOT NULL AND id <= prev_id;"
fi

if [[ $fail_count -eq 0 ]]; then
  echo "PASS all ${pass_count} assertions for mode=${MODE}"
else
  echo "FAIL ${fail_count} assertion(s) failed for mode=${MODE}" >&2
  exit 1
fi
