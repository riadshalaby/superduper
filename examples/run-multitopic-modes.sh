#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.multitopic.yml"
MAX_INSTANCES=5

usage() {
  echo "Usage:"
  echo "  $0 start --mode shared|dedicated [--count N] [--seeder-count N]"
  echo "  $0 stop"
  echo "  $0 down [--volumes]"
  echo ""
  echo "Options:"
  echo "  --mode MODE       Worker example mode: shared or dedicated (required for start)"
  echo "  --count N         Number of worker instances to run (1..$MAX_INSTANCES, default: 1)"
  echo "  --seeder-count N  Messages per topic from the embedded seeder (default: 500)"
  echo "  --volumes         With 'down', also delete volumes"
}

run_compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

print_shared_queries() {
  local per_topic="$1"
  local total="$2"
  local processed="$3"
  local stopped="$4"

  cat <<EOF
Shared mode verification SQL:
  SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events');  -- expect $total
  SELECT topic, COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') GROUP BY topic ORDER BY topic;  -- expect $per_topic per topic
  SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') AND status = 'PROCESSED';  -- expect $processed
  SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') AND status = 'STOPPED';  -- expect $stopped
  SELECT COUNT(*) FROM messages WHERE topic IN ('orders.events', 'invoices.events') AND status IN ('READY', 'PROCESSING', 'FAILED');  -- expect 0
EOF
}

print_dedicated_queries() {
  local per_topic="$1"
  local processed="$2"
  local stopped="$3"

  cat <<EOF
Dedicated mode verification SQL:
  SELECT COUNT(*) FROM orders_messages WHERE topic = 'orders.events';  -- expect $per_topic
  SELECT COUNT(*) FROM invoices_messages WHERE topic = 'invoices.events';  -- expect $per_topic
  SELECT to_regclass('public.messages');  -- expect null (no shared messages table)
  SELECT (SELECT COUNT(*) FROM orders_messages WHERE status = 'PROCESSED') + (SELECT COUNT(*) FROM invoices_messages WHERE status = 'PROCESSED');  -- expect $processed
  SELECT (SELECT COUNT(*) FROM orders_messages WHERE status = 'STOPPED') + (SELECT COUNT(*) FROM invoices_messages WHERE status = 'STOPPED');  -- expect $stopped
  SELECT (SELECT COUNT(*) FROM orders_messages WHERE status IN ('READY', 'PROCESSING', 'FAILED')) + (SELECT COUNT(*) FROM invoices_messages WHERE status IN ('READY', 'PROCESSING', 'FAILED'));  -- expect 0
EOF
}

cmd="${1:-start}"
if [[ $# -gt 0 ]]; then
  shift
fi

case "$cmd" in
  start)
    instance_count="${SUPERDUPER_INSTANCE_COUNT:-1}"
    mode="${SUPERDUPER_EXAMPLE_MODE:-}"
    seeder_count="${SUPERDUPER_SEEDER_COUNT:-500}"

    while [[ $# -gt 0 ]]; do
      case "$1" in
        --count|-c)
          instance_count="${2:-}"
          shift 2
          ;;
        --mode|-m)
          mode="${2:-}"
          shift 2
          ;;
        --seeder-count|-s)
          seeder_count="${2:-}"
          shift 2
          ;;
        --help|-h)
          usage
          exit 0
          ;;
        *)
          echo "Unknown argument for start: $1" >&2
          usage
          exit 1
          ;;
      esac
    done

    if [[ ! "$instance_count" =~ ^[0-9]+$ ]] || [[ "$instance_count" -lt 1 ]] || [[ "$instance_count" -gt "$MAX_INSTANCES" ]]; then
      echo "Invalid --count: '$instance_count'. Expected an integer in 1..$MAX_INSTANCES." >&2
      exit 1
    fi
    if [[ ! "$seeder_count" =~ ^[0-9]+$ ]]; then
      echo "Invalid --seeder-count: '$seeder_count'. Expected a non-negative integer." >&2
      exit 1
    fi

    case "$mode" in
      shared)
        worker_context="./examples/app-multitopic-shared"
        worker_group="example-multitopic-shared"
        ;;
      dedicated)
        worker_context="./examples/app-multitopic-dedicated"
        worker_group="example-multitopic-dedicated"
        ;;
      *)
        echo "Invalid --mode: '$mode'. Expected 'shared' or 'dedicated'." >&2
        exit 1
        ;;
    esac

    echo "Building jars..."
    mvn -DskipTests -q package

    base_services=(zookeeper kafka kafka-init kafka-ui postgres adminer prometheus grafana)
    all_workers=(worker-1 worker-2 worker-3 worker-4 worker-5)
    selected_workers=("${all_workers[@]:0:instance_count}")
    selected_services=("${base_services[@]}" "${selected_workers[@]}")
    disabled_services=()
    for ((i = instance_count; i < ${#all_workers[@]}; i++)); do
      disabled_services+=("${all_workers[i]}")
    done

    if [[ ${#disabled_services[@]} -gt 0 ]]; then
      run_compose rm -sf "${disabled_services[@]}" >/dev/null 2>&1 || true
    fi

    total_messages=$((seeder_count * 2))
    stopped_messages=$((total_messages / 40))
    processed_messages=$((total_messages - stopped_messages))

    echo "Starting services (mode=$mode, workers=$instance_count, SUPERDUPER_SEEDER_COUNT=$seeder_count per topic)..."
    SUPERDUPER_SEEDER_COUNT="$seeder_count" \
    SUPERDUPER_WORKER_CONTEXT="$worker_context" \
    SUPERDUPER_WORKER_GROUP_ID="$worker_group" \
      run_compose up --build -d "${selected_services[@]}"

    echo ""
    echo "Services:"
    echo "  Kafka UI:    http://localhost:8089"
    echo "  Adminer:     http://localhost:8090  (System: PostgreSQL, Server: postgres, User: superduper, Password: superduper, DB: superduper)"
    echo "  Prometheus:  http://localhost:9090"
    echo "  Grafana:     http://localhost:3000"
    echo ""
    echo "Logs:"
    echo "  docker compose -f $COMPOSE_FILE logs -f worker-1"
    echo "  docker compose -f $COMPOSE_FILE logs -f ${selected_workers[*]}"
    echo ""
    if [[ "$mode" == "shared" ]]; then
      print_shared_queries "$seeder_count" "$total_messages" "$processed_messages" "$stopped_messages"
    else
      print_dedicated_queries "$seeder_count" "$processed_messages" "$stopped_messages"
    fi
    ;;

  stop)
    if [[ ${1:-} == "--help" || ${1:-} == "-h" ]]; then
      usage
      exit 0
    fi
    run_compose stop
    ;;

  down)
    remove_volumes="false"
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --volumes|-v)
          remove_volumes="true"
          shift
          ;;
        --help|-h)
          usage
          exit 0
          ;;
        *)
          echo "Unknown argument for down: $1" >&2
          usage
          exit 1
          ;;
      esac
    done

    if [[ "$remove_volumes" == "true" ]]; then
      run_compose down -v --remove-orphans
    else
      run_compose down --remove-orphans
    fi
    ;;

  --help|-h)
    usage
    ;;

  *)
    echo "Unknown command: $cmd" >&2
    usage
    exit 1
    ;;
esac
