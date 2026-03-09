#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.multi.yml"
MAX_INSTANCES=5

usage() {
  echo "Usage:"
  echo "  $0 start [--count N] [--mode blocking|reactive] [--seeder-count N]"
  echo "  $0 stop"
  echo "  $0 down [--volumes]"
  echo ""
  echo "Options:"
  echo "  --count N         Number of worker/seeder instances to run (1..$MAX_INSTANCES, default: 3)"
  echo "  --mode MODE       Worker example mode: blocking or reactive (default: blocking)"
  echo "  --seeder-count N  SUPERDUPER_SEEDER_COUNT per seeder (default: 1000)"
  echo "  --volumes         With 'down', also delete volumes"
}

run_compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

if [[ $# -gt 0 && "${1:-}" =~ ^[0-9]+$ ]]; then
  set -- start --seeder-count "$1"
fi

cmd="${1:-start}"
if [[ $# -gt 0 ]]; then
  shift
fi

case "$cmd" in
  start)
    instance_count="${SUPERDUPER_INSTANCE_COUNT:-3}"
    mode="${SUPERDUPER_EXAMPLE_MODE:-blocking}"
    seeder_count="${SUPERDUPER_SEEDER_COUNT:-1000}"

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
      blocking)
        worker_context="./examples/app-blocking"
        worker_group="example-blocking"
        ;;
      reactive)
        worker_context="./examples/app-reactive"
        worker_group="example-reactive"
        ;;
      *)
        echo "Invalid --mode: '$mode'. Expected 'blocking' or 'reactive'." >&2
        exit 1
        ;;
    esac

    echo "Building jars..."
    mvn -DskipTests -q package

    base_services=(zookeeper kafka kafka-init kafka-ui postgres adminer prometheus grafana)
    all_seeders=(seeder-1 seeder-2 seeder-3 seeder-4 seeder-5)
    all_workers=(worker-1 worker-2 worker-3 worker-4 worker-5)
    selected_seeders=("${all_seeders[@]:0:instance_count}")
    selected_workers=("${all_workers[@]:0:instance_count}")
    selected_services=("${base_services[@]}" "${selected_seeders[@]}" "${selected_workers[@]}")
    disabled_services=()
    for ((i = instance_count; i < ${#all_seeders[@]}; i++)); do
      disabled_services+=("${all_seeders[i]}")
    done
    for ((i = instance_count; i < ${#all_workers[@]}; i++)); do
      disabled_services+=("${all_workers[i]}")
    done

    if [[ ${#disabled_services[@]} -gt 0 ]]; then
      run_compose rm -sf "${disabled_services[@]}" >/dev/null 2>&1 || true
    fi

    echo "Starting services (mode=$mode, instances=$instance_count, SUPERDUPER_SEEDER_COUNT=$seeder_count)..."
    SUPERDUPER_SEEDER_COUNT="$seeder_count" \
    SUPERDUPER_WORKER_CONTEXT="$worker_context" \
    SUPERDUPER_WORKER_GROUP_ID="$worker_group" \
      run_compose up --build -d "${selected_services[@]}"

    echo ""
    echo "Services:"
    echo "  Kafka UI:  http://localhost:8089"
    echo "  Adminer:   http://localhost:8090  (System: PostgreSQL, Server: postgres, User: superduper, Password: superduper, DB: superduper)"
    echo "  Prometheus: http://localhost:9090"
    echo "  Grafana:    http://localhost:3000"
    echo ""
    echo "To follow seeder logs:  docker compose -f $COMPOSE_FILE logs -f ${selected_seeders[*]}"
    echo "To follow worker logs:  docker compose -f $COMPOSE_FILE logs -f ${selected_workers[*]}"
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
