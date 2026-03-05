#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [SEEDER_COUNT]"
  echo ""
  echo "SEEDER_COUNT: non-negative integer for SUPERDUPER_SEEDER_COUNT (default: 1000)"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 1 ]]; then
  usage
  exit 1
fi

seeder_count="${1:-${SUPERDUPER_SEEDER_COUNT:-1000}}"
if [[ ! "$seeder_count" =~ ^[0-9]+$ ]]; then
  echo "Invalid SEEDER_COUNT: '$seeder_count'. Expected a non-negative integer." >&2
  exit 1
fi

echo "Building jars..."
mvn -DskipTests -q package

echo "Building Docker images and starting services (SUPERDUPER_SEEDER_COUNT=$seeder_count)..."
SUPERDUPER_SEEDER_COUNT="$seeder_count" docker compose -f docker-compose.multi.yml up --build -d

echo ""
echo "Services:"
echo "  Kafka UI:  http://localhost:8089"
echo "  Adminer:   http://localhost:8090  (System: PostgreSQL, Server: postgres, User: superduper, Password: superduper, DB: superduper)"
echo ""
echo "To follow seeder logs:  docker compose -f docker-compose.multi.yml logs -f seeder-1 seeder-2 seeder-3"
echo "To follow worker logs:  docker compose -f docker-compose.multi.yml logs -f worker-1 worker-2 worker-3"
