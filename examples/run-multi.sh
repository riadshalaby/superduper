#!/usr/bin/env bash
set -euo pipefail

echo "Building jars..."
mvn -DskipTests -q package

echo "Building Docker images and starting services..."
docker compose -f docker-compose.multi.yml up --build -d

echo ""
echo "Services:"
echo "  Kafka UI:  http://localhost:8089"
echo "  Adminer:   http://localhost:8090  (System: PostgreSQL, Server: postgres, User: superduper, Password: superduper, DB: superduper)"
echo ""
echo "To follow seeder logs:  docker compose -f docker-compose.multi.yml logs -f seeder-1 seeder-2 seeder-3"
echo "To follow worker logs:  docker compose -f docker-compose.multi.yml logs -f worker-1 worker-2 worker-3"
