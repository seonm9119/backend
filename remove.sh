#!/bin/bash

set -e

echo "⚠️  WARNING: This will REMOVE ALL Docker containers, images, networks, and volumes."
echo "⚠️  This action is IRREVERSIBLE."
read -p "Are you sure you want to continue? (yes/no): " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
  echo "Aborted."
  exit 1
fi

echo "🔴 Stopping all containers..."
docker ps -aq | xargs -r docker stop

echo "🗑 Removing all containers..."
docker ps -aq | xargs -r docker rm -f

echo "🗑 Removing all images..."
docker images -aq | xargs -r docker rmi -f

echo "🧹 Removing all networks (except default)..."
docker network ls -q \
  | grep -v -E "$(docker network inspect bridge host none -f '{{.Id}}' | tr '\n' '|')" \
  | xargs -r docker network rm

echo "🧹 Removing all volumes..."
docker volume ls -q | xargs -r docker volume rm -f

echo "🧼 Docker system prune (final cleanup)..."
docker system prune -af --volumes

echo "✅ Docker cleanup completed."
