#!/usr/bin/env bash
# Levanta todo el sistema con un solo comando: ./levantar.sh
set -euo pipefail
cd "$(dirname "$0")"

export GIT_COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo dev)"

echo "[1/3] Servicios principales (MongoDB, API, Spark, Dask)..."
docker compose up -d --build

echo "[2/3] Jenkins..."
docker compose -p lifemap-jenkins -f jenkins/docker-compose.jenkins.yml up -d --build

echo "[3/3] Esperando a la API..."
for intento in $(seq 1 30); do
  if docker exec lifemap_api python -c "import urllib.request,sys; sys.exit(0 if b'conectado' in urllib.request.urlopen('http://localhost:5000/health', timeout=3).read() else 1)" >/dev/null 2>&1; then
    echo "API lista y MongoDB conectado (commit $GIT_COMMIT)."
    docker compose ps
    docker compose -p lifemap-jenkins -f jenkins/docker-compose.jenkins.yml ps
    exit 0
  fi
  sleep 2
done

echo "La API no respondio en 60 s. Revisar: docker logs lifemap_api" >&2
exit 1
