#!/usr/bin/env bash
# Phase 13 manual verification checklist (run from repo root).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "== 1-3: clean build and tests =="
./mvnw -B clean verify

echo "== 4-6: single broker persistence smoke =="
TMP_DATA="$(mktemp -d)"
export JAVASTREAM_STORAGE_DATA_DIR="$TMP_DATA"
export JAVASTREAM_NETWORK_LISTEN_PORT=19092
export JAVASTREAM_MONITORING_HTTP_PORT=18080

java -jar target/javastream-*-jar-with-dependencies.jar &
BROKER_PID=$!
trap 'kill -TERM "$BROKER_PID" 2>/dev/null || true; wait "$BROKER_PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:18080/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

export JAVASTREAM_EXAMPLE_BROKER_PORT=19092
./mvnw -q exec:java -Dexec.mainClass=com.javastream.examples.ProducerExample -Dexec.args=phase13-smoke
kill -TERM "$BROKER_PID"
wait "$BROKER_PID" || true
BROKER_PID=""

java -jar target/javastream-*-jar-with-dependencies.jar &
BROKER_PID=$!
for _ in $(seq 1 30); do
  curl -fsS "http://127.0.0.1:18080/health" >/dev/null 2>&1 && break
  sleep 1
done
./mvnw -q exec:java -Dexec.mainClass=com.javastream.examples.ConsumerExample -Dexec.args=phase13-smoke

echo "== monitoring =="
curl -fsS "http://127.0.0.1:18080/ready" | head -c 200
echo
curl -fsS "http://127.0.0.1:18080/metrics" | head -5

echo "Phase 13 local smoke finished (cluster + Docker: run manually per README)."
