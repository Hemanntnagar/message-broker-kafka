#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./mvnw -q -DskipTests package
REPORT="BENCHMARK_REPORT.md"
rm -f "${REPORT}"
SCENARIOS=(
  baseline-1-producer-inmemory-256B
  producers-10-inmemory-256B-batch10
  producers-100-inmemory-256B
  message-4KiB-1-producer-inmemory
  durable-sync-1-producer-256B
  durable-none-1-producer-256B-batch20
  concurrent-connections-ping
)
for i in "${!SCENARIOS[@]}"; do
  name="${SCENARIOS[$i]}"
  append_flag=""
  if [[ $i -gt 0 ]]; then
    append_flag="--append"
    sleep 3
  fi
  ./mvnw -q exec:java \
    -Dexec.mainClass=com.javastream.benchmark.BenchmarkCli \
    -Dexec.args="--only ${name} --report ${REPORT} ${append_flag}"
done
echo "Wrote ${REPORT}"
