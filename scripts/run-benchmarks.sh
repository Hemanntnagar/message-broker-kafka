#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./mvnw -q clean verify
"$(dirname "$0")/run-benchmark-matrix.sh"
./mvnw -Pjmh -q package -DskipTests
java -jar target/javastream-jmh.jar -rf text -rff target/jmh-results.txt '.*' || true
echo "See BENCHMARK_REPORT.md and target/jmh-results.txt"
