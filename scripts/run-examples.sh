#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

MAIN="${1:?usage: $0 <ExampleMainClass> [exec args...]}"
shift || true

./mvnw -q -DskipTests package
exec ./mvnw -q exec:java -Dexec.mainClass="$MAIN" -Dexec.args="$*"
