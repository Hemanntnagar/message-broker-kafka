# Benchmarks

JavaStream separates **JMH microbenchmarks** from **end-to-end TCP load tests**.

## Quick commands

```bash
./mvnw clean verify
./mvnw -Pjmh -q package
java -jar target/javastream-jmh.jar -rf text '.*'
```

End-to-end matrix (writes [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md)):

```bash
./scripts/run-benchmark-matrix.sh
```

Quick smoke:

```bash
./mvnw -q -DskipTests package exec:java \
  -Dexec.mainClass=com.javastream.benchmark.BenchmarkCli \
  -Dexec.args="--quick --report BENCHMARK_REPORT.md"
```

## What is measured

| Tool | Scope |
|------|--------|
| JMH | `RecordPayloadCodec`, `SegmentIndex`, `LogSegment.append` |
| `LoadGenerator` | Producer/consumer throughput, latency percentiles, connection probe |
| `ResourceSampler` | Heap and GC deltas during scenarios |

## Latest results

See **[BENCHMARK_REPORT.md](BENCHMARK_REPORT.md)** for numbers captured on the reference machine. Re-run benchmarks after hardware or JVM changes; do not treat stale reports as current SLOs.

## Interpretation notes

- Scenarios may use in-memory or durable storage; check the report table column **Durable store**.
- `ACK_LEADER` is the default in load tests; cluster `ACK_ALL_REPLICAS` is not the default matrix row.
- OS-level disk I/O is not collected automatically for durable runs — use `iostat` / `fs_usage` locally.

More detail: [PERFORMANCE.md](PERFORMANCE.md).
