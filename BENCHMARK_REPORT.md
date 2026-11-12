# JavaStream benchmark report

All numbers below were captured on this machine (2026-11-12); re-run `scripts/run-benchmark-matrix.sh` after `./mvnw verify` for a fresh full matrix.

## Environment

| Item | Value |
|------|-------|
| OS | Mac OS X 27.2 |
| Available processors | 8 |
| JVM | 26.0.1 (Oracle Corporation) |
| Broker config | In-memory or file store per scenario; default topic partitions = 1 |
| Test client | `com.javastream.benchmark.LoadGenerator` (TCP, `ACK_LEADER`) |

## Results

### baseline-1-producer-inmemory-256B

| Metric | Value |
|--------|-------|
| Producers | 1 |
| Messages / producer | 5,000 |
| Message size (bytes) | 256 |
| Produce batch | 1 |
| Fetch batch | 100 |
| Durable store | false |
| Flush policy | NONE |
| Test window | 2026-11-12T18:01:07Z → 2026-11-12T18:01:09Z (~2.2 s) |
| Messages produced | 5,000 |
| Messages consumed | 5,000 |
| Throughput (msg/s) | 25,827.18 |
| Throughput (MB/s) | 6.3055 |
| Produce P50 (ms) | 0.037 |
| Produce P95 (ms) | 0.058 |
| Produce P99 (ms) | 0.089 |
| Fetch-batch P50 (ms) | 0.168 |
| Concurrent PING connections | n/a (throughput scenario) |
| Heap used (MB, start → end) | 53.0 → 188.7 |
| GC collections (Δ) | 2 |
| GC time (ms, Δ) | 40 |
| Process CPU load (end sample) | 0.25 |

### concurrent-connections-ping

| Metric | Value |
|--------|-------|
| Producers | 0 (probe only) |
| Concurrent PING connections held open | 256 |
| Process CPU load (end sample) | 0.32 |
| Notes | Sequential connect + PING; connections held until probe completes |

### quick-1-producer-inmemory (smoke)

| Metric | Value |
|--------|-------|
| Messages produced | 2,000 |
| Throughput (msg/s) | 3,317.50 |
| Produce P99 (ms) | 4.257 |

Additional scenarios (`producers-10`, `producers-100`, 4 KiB payloads, durable `SYNC` / `NONE` batch produce) are defined in `BenchmarkCli.standardScenarios()` and collected by `scripts/run-benchmark-matrix.sh` (one JVM per scenario to avoid local ephemeral port exhaustion).

## JMH component benchmarks

Build and run:

```bash
./mvnw -Pjmh -q package
java -jar target/javastream-jmh.jar -rf text '.*'
```

See `PERFORMANCE.md` for benchmark class list. Paste JMH output into this section after running locally.

## Apache Kafka comparison

Run the same payload size and producer count against a local Kafka broker with matching durability (`acks=1` vs JavaStream `ACK_LEADER`). Record results here manually — no cross-system numbers are invented in CI.

## Disk throughput

OS-level disk counters are not collected automatically. For durable runs (`storage.flush.policy=SYNC`), use `iostat` or `fs_usage` on macOS during the scenario window.
