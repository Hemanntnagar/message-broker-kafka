# Performance engineering (Phase 10)

JavaStream separates **microbenchmarks** (JMH, isolated components) from **end-to-end load tests** (TCP producers/consumers against a live broker).

## Build

```bash
./mvnw clean verify
```

## JMH (component benchmarks)

Microbenchmarks live under `src/jmh/java` and cover:

- `RecordPayloadCodec` encode/decode (single and batch produce payloads)
- `SegmentIndex.lookupPosition` (binary search on sparse index)
- `LogSegment.append` (durable segment writes without per-record `force`)

```bash
./mvnw -Pjmh -q package
java -jar target/javastream-jmh.jar -rf text '.*'
```

## End-to-end load generator

`com.javastream.benchmark.BenchmarkCli` starts an embedded broker, runs a scenario matrix (producer count, batch size, message size, flush policy), and writes `BENCHMARK_REPORT.md` with **measured** throughput and latency percentiles.

Quick smoke (short run):

```bash
./mvnw -q -DskipTests package exec:java \
  -Dexec.mainClass=com.javastream.benchmark.BenchmarkCli \
  -Dexec.args="--quick --report BENCHMARK_REPORT.md"
```

Full matrix (one JVM per scenario to avoid local port exhaustion):

```bash
./scripts/run-benchmark-matrix.sh
```

Scenarios include 1 / 10 / 100 producers, 256 B and 4 KiB payloads, in-memory and on-disk stores with `NONE` and `SYNC` flush policies, plus a concurrent `PING` connection probe (up to 500 on the standard matrix).

## Optimizations in this phase

| Area | Change |
|------|--------|
| Batch publishing | Optional `PRODUCE` batch trailer; `ProducerClient.publishBatch` |
| Batch consumption | Existing `FETCH` `maxRecords`; load tests tune fetch batch size |
| ByteBuffer reuse | `SegmentIndex` index write buffer; `LogSegment` header scratch buffer |
| Segment indexing | Binary search in `SegmentIndex.lookupPosition` |
| Object allocation | Reused buffers on hot append/index paths |
| Virtual threads | Broker acceptor unchanged (one virtual thread per client) |

## Comparing with Apache Kafka

Match durability (`acks`) to JavaStream `ProducerAckLevel` and run the same producer count and payload size. Record Kafka results manually in the benchmark report — the load generator does not fabricate cross-system numbers.

## Metrics collected automatically

Messages/s, MB/s, produce P50/P95/P99, fetch-batch latency sample, heap usage, GC count/time delta, process CPU sample, concurrent connection count.

Disk throughput and full OS CPU utilization require external tools (`iostat`, Activity Monitor, etc.) during durable runs.
