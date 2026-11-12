package com.javastream.benchmark;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

/** Renders measured benchmark output as Markdown. */
public final class BenchmarkReportWriter {

    private BenchmarkReportWriter() {}

    public static String renderHardwareAndJvm() {
        Runtime runtime = Runtime.getRuntime();
        return """
                ## Environment

                | Item | Value |
                |------|-------|
                | OS | %s |
                | Available processors | %d |
                | JVM | %s |
                | JVM args | %s |
                """
                .formatted(
                        System.getProperty("os.name") + " " + System.getProperty("os.version"),
                        runtime.availableProcessors(),
                        System.getProperty("java.version") + " (" + System.getProperty("java.vendor") + ")",
                        ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                                .collect(Collectors.joining(" ")));
    }

    public static String renderResult(BenchmarkRunResult result) {
        LoadScenario s = result.scenario();
        return """
                ### %s

                | Metric | Value |
                |--------|-------|
                | Producers | %d |
                | Messages / producer | %d |
                | Message size (bytes) | %d |
                | Produce batch | %d |
                | Fetch batch | %d |
                | Durable store | %s |
                | Flush policy | %s |
                | Test window | %s → %s |
                | Messages produced | %,d |
                | Messages consumed | %,d |
                | Throughput (msg/s) | %.2f |
                | Throughput (MB/s) | %.4f |
                | Produce P50 (ms) | %.3f |
                | Produce P95 (ms) | %.3f |
                | Produce P99 (ms) | %.3f |
                | Fetch-batch P50 (ms) | %.3f |
                | Concurrent PING connections | %d |
                | Heap used (MB, start → end) | %.1f → %.1f |
                | GC collections (Δ) | %d |
                | GC time (ms, Δ) | %d |
                | Process CPU load (end sample) | %s |

                """
                .formatted(
                        s.name(),
                        s.producerCount(),
                        s.messagesPerProducer(),
                        s.messageSizeBytes(),
                        s.produceBatchSize(),
                        s.fetchBatchSize(),
                        s.durableStore(),
                        s.flushPolicy(),
                        DateTimeFormatter.ISO_INSTANT.format(result.startedAt()),
                        DateTimeFormatter.ISO_INSTANT.format(result.endedAt()),
                        result.messagesProduced(),
                        result.messagesConsumed(),
                        result.messagesPerSecond(),
                        result.throughputMbPerSecond(),
                        result.produceLatency().p50Ms(),
                        result.produceLatency().p95Ms(),
                        result.produceLatency().p99Ms(),
                        result.roundTripLatency().p50Ms(),
                        result.successfulConnections(),
                        result.startResources().heapUsedMb(),
                        result.endResources().heapUsedMb(),
                        result.endResources().gcCollectionCount() - result.startResources().gcCollectionCount(),
                        result.endResources().gcCollectionTimeMillis() - result.startResources().gcCollectionTimeMillis(),
                        result.endResources().processCpuLoad() < 0
                                ? "n/a"
                                : String.format("%.2f", result.endResources().processCpuLoad()));
    }

    public static void writeReport(Path target, List<BenchmarkRunResult> results) throws IOException {
        writeReport(target, results, false);
    }

    public static void appendResults(Path target, List<BenchmarkRunResult> results) throws IOException {
        writeReport(target, results, true);
    }

    private static void writeReport(Path target, List<BenchmarkRunResult> results, boolean append) throws IOException {
        if (append && Files.exists(target)) {
            String existing = Files.readString(target);
            StringBuilder sections = new StringBuilder();
            for (BenchmarkRunResult result : results) {
                sections.append(renderResult(result));
            }
            String marker = "## Apache Kafka comparison";
            int insert = existing.indexOf(marker);
            if (insert < 0) {
                insert = existing.length();
            }
            String merged = existing.substring(0, insert) + sections + (insert < existing.length() ? existing.substring(insert) : "");
            Files.writeString(target, merged);
            return;
        }
        StringBuilder body = new StringBuilder();
        body.append("# JavaStream benchmark report\n\n");
        body.append("All numbers below were captured on this machine; do not treat them as universal limits.\n\n");
        body.append(renderHardwareAndJvm()).append('\n');
        body.append("## Results\n\n");
        for (BenchmarkRunResult result : results) {
            body.append(renderResult(result));
        }
        body.append("""
                ## Apache Kafka comparison

                Run the same payload size and producer count against a local Kafka broker with matching durability
                (`acks=1` vs JavaStream `ACK_LEADER`, `acks=all` vs `ACK_ALL_REPLICAS`). Record Kafka numbers in a
                sibling section manually — this report intentionally omits invented cross-system figures.

                ## Disk throughput

                OS-level disk counters are not collected automatically. For durable runs (`storage.flush.policy=SYNC`),
                use `iostat` or `fs_usage` on macOS during the scenario window.

                """);
        Files.writeString(target, body.toString(), StandardCharsets.UTF_8);
    }
}
