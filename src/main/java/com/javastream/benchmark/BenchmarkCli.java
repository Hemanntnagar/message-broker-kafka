package com.javastream.benchmark;

import com.javastream.storage.log.FlushPolicy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * End-to-end load-test driver. Example:
 *
 * <pre>{@code
 * java -cp target/javastream-0.1.0-SNAPSHOT.jar com.javastream.benchmark.BenchmarkCli \
 *   --report BENCHMARK_REPORT.md
 * }</pre>
 */
public final class BenchmarkCli {

    public static void main(String[] args) throws Exception {
        System.setProperty("logback.configurationFile", "logback-benchmark.xml");
        Path report = Path.of("BENCHMARK_REPORT.md");
        boolean quick = false;
        String onlyScenario = null;
        boolean append = false;
        for (int i = 0; i < args.length; i++) {
            if ("--report".equals(args[i]) && i + 1 < args.length) {
                report = Path.of(args[++i]);
            } else if ("--quick".equals(args[i])) {
                quick = true;
            } else if ("--only".equals(args[i]) && i + 1 < args.length) {
                onlyScenario = args[++i];
            } else if ("--append".equals(args[i])) {
                append = true;
            }
        }

        List<LoadScenario> scenarios = quick ? quickScenarios() : standardScenarios();
        final String selectedScenario = onlyScenario;
        if (selectedScenario != null) {
            scenarios =
                    scenarios.stream()
                            .filter(s -> s.name().equals(selectedScenario))
                            .toList();
            if (scenarios.isEmpty()) {
                System.err.println("Unknown scenario: " + selectedScenario);
                System.exit(2);
            }
        }
        LoadGenerator generator = new LoadGenerator();
        List<BenchmarkRunResult> results = new ArrayList<>();

        for (LoadScenario scenario : scenarios) {
            System.out.println("Running scenario: " + scenario.name());
            Thread.sleep(onlyScenario != null ? 0 : 30_000);
            try (EmbeddedBenchmarkBroker broker =
                    EmbeddedBenchmarkBroker.start(scenario.durableStore(), scenario.flushPolicy())) {
                results.add(generator.run(broker, scenario));
            } catch (Exception e) {
                System.err.println("Scenario failed: " + scenario.name() + " — " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }

        if (results.isEmpty()) {
            System.err.println("No successful benchmark scenarios.");
            System.exit(1);
        }
        if (append) {
            BenchmarkReportWriter.appendResults(report, results);
        } else {
            BenchmarkReportWriter.writeReport(report, results);
        }
        System.out.println("Wrote " + report.toAbsolutePath());
    }

    private static List<LoadScenario> quickScenarios() {
        return List.of(
                new LoadScenario(
                        "quick-1-producer-inmemory",
                        1,
                        2_000,
                        256,
                        1,
                        100,
                        1,
                        0,
                        false,
                        FlushPolicy.NONE,
                        50));
    }

    private static List<LoadScenario> standardScenarios() {
        return List.of(
                new LoadScenario(
                        "baseline-1-producer-inmemory-256B",
                        1,
                        5_000,
                        256,
                        1,
                        100,
                        2,
                        0,
                        false,
                        FlushPolicy.NONE,
                        0),
                new LoadScenario(
                        "producers-10-inmemory-256B-batch10",
                        10,
                        2_000,
                        256,
                        10,
                        100,
                        2,
                        0,
                        false,
                        FlushPolicy.NONE,
                        0),
                new LoadScenario(
                        "producers-100-inmemory-256B",
                        100,
                        200,
                        256,
                        1,
                        50,
                        2,
                        0,
                        false,
                        FlushPolicy.NONE,
                        0),
                new LoadScenario(
                        "message-4KiB-1-producer-inmemory",
                        1,
                        5_000,
                        4_096,
                        1,
                        50,
                        2,
                        0,
                        false,
                        FlushPolicy.NONE,
                        0),
                new LoadScenario(
                        "durable-sync-1-producer-256B",
                        1,
                        2_000,
                        256,
                        1,
                        50,
                        2,
                        0,
                        true,
                        FlushPolicy.SYNC,
                        0),
                new LoadScenario(
                        "durable-none-1-producer-256B-batch20",
                        1,
                        5_000,
                        256,
                        20,
                        100,
                        2,
                        0,
                        true,
                        FlushPolicy.NONE,
                        0),
                new LoadScenario(
                        "concurrent-connections-ping",
                        0,
                        0,
                        0,
                        1,
                        1,
                        0,
                        0,
                        false,
                        FlushPolicy.NONE,
                        256,
                        true));
    }
}
