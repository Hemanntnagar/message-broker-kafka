package com.javastream.benchmark.jmh;

import com.javastream.storage.log.LogSegment;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class LogSegmentAppendBenchmark {

    @State(Scope.Benchmark)
    public static class SegmentState {
        Path dir;
        LogSegment segment;
        AtomicLong nextOffset = new AtomicLong();
        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[256];

        @Setup
        public void setup() throws Exception {
            dir = Files.createTempDirectory("jmh-log");
            segment =
                    new LogSegment(
                            0,
                            dir.resolve("segment-0.log"),
                            dir.resolve("segment-0.idx"),
                            4096,
                            128L * 1024 * 1024,
                            true);
        }

        @TearDown
        public void tearDown() throws Exception {
            segment.close();
            Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                    // ignore
                }
            });
        }
    }

    @Benchmark
    public LogSegment.AppendResult appendNoSync(SegmentState state) throws Exception {
        long offset = state.nextOffset.getAndIncrement();
        return state.segment.append(
                offset,
                offset + 1,
                System.currentTimeMillis(),
                state.key,
                state.value,
                Map.of(),
                false);
    }
}
