package com.javastream.benchmark.jmh;

import com.javastream.storage.log.SegmentIndex;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(1)
public class SegmentIndexBenchmark {

    @State(Scope.Benchmark)
    public static class IndexState {
        Path indexFile;
        SegmentIndex index;

        @Setup
        public void setup() throws Exception {
            indexFile = Files.createTempFile("jmh-index", ".idx");
            index = new SegmentIndex(indexFile, 1, true);
            for (long offset = 0; offset < 50_000; offset++) {
                index.maybeAppend(offset, offset * 128, offset + 1);
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            index.close();
            Files.deleteIfExists(indexFile);
        }
    }

    @Benchmark
    public long lookup(IndexState state) {
        return state.index.lookupPosition(37_777);
    }
}
