package com.javastream.benchmark;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Computes percentile latency from collected nanosecond samples. */
public final class LatencyPercentiles {

    private final long[] sortedNanos;

    public LatencyPercentiles(long[] samplesNanos) {
        long[] copy = Arrays.copyOf(samplesNanos, samplesNanos.length);
        Arrays.sort(copy);
        this.sortedNanos = copy;
    }

    public int sampleCount() {
        return sortedNanos.length;
    }

    public double percentileMs(double p) {
        if (sortedNanos.length == 0) {
            return 0.0;
        }
        int index = (int) Math.ceil(p / 100.0 * sortedNanos.length) - 1;
        index = Math.max(0, Math.min(sortedNanos.length - 1, index));
        return sortedNanos[index] / (double) TimeUnit.MILLISECONDS.toNanos(1);
    }

    public double p50Ms() {
        return percentileMs(50);
    }

    public double p95Ms() {
        return percentileMs(95);
    }

    public double p99Ms() {
        return percentileMs(99);
    }
}
