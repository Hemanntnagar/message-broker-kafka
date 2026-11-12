package com.javastream.benchmark;

import java.time.Instant;

/** Aggregated measurements from one load scenario. */
public record BenchmarkRunResult(
        LoadScenario scenario,
        Instant startedAt,
        Instant endedAt,
        long messagesProduced,
        long messagesConsumed,
        double messagesPerSecond,
        double throughputMbPerSecond,
        LatencyPercentiles produceLatency,
        LatencyPercentiles roundTripLatency,
        ResourceSampler startResources,
        ResourceSampler endResources,
        int successfulConnections,
        String notes) {}
