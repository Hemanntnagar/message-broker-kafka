package com.javastream.benchmark;

import com.javastream.storage.log.FlushPolicy;

/** Parameters for an end-to-end broker load test. */
public record LoadScenario(
        String name,
        int producerCount,
        int messagesPerProducer,
        int messageSizeBytes,
        int produceBatchSize,
        int fetchBatchSize,
        long warmupSeconds,
        long durationSeconds,
        boolean durableStore,
        FlushPolicy flushPolicy,
        int connectionProbeCount,
        boolean holdConnectionsDuringProbe) {

    public LoadScenario(
            String name,
            int producerCount,
            int messagesPerProducer,
            int messageSizeBytes,
            int produceBatchSize,
            int fetchBatchSize,
            long warmupSeconds,
            long durationSeconds,
            boolean durableStore,
            FlushPolicy flushPolicy,
            int connectionProbeCount) {
        this(
                name,
                producerCount,
                messagesPerProducer,
                messageSizeBytes,
                produceBatchSize,
                fetchBatchSize,
                warmupSeconds,
                durationSeconds,
                durableStore,
                flushPolicy,
                connectionProbeCount,
                false);
    }

    public long targetMessageCount() {
        return (long) producerCount * messagesPerProducer;
    }

    public double payloadMbTotal() {
        return targetMessageCount() * messageSizeBytes / (1024.0 * 1024.0);
    }
}
