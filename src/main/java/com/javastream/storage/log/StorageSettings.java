package com.javastream.storage.log;

import com.javastream.config.JavaStreamConfig;

import java.nio.file.Path;
import java.util.Properties;

/** Immutable durable log configuration. */
public record StorageSettings(
        Path dataDirectory,
        int brokerId,
        long segmentMaxBytes,
        FlushPolicy flushPolicy,
        long flushIntervalMillis,
        int indexStrideRecords) {

    public static final long DEFAULT_SEGMENT_MAX_BYTES = 128L * 1024 * 1024;
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 1_000L;
    public static final int DEFAULT_INDEX_STRIDE = 4_096;

    public StorageSettings {
        if (segmentMaxBytes <= 0) {
            throw new IllegalArgumentException("segmentMaxBytes must be positive");
        }
        if (flushIntervalMillis <= 0) {
            throw new IllegalArgumentException("flushIntervalMillis must be positive");
        }
        if (indexStrideRecords <= 0) {
            throw new IllegalArgumentException("indexStrideRecords must be positive");
        }
    }

    public Path brokerRoot() {
        return dataDirectory.resolve("broker");
    }

    public static StorageSettings from(JavaStreamConfig config, Properties props) {
        long segmentMax = parseLong(
                props.getProperty("storage.segment.max.bytes", Long.toString(DEFAULT_SEGMENT_MAX_BYTES)),
                "storage.segment.max.bytes");
        FlushPolicy flushPolicy =
                FlushPolicy.valueOf(
                        props.getProperty("storage.flush.policy", "PERIODIC").trim().toUpperCase());
        long flushInterval = parseLong(
                props.getProperty("storage.flush.interval.ms", Long.toString(DEFAULT_FLUSH_INTERVAL_MS)),
                "storage.flush.interval.ms");
        int indexStride = parseInt(
                props.getProperty("storage.index.stride.records", Integer.toString(DEFAULT_INDEX_STRIDE)),
                "storage.index.stride.records");
        return new StorageSettings(
                Path.of(config.dataDirectory()),
                config.brokerId(),
                segmentMax,
                flushPolicy,
                flushInterval,
                indexStride);
    }

    private static long parseLong(String value, String key) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long for " + key + ": " + value, e);
        }
    }

    private static int parseInt(String value, String key) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid int for " + key + ": " + value, e);
        }
    }
}
