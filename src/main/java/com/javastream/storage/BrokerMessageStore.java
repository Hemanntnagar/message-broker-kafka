package com.javastream.storage;

import com.javastream.partition.PartitionId;

import java.util.List;
import java.util.Map;

/**
 * Broker-facing append log API (in-memory or durable file-backed).
 */
public interface BrokerMessageStore extends MessageStore {

    AppendResult appendRecord(
            PartitionId partition,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers,
            long timestampEpochMillis);

    List<StoredRecord> fetch(PartitionId partition, long offset, int maxRecords);

    void ensurePartition(PartitionId partition);

    void removePartition(PartitionId partition);

    /** Persists buffered writes according to the configured flush policy. */
    void flush();

    void close();

    record AppendResult(long messageId, long offset) {}

    record StoredRecord(
            long messageId,
            long offset,
            long timestampEpochMillis,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers) {}
}
