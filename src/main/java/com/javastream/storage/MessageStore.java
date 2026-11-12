package com.javastream.storage;

import com.javastream.partition.PartitionId;

/**
 * Append-only durable log per partition (implementation in storage phase).
 */
public interface MessageStore {

    long append(PartitionId partition, byte[] key, byte[] value);

    StorageRecord read(PartitionId partition, long offset);

    record StorageRecord(long offset, byte[] key, byte[] value) {
    }
}
