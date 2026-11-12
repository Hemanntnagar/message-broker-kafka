package com.javastream.partition;

/**
 * Snapshot of partition log stats (no message bytes). {@code logEndOffset} is the offset assigned to the
 * next append (equivalent to record count).
 */
public record PartitionMetadata(int partitionIndex, long logEndOffset, long recordCount) {

    public PartitionMetadata {
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must be non-negative");
        }
        if (logEndOffset < 0) {
            throw new IllegalArgumentException("logEndOffset must be non-negative");
        }
        if (recordCount < 0) {
            throw new IllegalArgumentException("recordCount must be non-negative");
        }
    }
}
