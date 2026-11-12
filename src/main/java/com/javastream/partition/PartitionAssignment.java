package com.javastream.partition;

import com.javastream.topic.TopicName;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Chooses a partition for produce: explicit hint, key hash, or round-robin when key is absent.
 * Ordering is per-partition only, not across partitions.
 */
public final class PartitionAssignment {

    private final AtomicInteger roundRobinCounter = new AtomicInteger();

    public int resolve(TopicName topic, int partitionHint, byte[] key, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        if (partitionHint >= 0 && partitionHint < partitionCount) {
            return partitionHint;
        }
        if (key != null && key.length > 0) {
            return Math.floorMod(hashKey(key), partitionCount);
        }
        return Math.floorMod(roundRobinCounter.getAndIncrement(), partitionCount);
    }

  /** Same key always maps to the same partition for a fixed partition count. */
    public static int hashKey(byte[] key) {
        int hash = 0;
        for (byte b : key) {
            hash = 31 * hash + b;
        }
        return hash;
    }
}
