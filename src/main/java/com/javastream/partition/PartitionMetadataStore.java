package com.javastream.partition;

import com.javastream.topic.TopicName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds per-partition offset statistics separately from {@link com.javastream.storage.MessageStore}.
 */
public final class PartitionMetadataStore {

    private final ConcurrentHashMap<PartitionId, PartitionStats> stats = new ConcurrentHashMap<>();

    public void register(PartitionId partitionId) {
        stats.computeIfAbsent(partitionId, ignored -> new PartitionStats(partitionId.index()));
    }

    public void unregister(PartitionId partitionId) {
        stats.remove(partitionId);
    }

    public void unregisterAll(Iterable<PartitionId> partitionIds) {
        for (PartitionId id : partitionIds) {
            stats.remove(id);
        }
    }

    /** Restores in-memory stats after durable log recovery (offsets start at 0). */
    public void recover(PartitionId partitionId, long logEndOffset) {
        if (logEndOffset < 0) {
            throw new IllegalArgumentException("logEndOffset must be non-negative");
        }
        PartitionStats partitionStats = new PartitionStats(partitionId.index());
        partitionStats.logEndOffset.set(logEndOffset);
        partitionStats.recordCount.set(logEndOffset);
        stats.put(partitionId, partitionStats);
    }

    public long onAppend(PartitionId partitionId, long assignedOffset) {
        PartitionStats partitionStats =
                stats.computeIfAbsent(partitionId, ignored -> new PartitionStats(partitionId.index()));
        return partitionStats.recordAppend(assignedOffset);
    }

    public Optional<PartitionMetadata> snapshot(PartitionId partitionId) {
        PartitionStats partitionStats = stats.get(partitionId);
        if (partitionStats == null) {
            return Optional.empty();
        }
        return Optional.of(partitionStats.snapshot());
    }

    public List<PartitionMetadata> snapshotsForTopic(TopicName topicName, int partitionCount) {
        List<PartitionMetadata> list = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            PartitionId id = new PartitionId(topicName, i);
            PartitionStats partitionStats = stats.get(id);
            if (partitionStats == null) {
                list.add(new PartitionMetadata(i, 0, 0));
            } else {
                list.add(partitionStats.snapshot());
            }
        }
        return List.copyOf(list);
    }

    private static final class PartitionStats {
        private final int partitionIndex;
        private final AtomicLong logEndOffset = new AtomicLong(0);
        private final AtomicLong recordCount = new AtomicLong(0);

        PartitionStats(int partitionIndex) {
            this.partitionIndex = partitionIndex;
        }

        synchronized long recordAppend(long assignedOffset) {
            long next = logEndOffset.get();
            if (assignedOffset != next) {
                throw new IllegalStateException(
                        "Offset sequence corruption: expected %d, got %d on partition %d"
                                .formatted(next, assignedOffset, partitionIndex));
            }
            logEndOffset.incrementAndGet();
            recordCount.incrementAndGet();
            return assignedOffset;
        }

        PartitionMetadata snapshot() {
            long end = logEndOffset.get();
            return new PartitionMetadata(partitionIndex, end, recordCount.get());
        }
    }
}
