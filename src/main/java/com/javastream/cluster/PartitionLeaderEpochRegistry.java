package com.javastream.cluster;

import com.javastream.partition.PartitionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local leader epoch (fencing token) per partition. Incremented whenever the coordinator assigns a
 * new leader so a stale leader cannot accept writes after failover.
 */
public final class PartitionLeaderEpochRegistry {

    private final ConcurrentHashMap<PartitionId, AtomicLong> epochs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionId, Integer> leaders = new ConcurrentHashMap<>();

    public long epoch(PartitionId partition) {
        AtomicLong value = epochs.get(partition);
        return value == null ? 0L : value.get();
    }

    public void apply(PartitionReplicaAssignment assignment) {
        PartitionId partition = assignment.partitionId();
        AtomicLong epochRef = epochs.computeIfAbsent(partition, ignored -> new AtomicLong(0L));
        leaders.put(partition, assignment.leaderId());
        epochRef.updateAndGet(current -> Math.max(current, assignment.leaderEpoch()));
    }

    public boolean isCurrentLeader(PartitionId partition, int localBrokerId, int assignedLeaderId) {
        if (assignedLeaderId != localBrokerId) {
            return false;
        }
        return leaders.getOrDefault(partition, -1) == localBrokerId;
    }

    public Map<PartitionId, Long> snapshot() {
        ConcurrentHashMap<PartitionId, Long> copy = new ConcurrentHashMap<>();
        for (Map.Entry<PartitionId, AtomicLong> entry : epochs.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().get());
        }
        return Map.copyOf(copy);
    }
}
