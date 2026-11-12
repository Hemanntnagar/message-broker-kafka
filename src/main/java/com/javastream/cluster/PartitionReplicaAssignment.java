package com.javastream.cluster;

import com.javastream.partition.PartitionId;

import java.util.List;

/** Leader, follower replica set, and monotonic leader epoch for one partition. */
public record PartitionReplicaAssignment(
        PartitionId partitionId, int leaderId, List<Integer> replicaIds, long leaderEpoch) {

    public PartitionReplicaAssignment(PartitionId partitionId, int leaderId, List<Integer> replicaIds) {
        this(partitionId, leaderId, replicaIds, 1L);
    }

    public PartitionReplicaAssignment {
        if (leaderId < 0) {
            throw new IllegalArgumentException("leaderId must be non-negative");
        }
        if (replicaIds == null || replicaIds.isEmpty()) {
            throw new IllegalArgumentException("replicaIds must not be empty");
        }
        if (!replicaIds.contains(leaderId)) {
            throw new IllegalArgumentException("leader must be a member of replicaIds");
        }
        if (leaderEpoch < 1) {
            throw new IllegalArgumentException("leaderEpoch must be >= 1");
        }
        replicaIds = List.copyOf(replicaIds);
    }
}
