package com.javastream.cluster;

import com.javastream.partition.PartitionId;

import java.util.List;

/** Leader and follower replica set for one partition. */
public record PartitionReplicaAssignment(PartitionId partitionId, int leaderId, List<Integer> replicaIds) {

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
        replicaIds = List.copyOf(replicaIds);
    }
}
