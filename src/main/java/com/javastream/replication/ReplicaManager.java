package com.javastream.replication;

import com.javastream.partition.PartitionId;

import java.util.Set;

/**
 * Tracks in-sync replicas and replication state per partition.
 */
public interface ReplicaManager {

    Set<Integer> inSyncReplicaIds(PartitionId partition);

    void markReplicaInSync(PartitionId partition, int replicaBrokerId);
}
