package com.javastream.replication;

import com.javastream.partition.PartitionId;

import java.util.Set;

/**
 * Placeholder ISR view for single-broker deployments until inter-broker replication is wired.
 */
public final class SingleBrokerReplicaManager implements ReplicaManager {

    private final int brokerId;

    public SingleBrokerReplicaManager(int brokerId) {
        this.brokerId = brokerId;
    }

    @Override
    public Set<Integer> inSyncReplicaIds(PartitionId partition) {
        return Set.of(brokerId);
    }

    @Override
    public void markReplicaInSync(PartitionId partition, int replicaBrokerId) {
        // no-op until multi-broker replication is implemented
    }
}
