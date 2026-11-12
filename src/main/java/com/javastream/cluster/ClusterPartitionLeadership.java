package com.javastream.cluster;

import com.javastream.partition.PartitionId;
import com.javastream.partition.PartitionLeader;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Partition leadership view driven by {@link ClusterManager} assignments. */
public final class ClusterPartitionLeadership implements PartitionLeader {

    private final AtomicReference<ClusterManager> clusterManager = new AtomicReference<>();

    public void attach(ClusterManager manager) {
        clusterManager.set(Objects.requireNonNull(manager));
    }

    @Override
    public BrokerId leaderFor(PartitionId partition) {
        ClusterManager manager = clusterManager.get();
        if (manager == null) {
            throw new IllegalStateException("ClusterManager not attached");
        }
        return manager.assignment(partition)
                .map(a -> new BrokerId(a.leaderId()))
                .orElseThrow(() -> new IllegalStateException("No assignment for partition " + partition));
    }

    @Override
    public void assignLeader(PartitionId partition, BrokerId brokerId) {
        throw new UnsupportedOperationException("Leaders are assigned by ClusterManager only");
    }

    public boolean isLocalLeader(PartitionId partition, int localBrokerId) {
        ClusterManager manager = clusterManager.get();
        if (manager == null) {
            return true;
        }
        return manager.assignment(partition).map(a -> a.leaderId() == localBrokerId).orElse(true);
    }
}
