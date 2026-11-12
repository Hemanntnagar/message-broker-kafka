package com.javastream.partition;

import com.javastream.cluster.BrokerId;

/**
 * Leadership metadata for a single partition replica set.
 */
public interface PartitionLeader {

    BrokerId leaderFor(PartitionId partition);

    void assignLeader(PartitionId partition, BrokerId brokerId);
}
