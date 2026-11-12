package com.javastream.cluster;

import java.util.Set;

/**
 * View of live brokers in the cluster (coordination layer added later).
 */
public interface ClusterMembership {

    void register(BrokerId brokerId);

    void deregister(BrokerId brokerId);

    Set<BrokerId> members();
}
