package com.javastream.cluster;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Thread-safe {@link ClusterMembership} backed by an in-memory set. */
public final class InMemoryClusterMembership implements ClusterMembership {

    private final ConcurrentHashMap<BrokerId, Boolean> members = new ConcurrentHashMap<>();

    @Override
    public void register(BrokerId brokerId) {
        members.put(brokerId, Boolean.TRUE);
    }

    @Override
    public void deregister(BrokerId brokerId) {
        members.remove(brokerId);
    }

    @Override
    public Set<BrokerId> members() {
        return Set.copyOf(members.keySet());
    }
}
