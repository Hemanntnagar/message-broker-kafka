package com.javastream.cluster;

import com.javastream.cluster.InMemoryClusterMembership;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerFailureDetectionTest {

    @Test
    void heartbeatGapMarksSuspectedBeforeEviction() throws Exception {
        ClusterManager manager =
                new ClusterManager(0, 1, 200, new InMemoryClusterMembership(), true);
        manager.register(new BrokerEndpoint(1, "127.0.0.1", 1, 9_999));
        Thread.sleep(250);
        manager.expireStaleBrokers();
        assertEquals(BrokerSessionState.SUSPECTED_UNAVAILABLE, manager.brokerState(1).orElseThrow());
        assertTrue(manager.broker(1).isPresent(), "suspected broker remains registered");
    }

    @Test
    void voluntaryLeaveRemovesBrokerImmediately() {
        ClusterManager manager =
                new ClusterManager(0, 1, 5_000, new InMemoryClusterMembership(), true);
        manager.register(new BrokerEndpoint(1, "127.0.0.1", 1, 9_999));
        manager.voluntaryLeave(1);
        assertTrue(manager.broker(1).isEmpty());
    }

    @Test
    void heartbeatRecoversSuspectedBroker() throws Exception {
        ClusterManager manager =
                new ClusterManager(0, 1, 100, new InMemoryClusterMembership(), true);
        manager.register(new BrokerEndpoint(1, "127.0.0.1", 1, 9_999));
        Thread.sleep(150);
        manager.expireStaleBrokers();
        assertEquals(BrokerSessionState.SUSPECTED_UNAVAILABLE, manager.brokerState(1).orElseThrow());
        manager.heartbeat(1);
        assertEquals(BrokerSessionState.AVAILABLE, manager.brokerState(1).orElseThrow());
    }
}
