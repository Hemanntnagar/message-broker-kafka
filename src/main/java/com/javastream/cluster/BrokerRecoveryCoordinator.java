package com.javastream.cluster;

import com.javastream.broker.BrokerServices;
import com.javastream.replication.ReplicationEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Startup and post-failover recovery hooks for cluster brokers. */
public final class BrokerRecoveryCoordinator {

    private static final Logger log = LoggerFactory.getLogger(BrokerRecoveryCoordinator.class);

    private BrokerRecoveryCoordinator() {}

    public static void onBrokerStarted(ReplicationEngine replicationEngine, BrokerServices brokerServices) {
        replicationEngine.syncAssignmentsFromCluster();
        replicationEngine.catchUpAllAssignedFollowers();
        brokerServices.consumerGroupManager();
        log.info(
                "Broker {} recovery: assignments synced, follower catch-up scheduled, consumer offsets loaded from catalog",
                brokerServices.brokerId());
    }

    public static void onGracefulShutdown(ClusterManager clusterManager, int brokerId, boolean coordinator) {
        if (coordinator) {
            return;
        }
        clusterManager.voluntaryLeave(brokerId);
    }
}
