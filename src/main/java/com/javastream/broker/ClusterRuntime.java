package com.javastream.broker;

import com.javastream.cluster.BrokerEndpoint;
import com.javastream.cluster.ClusterManager;
import com.javastream.cluster.ClusterMemberSession;
import com.javastream.cluster.ClusterPartitionLeadership;
import com.javastream.cluster.InMemoryClusterMembership;
import com.javastream.config.JavaStreamConfig;
import com.javastream.replication.ClusterReplicaManager;
import com.javastream.replication.ReplicationEngine;
import com.javastream.replication.interbroker.InterBrokerRequestHandler;
import com.javastream.replication.interbroker.InterBrokerServer;
import com.javastream.storage.InMemoryMessageStore;

/** Wires cluster, replication, and broker services for {@link ClusterBrokerNode}. */
public final class ClusterRuntime implements AutoCloseable {

    private final JavaStreamConfig config;
    private final ClusterManager clusterManager;
    private final ClusterReplicaManager replicaManager;
    private final ClusterPartitionLeadership leadership;
    private final BrokerServices brokerServices;
    private final ReplicationEngine replicationEngine;
    private final InterBrokerServer interBrokerServer;
    private volatile ClusterMemberSession memberSession;

    private ClusterRuntime(
            JavaStreamConfig config,
            ClusterManager clusterManager,
            ClusterReplicaManager replicaManager,
            ClusterPartitionLeadership leadership,
            BrokerServices brokerServices,
            ReplicationEngine replicationEngine,
            InterBrokerServer interBrokerServer,
            ClusterMemberSession memberSession) {
        this.config = config;
        this.clusterManager = clusterManager;
        this.replicaManager = replicaManager;
        this.leadership = leadership;
        this.brokerServices = brokerServices;
        this.replicationEngine = replicationEngine;
        this.interBrokerServer = interBrokerServer;
        this.memberSession = memberSession;
    }

    public static ClusterRuntime create(JavaStreamConfig config) {
        boolean coordinator = config.brokerId() == config.coordinatorBrokerId();
        ClusterManager clusterManager =
                new ClusterManager(
                        config.coordinatorBrokerId(),
                        config.replicationFactor(),
                        config.clusterSessionTimeoutMs(),
                        new InMemoryClusterMembership(),
                        coordinator);
        ClusterReplicaManager replicaManager = new ClusterReplicaManager(config.brokerId());
        ClusterPartitionLeadership leadership = new ClusterPartitionLeadership();
        leadership.attach(clusterManager);

        BrokerServices brokerServices =
                new BrokerServices(
                        new InMemoryMessageStore(),
                        config.defaultTopicPartitionCount(),
                        config.brokerId(),
                        replicaManager,
                        leadership,
                        null);
        ReplicationEngine replicationEngine =
                new ReplicationEngine(config.brokerId(), clusterManager, replicaManager, brokerServices);
        brokerServices.attachReplicationEngine(replicationEngine);

        InterBrokerRequestHandler handler =
                new InterBrokerRequestHandler(
                        config.brokerId(), clusterManager, replicationEngine, brokerServices);
        InterBrokerServer interBrokerServer =
                new InterBrokerServer(config.listenHost(), config.replicationListenPort(), handler);

        return new ClusterRuntime(
                config,
                clusterManager,
                replicaManager,
                leadership,
                brokerServices,
                replicationEngine,
                interBrokerServer,
                null);
    }

    public void startNetworking(int boundClientPort) {
        interBrokerServer.start();
        int replicationPort =
                config.replicationListenPort() == 0 ? interBrokerServer.boundPort() : config.replicationListenPort();
        BrokerEndpoint self =
                new BrokerEndpoint(
                        config.brokerId(), effectiveHost(config), boundClientPort, replicationPort);
        ClusterMemberSession session =
                new ClusterMemberSession(
                        self,
                        config.coordinatorHost(),
                        resolveCoordinatorPort(),
                        config.clusterHeartbeatIntervalMs(),
                        clusterManager,
                        replicationEngine);
        session.start();
        memberSession = session;
    }

    private int resolveCoordinatorPort() {
        if (config.brokerId() == config.coordinatorBrokerId()) {
            return interBrokerServer.boundPort();
        }
        return config.coordinatorReplicationPort();
    }

    private static String effectiveHost(JavaStreamConfig config) {
        if ("0.0.0.0".equals(config.listenHost()) || "::".equals(config.listenHost())) {
            return "127.0.0.1";
        }
        return config.listenHost();
    }

    public BrokerServices brokerServices() {
        return brokerServices;
    }

    public ClusterPartitionLeadership leadership() {
        return leadership;
    }

    public ReplicationEngine replicationEngine() {
        return replicationEngine;
    }

    public ClusterManager clusterManager() {
        return clusterManager;
    }

    public InterBrokerServer interBrokerServer() {
        return interBrokerServer;
    }

    public void gracefulClusterShutdown() {
        ClusterMemberSession session = memberSession;
        if (session != null) {
            session.leaveCluster();
        }
    }

    @Override
    public void close() {
        ClusterMemberSession session = memberSession;
        if (session != null) {
            session.close();
        }
        interBrokerServer.stop();
    }
}
