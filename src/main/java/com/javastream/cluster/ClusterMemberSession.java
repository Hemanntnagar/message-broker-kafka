package com.javastream.cluster;

import com.javastream.replication.ReplicationEngine;
import com.javastream.replication.interbroker.InterBrokerClient;
import com.javastream.replication.interbroker.InterBrokerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.javastream.cluster.BrokerRecoveryCoordinator;
import com.javastream.partition.PartitionId;
import com.javastream.topic.TopicName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Registers this broker with the coordinator and sends periodic cluster heartbeats. */
public final class ClusterMemberSession implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterMemberSession.class);

    private final BrokerEndpoint self;
    private final String coordinatorHost;
    private final int coordinatorReplicationPort;
    private final long heartbeatIntervalMs;
    private final ClusterManager clusterManager;
    private final ReplicationEngine replicationEngine;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread heartbeatThread;

    public ClusterMemberSession(
            BrokerEndpoint self,
            String coordinatorHost,
            int coordinatorReplicationPort,
            long heartbeatIntervalMs,
            ClusterManager clusterManager,
            ReplicationEngine replicationEngine) {
        this.self = self;
        this.coordinatorHost = coordinatorHost;
        this.coordinatorReplicationPort = coordinatorReplicationPort;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.clusterManager = clusterManager;
        this.replicationEngine = replicationEngine;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        registerWithCoordinator();
        clusterManager.addAssignmentListener(replicationEngine::syncAssignmentsFromCluster);
        clusterManager.addAssignmentListener(replicationEngine::catchUpAllAssignedFollowers);
        BrokerRecoveryCoordinator.onBrokerStarted(replicationEngine, replicationEngine.brokerServices());
        heartbeatThread = Thread.ofVirtual().name("cluster-heartbeat-" + self.brokerId()).start(this::heartbeatLoop);
    }

    private void registerWithCoordinator() {
        try {
            if (self.brokerId() == clusterManager.coordinatorBrokerId()) {
                clusterManager.register(self);
                clusterManager.applyAssignments(clusterManager.buildAssignmentPush());
            } else {
                InterBrokerClient client =
                        new InterBrokerClient(coordinatorHost, coordinatorReplicationPort, 5_000, 10_000);
                InterBrokerCodec.AssignmentPush push =
                        client.register(
                                new InterBrokerCodec.RegisterRequest(
                                        self.brokerId(), self.host(), self.clientPort(), self.replicationPort()));
                clusterManager.applyAssignments(push);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to register with cluster coordinator", e);
        }
    }

    private void heartbeatLoop() {
        while (running.get()) {
            try {
                Thread.sleep(heartbeatIntervalMs);
                sendHeartbeat();
                if (clusterManager.isCoordinator()) {
                    clusterManager.expireStaleBrokers();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.debug("Cluster heartbeat failed for broker {}: {}", self.brokerId(), e.toString());
            }
        }
    }

    private void sendHeartbeat() throws Exception {
        if (self.brokerId() == clusterManager.coordinatorBrokerId()) {
            clusterManager.heartbeat(self.brokerId(), collectLogEndReports());
            return;
        }
        InterBrokerClient client = new InterBrokerClient(coordinatorHost, coordinatorReplicationPort, 2_000, 5_000);
        client.heartbeat(new InterBrokerCodec.HeartbeatRequest(self.brokerId(), collectLogEndReports()));
    }

    private List<InterBrokerCodec.PartitionLogEndReport> collectLogEndReports() {
        List<InterBrokerCodec.PartitionLogEndReport> reports = new ArrayList<>();
        for (PartitionReplicaAssignment assignment : clusterManager.allAssignments()) {
            if (!assignment.replicaIds().contains(self.brokerId())) {
                continue;
            }
            PartitionId partitionId = assignment.partitionId();
            long end = replicationEngine.followerLogEndOffset(partitionId);
            reports.add(
                    new InterBrokerCodec.PartitionLogEndReport(
                            partitionId.topic().value(), partitionId.index(), end));
        }
        return reports;
    }

    public void leaveCluster() {
        if (self.brokerId() == clusterManager.coordinatorBrokerId()) {
            return;
        }
        try {
            InterBrokerClient client =
                    new InterBrokerClient(coordinatorHost, coordinatorReplicationPort, 2_000, 5_000);
            client.deregister(self.brokerId());
        } catch (Exception e) {
            log.debug("Graceful cluster deregister failed for broker {}: {}", self.brokerId(), e.toString());
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false) && heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
    }
}
