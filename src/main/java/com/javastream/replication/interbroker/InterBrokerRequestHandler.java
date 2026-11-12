package com.javastream.replication.interbroker;

import com.javastream.broker.BrokerServices;
import com.javastream.cluster.ClusterManager;
import com.javastream.replication.ReplicationEngine;
import com.javastream.topic.TopicName;

/** Dispatches inter-broker RPC payloads on this broker's replication listener. */
public final class InterBrokerRequestHandler {

    private final int localBrokerId;
    private final ClusterManager clusterManager;
    private final ReplicationEngine replicationEngine;
    private final BrokerServices brokerServices;

    public InterBrokerRequestHandler(
            int localBrokerId,
            ClusterManager clusterManager,
            ReplicationEngine replicationEngine,
            BrokerServices brokerServices) {
        this.localBrokerId = localBrokerId;
        this.clusterManager = clusterManager;
        this.replicationEngine = replicationEngine;
        this.brokerServices = brokerServices;
    }

    public byte[] handle(InterBrokerOperation operation, byte[] payload) {
        return switch (operation) {
            case BROKER_REGISTER -> handleRegister(payload);
            case BROKER_HEARTBEAT -> handleHeartbeat(payload);
            case REPLICATE_RECORD -> handleReplicate(payload);
            case REPLICA_FETCH -> handleReplicaFetch(payload);
            case TOPIC_CREATED -> handleTopicCreated(payload);
            case ASSIGNMENT_PUSH -> handleAssignmentPush(payload);
            case BROKER_DEREGISTER -> handleDeregister(payload);
        };
    }

    private byte[] handleRegister(byte[] payload) {
        InterBrokerCodec.RegisterRequest request = InterBrokerCodec.decodeRegister(payload);
        if (!clusterManager.isCoordinator()) {
            throw new IllegalStateException("Registration accepted only on coordinator");
        }
        clusterManager.register(
                new com.javastream.cluster.BrokerEndpoint(
                        request.brokerId(),
                        request.host(),
                        request.clientPort(),
                        request.replicationPort()));
        return InterBrokerCodec.encodeAssignmentPush(clusterManager.buildAssignmentPush());
    }

    private byte[] handleHeartbeat(byte[] payload) {
        InterBrokerCodec.HeartbeatRequest request = InterBrokerCodec.decodeHeartbeat(payload);
        clusterManager.heartbeat(request.brokerId(), request.logEnds());
        return new byte[0];
    }

    private byte[] handleDeregister(byte[] payload) {
        int brokerId = InterBrokerCodec.decodeDeregister(payload);
        clusterManager.voluntaryLeave(brokerId);
        return new byte[0];
    }

    private byte[] handleReplicate(byte[] payload) {
        InterBrokerCodec.ReplicateRecordRequest request = InterBrokerCodec.decodeReplicate(payload);
        replicationEngine.applyReplicatedRecord(request);
        long end = replicationEngine.followerLogEndOffset(
                new com.javastream.partition.PartitionId(new TopicName(request.topic()), request.partition()));
        return InterBrokerCodec.encodeReplicateResponse(new InterBrokerCodec.ReplicateRecordResponse(end));
    }

    private byte[] handleReplicaFetch(byte[] payload) {
        InterBrokerCodec.ReplicaFetchRequest request = InterBrokerCodec.decodeReplicaFetch(payload);
        var records =
                replicationEngine.replicaFetch(
                        request.topic(), request.partition(), request.fromOffset(), request.maxRecords());
        return InterBrokerCodec.encodeReplicaFetchResponse(records);
    }

    private byte[] handleTopicCreated(byte[] payload) {
        InterBrokerCodec.TopicCreatedNotification notification = InterBrokerCodec.decodeTopicCreated(payload);
        TopicName topic = new TopicName(notification.topic());
        if (brokerServices.partitionCount(topic).isEmpty()) {
            brokerServices.registerTopic(topic, notification.partitionCount());
        }
        return new byte[0];
    }

    private byte[] handleAssignmentPush(byte[] payload) {
        InterBrokerCodec.AssignmentPush push = InterBrokerCodec.decodeAssignmentPush(payload);
        clusterManager.applyAssignments(push);
        replicationEngine.syncAssignmentsFromCluster();
        if (localBrokerId != clusterManager.coordinatorBrokerId()) {
            replicationEngine.catchUpAllAssignedFollowers();
        }
        return new byte[0];
    }
}
