package com.javastream.replication;

import com.javastream.broker.BrokerServices;
import com.javastream.cluster.BrokerEndpoint;
import com.javastream.cluster.ClusterManager;
import com.javastream.cluster.PartitionReplicaAssignment;
import com.javastream.delivery.ProducerAckLevel;
import com.javastream.partition.PartitionId;
import com.javastream.replication.interbroker.InterBrokerClient;
import com.javastream.replication.interbroker.InterBrokerCodec;
import com.javastream.storage.BrokerMessageStore;
import com.javastream.topic.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Leader-side replication, follower catch-up, and ISR offset tracking. */
public final class ReplicationEngine {

    private static final Logger log = LoggerFactory.getLogger(ReplicationEngine.class);

    private final int localBrokerId;
    private final ClusterManager clusterManager;
    private final ClusterReplicaManager replicaManager;
    private final BrokerServices brokerServices;

    public ReplicationEngine(
            int localBrokerId,
            ClusterManager clusterManager,
            ClusterReplicaManager replicaManager,
            BrokerServices brokerServices) {
        this.localBrokerId = localBrokerId;
        this.clusterManager = Objects.requireNonNull(clusterManager);
        this.replicaManager = Objects.requireNonNull(replicaManager);
        this.brokerServices = Objects.requireNonNull(brokerServices);
    }

    public BrokerServices brokerServices() {
        return brokerServices;
    }

    public void syncAssignmentsFromCluster() {
        for (PartitionReplicaAssignment assignment : clusterManager.allAssignments()) {
            applyAssignment(assignment);
        }
    }

    public void applyAssignment(PartitionReplicaAssignment assignment) {
        replicaManager.configureReplicas(
                assignment.partitionId(), assignment.leaderId(), assignment.replicaIds());
        brokerServices
                .leadership()
                .ifPresent(leadership -> leadership.epochRegistry().apply(assignment));
    }

    public void onTopicCreatedLocally(TopicName topic, int partitionCount) {
        if (clusterManager.isCoordinator()) {
            clusterManager.onTopicCreated(topic, partitionCount);
        }
        syncAssignmentsFromCluster();
    }

    public void replicateAfterLeaderAppend(
            PartitionId partitionId,
            long messageId,
            long offset,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers,
            long timestampEpochMillis) {
        PartitionReplicaAssignment assignment =
                clusterManager.assignment(partitionId).orElse(null);
        if (assignment == null || assignment.leaderId() != localBrokerId) {
            return;
        }
        long fencingEpoch = assignment.leaderEpoch();
        brokerServices
                .leadership()
                .ifPresent(
                        leadership -> {
                            if (leadership.epochRegistry().epoch(partitionId) < fencingEpoch) {
                                throw new IllegalStateException(
                                        "Fenced leader cannot replicate on " + partitionId);
                            }
                        });
        replicaManager.recordLeaderAppend(partitionId, offset);
        for (int replicaId : assignment.replicaIds()) {
            if (replicaId == localBrokerId) {
                continue;
            }
            BrokerEndpoint endpoint =
                    clusterManager.broker(replicaId).orElse(null);
            if (endpoint == null) {
                replicaManager.removeBrokerFromIsr(partitionId, replicaId);
                continue;
            }
            var request =
                    new InterBrokerCodec.ReplicateRecordRequest(
                            partitionId.topic().value(),
                            partitionId.index(),
                            messageId,
                            offset,
                            timestampEpochMillis,
                            key,
                            value,
                            headers,
                            fencingEpoch);
            try {
                InterBrokerClient client = new InterBrokerClient(endpoint.host(), endpoint.replicationPort(), 2_000, 10_000);
                InterBrokerCodec.ReplicateRecordResponse response = client.replicate(request);
                replicaManager.recordReplicaOffset(partitionId, replicaId, response.replicaLogEndOffset());
                replicaManager.markReplicaInSync(partitionId, replicaId);
            } catch (Exception e) {
                log.warn(
                        "Replication to broker {} failed for {}: {}",
                        replicaId,
                        partitionId,
                        e.toString());
                replicaManager.removeBrokerFromIsr(partitionId, replicaId);
            }
        }
    }

    public void applyReplicatedRecord(InterBrokerCodec.ReplicateRecordRequest request) {
        TopicName topic = new TopicName(request.topic());
        PartitionId partitionId = new PartitionId(topic, request.partition());
        PartitionReplicaAssignment assignment =
                clusterManager.assignment(partitionId).orElse(null);
        if (assignment != null
                && request.leaderEpoch() > 0
                && request.leaderEpoch() < assignment.leaderEpoch()) {
            throw new IllegalStateException(
                    "Rejecting replicate from stale leader epoch "
                            + request.leaderEpoch()
                            + " < "
                            + assignment.leaderEpoch());
        }
        if (brokerServices.partitionCount(topic).isEmpty()) {
            throw new IllegalStateException("Replica received record for unknown topic " + topic);
        }
        long localEnd = brokerServices.messageStore().fetch(partitionId, 0, Integer.MAX_VALUE).size();
        if (request.offset() != localEnd) {
            throw new IllegalStateException(
                    "Replica offset gap on %s: expected next %d, got %d"
                            .formatted(partitionId, localEnd, request.offset()));
        }
        BrokerMessageStore.AppendResult append =
                brokerServices.appendRecord(
                        partitionId,
                        request.key(),
                        request.value(),
                        request.headers(),
                        request.timestampEpochMillis());
        if (append.offset() != request.offset()) {
            throw new IllegalStateException("Replica offset mismatch after append on " + partitionId);
        }
        replicaManager.recordReplicaOffset(partitionId, localBrokerId, append.offset() + 1);
        replicaManager.markReplicaInSync(partitionId, localBrokerId);
    }

    public long followerLogEndOffset(PartitionId partitionId) {
        return brokerServices.messageStore().fetch(partitionId, 0, Integer.MAX_VALUE).size();
    }

    public List<BrokerMessageStore.StoredRecord> replicaFetch(
            String topic, int partition, long fromOffset, int maxRecords) {
        PartitionId partitionId = new PartitionId(new TopicName(topic), partition);
        return brokerServices.messageStore().fetch(partitionId, fromOffset, maxRecords);
    }

    public void catchUpLocalFollower(PartitionId partitionId) {
        PartitionReplicaAssignment assignment =
                clusterManager.assignment(partitionId).orElse(null);
        if (assignment == null || assignment.leaderId() == localBrokerId) {
            return;
        }
        if (!assignment.replicaIds().contains(localBrokerId)) {
            return;
        }
        int leaderId = assignment.leaderId();
        BrokerEndpoint leader =
                clusterManager.broker(leaderId).orElse(null);
        if (leader == null) {
            return;
        }
        long localEnd = followerLogEndOffset(partitionId);
        try {
            InterBrokerClient client = new InterBrokerClient(leader.host(), leader.replicationPort(), 2_000, 15_000);
            List<BrokerMessageStore.StoredRecord> batch;
            do {
                var fetchRequest =
                        new InterBrokerCodec.ReplicaFetchRequest(
                                partitionId.topic().value(), partitionId.index(), localEnd, 500);
                batch = client.replicaFetch(fetchRequest);
                for (BrokerMessageStore.StoredRecord record : batch) {
                    var replicate =
                            new InterBrokerCodec.ReplicateRecordRequest(
                                    partitionId.topic().value(),
                                    partitionId.index(),
                                    record.messageId(),
                                    record.offset(),
                                    record.timestampEpochMillis(),
                                    record.key(),
                                    record.value(),
                                    record.headers());
                    applyReplicatedRecord(replicate);
                    localEnd = record.offset() + 1;
                }
            } while (!batch.isEmpty());
            replicaManager.markReplicaInSync(partitionId, localBrokerId);
        } catch (Exception e) {
            log.warn("Follower catch-up failed for {}: {}", partitionId, e.toString());
        }
    }

    public void catchUpAllAssignedFollowers() {
        for (PartitionReplicaAssignment assignment : clusterManager.allAssignments()) {
            if (assignment.replicaIds().contains(localBrokerId) && assignment.leaderId() != localBrokerId) {
                catchUpLocalFollower(assignment.partitionId());
            }
        }
    }

    public void awaitAcknowledgement(
            ProducerAckLevel ackLevel, PartitionId partitionId, long leaderOffset) {
        if (ackLevel != ProducerAckLevel.ACK_ALL_REPLICAS) {
            return;
        }
        replicaManager.awaitThrough(partitionId, leaderOffset, 10_000);
    }
}
