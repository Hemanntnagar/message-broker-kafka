package com.javastream.cluster;

import com.javastream.partition.PartitionId;
import com.javastream.replication.interbroker.InterBrokerClient;
import com.javastream.replication.interbroker.InterBrokerCodec;
import com.javastream.topic.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Cluster coordinator state: broker registration, heartbeats, partition replica placement, and
 * deterministic leader election. Runs in authoritative mode on the coordinator broker only.
 */
public final class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private final int coordinatorBrokerId;
    private final int replicationFactor;
    private final long sessionTimeoutMs;
    private final ClusterMembership membership;
    private final boolean coordinator;

    private final ConcurrentHashMap<Integer, BrokerEndpoint> brokers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Long> lastHeartbeatMillis = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionId, PartitionReplicaAssignment> assignments = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> topicCatalog = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Runnable> assignmentListeners = new CopyOnWriteArrayList<>();

    public ClusterManager(
            int coordinatorBrokerId,
            int replicationFactor,
            long sessionTimeoutMs,
            ClusterMembership membership,
            boolean coordinator) {
        this.coordinatorBrokerId = coordinatorBrokerId;
        this.replicationFactor = Math.max(1, replicationFactor);
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.membership = membership;
        this.coordinator = coordinator;
    }

    public boolean isCoordinator() {
        return coordinator;
    }

    public int coordinatorBrokerId() {
        return coordinatorBrokerId;
    }

    public void addAssignmentListener(Runnable listener) {
        assignmentListeners.add(listener);
    }

    public BrokerEndpoint register(BrokerEndpoint endpoint) {
        if (!coordinator) {
            throw new IllegalStateException("Only coordinator broker may accept registrations");
        }
        brokers.put(endpoint.brokerId(), endpoint);
        lastHeartbeatMillis.put(endpoint.brokerId(), System.currentTimeMillis());
        membership.register(endpoint.id());
        log.info("Registered broker {} at {}:{} (replication {})", endpoint.brokerId(), endpoint.host(), endpoint.clientPort(), endpoint.replicationPort());
        rebalanceLeadersOnMembershipChange();
        broadcastTopicCatalog(endpoint);
        broadcastAssignments();
        return endpoint;
    }

    public void heartbeat(int brokerId) {
        if (!coordinator) {
            throw new IllegalStateException("Only coordinator broker may accept heartbeats");
        }
        if (!brokers.containsKey(brokerId)) {
            throw new IllegalArgumentException("Unknown broker " + brokerId);
        }
        lastHeartbeatMillis.put(brokerId, System.currentTimeMillis());
    }

    public void expireStaleBrokers() {
        if (!coordinator) {
            return;
        }
        long now = System.currentTimeMillis();
        List<Integer> expired = new ArrayList<>();
        for (Map.Entry<Integer, Long> entry : lastHeartbeatMillis.entrySet()) {
            if (now - entry.getValue() > sessionTimeoutMs) {
                expired.add(entry.getKey());
            }
        }
        for (int brokerId : expired) {
            if (brokerId == coordinatorBrokerId) {
                continue;
            }
            log.warn("Broker {} expired from cluster membership", brokerId);
            brokers.remove(brokerId);
            lastHeartbeatMillis.remove(brokerId);
            membership.deregister(new BrokerId(brokerId));
        }
        if (!expired.isEmpty()) {
            rebalanceLeadersOnMembershipChange();
        }
    }

    public Set<BrokerEndpoint> liveBrokers() {
        return Set.copyOf(brokers.values());
    }

    public Optional<BrokerEndpoint> broker(int brokerId) {
        return Optional.ofNullable(brokers.get(brokerId));
    }

    public void applyAssignments(InterBrokerCodec.AssignmentPush push) {
        for (InterBrokerCodec.BrokerDirectoryEntry directoryEntry : push.brokers()) {
            brokers.put(
                    directoryEntry.brokerId(),
                    new BrokerEndpoint(
                            directoryEntry.brokerId(),
                            directoryEntry.host(),
                            directoryEntry.clientPort(),
                            directoryEntry.replicationPort()));
        }
        for (InterBrokerCodec.PartitionAssignmentEntry entry : push.assignments()) {
            PartitionId partitionId = new PartitionId(new TopicName(entry.topic()), entry.partition());
            assignments.put(
                    partitionId,
                    new PartitionReplicaAssignment(partitionId, entry.leaderId(), entry.replicaIds()));
        }
        notifyAssignmentListeners();
    }

    public Optional<PartitionReplicaAssignment> assignment(PartitionId partitionId) {
        return Optional.ofNullable(assignments.get(partitionId));
    }

    public List<PartitionReplicaAssignment> allAssignments() {
        return List.copyOf(assignments.values());
    }

    public void onTopicCreated(TopicName topic, int partitionCount) {
        if (!coordinator) {
            return;
        }
        topicCatalog.put(topic.value(), partitionCount);
        List<Integer> brokerIds = brokers.keySet().stream().sorted().toList();
        if (brokerIds.isEmpty()) {
            return;
        }
        int effectiveRf = Math.min(replicationFactor, brokerIds.size());
        for (int partition = 0; partition < partitionCount; partition++) {
            List<Integer> replicas = new ArrayList<>(effectiveRf);
            int start = partition % brokerIds.size();
            for (int i = 0; i < effectiveRf; i++) {
                replicas.add(brokerIds.get((start + i) % brokerIds.size()));
            }
            int leader = electLeader(replicas, aliveBrokerIds());
            PartitionId partitionId = new PartitionId(topic, partition);
            assignments.put(partitionId, new PartitionReplicaAssignment(partitionId, leader, replicas));
        }
        notifyAssignmentListeners();
        broadcastAssignments();
        broadcastTopicCreated(topic, partitionCount);
    }

    private void broadcastTopicCreated(TopicName topic, int partitionCount) {
        sendTopicCreated(topic.value(), partitionCount, brokers.values());
    }

    private void broadcastTopicCatalog(BrokerEndpoint target) {
        for (Map.Entry<String, Integer> entry : topicCatalog.entrySet()) {
            sendTopicCreated(entry.getKey(), entry.getValue(), List.of(target));
        }
    }

    private void sendTopicCreated(String topic, int partitionCount, Iterable<BrokerEndpoint> targets) {
        var notification = new InterBrokerCodec.TopicCreatedNotification(topic, partitionCount);
        byte[] payload = InterBrokerCodec.encodeTopicCreated(notification);
        for (BrokerEndpoint endpoint : targets) {
            if (endpoint.brokerId() == coordinatorBrokerId) {
                continue;
            }
            try {
                new InterBrokerClient(endpoint.host(), endpoint.replicationPort(), 2_000, 5_000)
                        .rpc(
                                com.javastream.replication.interbroker.InterBrokerOperation.TOPIC_CREATED,
                                payload);
            } catch (Exception e) {
                log.debug("Failed to notify broker {} about topic {}: {}", endpoint.brokerId(), topic, e.toString());
            }
        }
    }

    private void broadcastAssignments() {
        InterBrokerCodec.AssignmentPush push = buildAssignmentPush();
        byte[] payload = InterBrokerCodec.encodeAssignmentPush(push);
        for (BrokerEndpoint endpoint : brokers.values()) {
            if (endpoint.brokerId() == coordinatorBrokerId) {
                continue;
            }
            try {
                new InterBrokerClient(endpoint.host(), endpoint.replicationPort(), 2_000, 5_000)
                        .rpc(
                                com.javastream.replication.interbroker.InterBrokerOperation.ASSIGNMENT_PUSH,
                                payload);
            } catch (Exception e) {
                log.debug("Failed to push assignments to broker {}: {}", endpoint.brokerId(), e.toString());
            }
        }
    }

    public InterBrokerCodec.AssignmentPush buildAssignmentPush() {
        List<InterBrokerCodec.BrokerDirectoryEntry> directory = new ArrayList<>();
        for (BrokerEndpoint endpoint : brokers.values()) {
            directory.add(
                    new InterBrokerCodec.BrokerDirectoryEntry(
                            endpoint.brokerId(),
                            endpoint.host(),
                            endpoint.clientPort(),
                            endpoint.replicationPort()));
        }
        List<InterBrokerCodec.PartitionAssignmentEntry> entries = new ArrayList<>();
        for (PartitionReplicaAssignment assignment : assignments.values()) {
            PartitionId id = assignment.partitionId();
            entries.add(
                    new InterBrokerCodec.PartitionAssignmentEntry(
                            id.topic().value(),
                            id.index(),
                            assignment.leaderId(),
                            assignment.replicaIds()));
        }
        return new InterBrokerCodec.AssignmentPush(directory, entries);
    }

    private void rebalanceLeadersOnMembershipChange() {
        Set<Integer> alive = aliveBrokerIds();
        boolean changed = false;
        for (Map.Entry<PartitionId, PartitionReplicaAssignment> entry : assignments.entrySet()) {
            PartitionReplicaAssignment current = entry.getValue();
            if (!alive.contains(current.leaderId())) {
                int newLeader = electLeader(current.replicaIds(), alive);
                if (newLeader != current.leaderId()) {
                    entry.setValue(new PartitionReplicaAssignment(entry.getKey(), newLeader, current.replicaIds()));
                    changed = true;
                    log.info("Failover: partition {} leader {} -> {}", entry.getKey(), current.leaderId(), newLeader);
                }
            }
        }
        if (changed) {
            notifyAssignmentListeners();
            broadcastAssignments();
        }
    }

    private Set<Integer> aliveBrokerIds() {
        long now = System.currentTimeMillis();
        return brokers.keySet().stream()
                .filter(id -> now - lastHeartbeatMillis.getOrDefault(id, 0L) <= sessionTimeoutMs)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    /**
     * Deterministic leader pick: smallest broker id among alive replicas. Not consensus-safe.
     */
    public static int electLeader(List<Integer> replicaIds, Set<Integer> aliveBrokerIds) {
        return replicaIds.stream()
                .filter(aliveBrokerIds::contains)
                .min(Comparator.naturalOrder())
                .orElse(replicaIds.stream().min(Comparator.naturalOrder()).orElse(replicaIds.getFirst()));
    }

    private void notifyAssignmentListeners() {
        for (Runnable listener : assignmentListeners) {
            try {
                listener.run();
            } catch (Exception e) {
                log.warn("Assignment listener failed: {}", e.toString());
            }
        }
    }
}
