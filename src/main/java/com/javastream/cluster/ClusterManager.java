package com.javastream.cluster;

import com.javastream.partition.PartitionId;
import com.javastream.replication.interbroker.InterBrokerClient;
import com.javastream.replication.interbroker.InterBrokerCodec;
import com.javastream.topic.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
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

    private final ConcurrentHashMap<Integer, BrokerSession> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionId, PartitionReplicaAssignment> assignments = new ConcurrentHashMap<>();
    private final BrokerReachabilityProbe reachabilityProbe = new BrokerReachabilityProbe(500);
    /** Heartbeat loss beyond session timeout is suspicion only until probes fail or this factor elapses. */
    private static final int EXPIRY_CONFIRMATION_FACTOR = 2;
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
        long now = System.currentTimeMillis();
        sessions.put(endpoint.brokerId(), new BrokerSession(endpoint, now));
        membership.register(endpoint.id());
        log.info("Registered broker {} at {}:{} (replication {})", endpoint.brokerId(), endpoint.host(), endpoint.clientPort(), endpoint.replicationPort());
        rebalanceLeadersOnMembershipChange();
        broadcastTopicCatalog(endpoint);
        broadcastAssignments();
        return endpoint;
    }

    public void heartbeat(int brokerId) {
        heartbeat(brokerId, List.of());
    }

    public void heartbeat(int brokerId, List<InterBrokerCodec.PartitionLogEndReport> logEnds) {
        if (!coordinator) {
            throw new IllegalStateException("Only coordinator broker may accept heartbeats");
        }
        BrokerSession session = sessions.get(brokerId);
        if (session == null) {
            throw new IllegalArgumentException("Unknown broker " + brokerId);
        }
        long now = System.currentTimeMillis();
        session.lastHeartbeatMillis = now;
        session.firstMissedHeartbeatMillis = 0;
        session.consecutiveFailedProbes = 0;
        session.state = BrokerSessionState.AVAILABLE;
        for (InterBrokerCodec.PartitionLogEndReport report : logEnds) {
            PartitionId partitionId = new PartitionId(new TopicName(report.topic()), report.partition());
            session.reportedLogEnds.put(partitionId, report.logEndOffset());
        }
    }

    public void voluntaryLeave(int brokerId) {
        if (!coordinator) {
            return;
        }
        if (brokerId == coordinatorBrokerId) {
            return;
        }
        log.info("Broker {} left the cluster voluntarily", brokerId);
        removeBrokerSession(brokerId);
        rebalanceLeadersOnMembershipChange();
        broadcastAssignments();
    }

    public void expireStaleBrokers() {
        if (!coordinator) {
            return;
        }
        long now = System.currentTimeMillis();
        List<Integer> expired = new ArrayList<>();
        for (Map.Entry<Integer, BrokerSession> entry : sessions.entrySet()) {
            int brokerId = entry.getKey();
            if (brokerId == coordinatorBrokerId) {
                continue;
            }
            BrokerSession session = entry.getValue();
            long sinceHeartbeat = now - session.lastHeartbeatMillis;
            if (sinceHeartbeat <= sessionTimeoutMs) {
                if (session.state != BrokerSessionState.AVAILABLE) {
                    session.state = BrokerSessionState.AVAILABLE;
                }
                continue;
            }
            if (session.firstMissedHeartbeatMillis == 0) {
                session.firstMissedHeartbeatMillis = session.lastHeartbeatMillis + sessionTimeoutMs;
            }
            session.state = BrokerSessionState.SUSPECTED_UNAVAILABLE;
            boolean probeOk = reachabilityProbe.canConnect(session.endpoint);
            if (probeOk) {
                session.consecutiveFailedProbes = 0;
                log.debug(
                        "Broker {} past heartbeat deadline but replication port reachable — keeping as suspected",
                        brokerId);
                if (sinceHeartbeat >= sessionTimeoutMs * EXPIRY_CONFIRMATION_FACTOR) {
                    session.state = BrokerSessionState.CONFIRMED_UNAVAILABLE;
                    expired.add(brokerId);
                }
                continue;
            }
            session.consecutiveFailedProbes++;
            if (session.consecutiveFailedProbes >= 2
                    || sinceHeartbeat >= sessionTimeoutMs * EXPIRY_CONFIRMATION_FACTOR) {
                session.state = BrokerSessionState.CONFIRMED_UNAVAILABLE;
                expired.add(brokerId);
            }
        }
        for (int brokerId : expired) {
            log.warn("Broker {} confirmed unavailable — removing from cluster membership", brokerId);
            removeBrokerSession(brokerId);
        }
        if (!expired.isEmpty()) {
            rebalanceLeadersOnMembershipChange();
        }
    }

    private void removeBrokerSession(int brokerId) {
        sessions.remove(brokerId);
        membership.deregister(new BrokerId(brokerId));
    }

    public Set<BrokerEndpoint> liveBrokers() {
        List<BrokerEndpoint> endpoints = new ArrayList<>();
        for (BrokerSession session : sessions.values()) {
            if (session.state != BrokerSessionState.CONFIRMED_UNAVAILABLE) {
                endpoints.add(session.endpoint);
            }
        }
        return Set.copyOf(endpoints);
    }

    public Optional<BrokerEndpoint> broker(int brokerId) {
        BrokerSession session = sessions.get(brokerId);
        if (session == null || session.state == BrokerSessionState.CONFIRMED_UNAVAILABLE) {
            return Optional.empty();
        }
        return Optional.of(session.endpoint);
    }

    public Optional<BrokerSessionState> brokerState(int brokerId) {
        BrokerSession session = sessions.get(brokerId);
        return session == null ? Optional.empty() : Optional.of(session.state);
    }

    public void applyAssignments(InterBrokerCodec.AssignmentPush push) {
        long now = System.currentTimeMillis();
        for (InterBrokerCodec.BrokerDirectoryEntry directoryEntry : push.brokers()) {
            BrokerEndpoint endpoint =
                    new BrokerEndpoint(
                            directoryEntry.brokerId(),
                            directoryEntry.host(),
                            directoryEntry.clientPort(),
                            directoryEntry.replicationPort());
            sessions.compute(
                    directoryEntry.brokerId(),
                    (id, existing) ->
                            existing == null ? new BrokerSession(endpoint, now) : existing.withEndpoint(endpoint));
        }
        for (InterBrokerCodec.PartitionAssignmentEntry entry : push.assignments()) {
            PartitionId partitionId = new PartitionId(new TopicName(entry.topic()), entry.partition());
            assignments.put(
                    partitionId,
                    new PartitionReplicaAssignment(
                            partitionId, entry.leaderId(), entry.replicaIds(), entry.leaderEpoch()));
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
        List<Integer> brokerIds =
                sessions.keySet().stream()
                        .filter(id -> brokerState(id).orElse(BrokerSessionState.CONFIRMED_UNAVAILABLE)
                                != BrokerSessionState.CONFIRMED_UNAVAILABLE)
                        .sorted()
                        .toList();
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
        sendTopicCreated(topic.value(), partitionCount, registeredEndpoints());
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
        for (BrokerEndpoint endpoint : registeredEndpoints()) {
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
        for (BrokerEndpoint endpoint : registeredEndpoints()) {
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
                            assignment.replicaIds(),
                            assignment.leaderEpoch()));
        }
        return new InterBrokerCodec.AssignmentPush(directory, entries);
    }

    private void rebalanceLeadersOnMembershipChange() {
        Set<Integer> alive = aliveBrokerIds();
        boolean changed = false;
        for (Map.Entry<PartitionId, PartitionReplicaAssignment> entry : assignments.entrySet()) {
            PartitionReplicaAssignment current = entry.getValue();
            PartitionId partitionId = entry.getKey();
            if (!alive.contains(current.leaderId())) {
                int newLeader = electLeaderConsistent(partitionId, current.replicaIds(), alive);
                if (newLeader != current.leaderId()) {
                    long epoch = current.leaderEpoch() + 1;
                    entry.setValue(
                            new PartitionReplicaAssignment(partitionId, newLeader, current.replicaIds(), epoch));
                    changed = true;
                    log.info(
                            "Failover: partition {} leader {} -> {} (epoch {})",
                            partitionId,
                            current.leaderId(),
                            newLeader,
                            epoch);
                }
            }
        }
        if (changed) {
            notifyAssignmentListeners();
            broadcastAssignments();
        }
    }

    private Set<Integer> aliveBrokerIds() {
        Set<Integer> alive = new HashSet<>();
        for (Map.Entry<Integer, BrokerSession> entry : sessions.entrySet()) {
            if (entry.getValue().state != BrokerSessionState.CONFIRMED_UNAVAILABLE) {
                alive.add(entry.getKey());
            }
        }
        return Set.copyOf(alive);
    }

    private List<BrokerEndpoint> registeredEndpoints() {
        return sessions.values().stream().map(session -> session.endpoint).toList();
    }

    int electLeaderConsistent(PartitionId partitionId, List<Integer> replicaIds, Set<Integer> aliveBrokerIds) {
        long maxLogEnd = 0;
        for (int replicaId : replicaIds) {
            BrokerSession session = sessions.get(replicaId);
            if (session == null) {
                continue;
            }
            maxLogEnd = Math.max(maxLogEnd, session.reportedLogEnds.getOrDefault(partitionId, 0L));
        }
        List<Integer> caughtUp = new ArrayList<>();
        for (int replicaId : replicaIds) {
            if (!aliveBrokerIds.contains(replicaId)) {
                continue;
            }
            BrokerSession session = sessions.get(replicaId);
            long end = session == null ? 0L : session.reportedLogEnds.getOrDefault(partitionId, 0L);
            if (maxLogEnd == 0 || end >= maxLogEnd) {
                caughtUp.add(replicaId);
            }
        }
        if (!caughtUp.isEmpty()) {
            return caughtUp.stream().min(Comparator.naturalOrder()).orElseThrow();
        }
        return electLeader(replicaIds, aliveBrokerIds);
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
