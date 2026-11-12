package com.javastream.group;

import com.javastream.exception.JavaStreamException;
import com.javastream.topic.TopicName;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Coordinates consumer group membership, partition assignment, heartbeats, and per-group offset commits.
 */
public final class ConsumerGroupManager implements ConsumerGroupCoordinator, AutoCloseable {

    private final Function<TopicName, Optional<Integer>> partitionCountLookup;
    private final GroupOffsetStore offsetStore;
    private final RoundRobinPartitionAssignor assignor = new RoundRobinPartitionAssignor();
    private final long sessionTimeoutMillis;

    private final ReentrantLock groupLock = new ReentrantLock();
    private final Map<String, GroupState> groups = new HashMap<>();
    private final Map<GroupPartitionOffsetKey, Long> committedOffsets = new ConcurrentHashMap<>();
    private final Map<GroupPartitionKey, String> partitionOwners = new HashMap<>();

    private final ScheduledExecutorService expirationScheduler;
    private volatile boolean closed;

    public ConsumerGroupManager(
            Function<TopicName, Optional<Integer>> partitionCountLookup, Path offsetCatalogFile) {
        this(partitionCountLookup, offsetCatalogFile, Duration.ofSeconds(10));
    }

    public ConsumerGroupManager(
            Function<TopicName, Optional<Integer>> partitionCountLookup,
            Path offsetCatalogFile,
            Duration sessionTimeout) {
        this.partitionCountLookup =
                Objects.requireNonNull(partitionCountLookup, "partitionCountLookup");
        this.offsetStore = new GroupOffsetStore(offsetCatalogFile);
        this.sessionTimeoutMillis = sessionTimeout.toMillis();
        committedOffsets.putAll(offsetStore.load());
        this.expirationScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        Thread.ofPlatform().daemon().name("consumer-group-expiry").factory());
        long sweepPeriod = Math.max(500, sessionTimeoutMillis / 3);
        expirationScheduler.scheduleAtFixedRate(
                this::expireInactiveMembersQuietly, sweepPeriod, sweepPeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    public JoinGroupResult joinGroup(String groupId, String memberId, String topic) {
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(memberId, "memberId");
        Objects.requireNonNull(topic, "topic");
        TopicName topicName = new TopicName(topic);
        int partitionCount =
                partitionCountLookup
                        .apply(topicName)
                        .orElseThrow(() -> new JavaStreamException("Unknown topic: " + topic));

        groupLock.lock();
        try {
            GroupState group =
                    groups.computeIfAbsent(groupId, id -> new GroupState(id, topic, partitionCount));
            if (!group.topic.equals(topic)) {
                throw new JavaStreamException(
                        "Group " + groupId + " is already subscribed to " + group.topic + ", not " + topic);
            }
            if (group.partitionCount != partitionCount) {
                group.partitionCount = partitionCount;
            }

            List<Integer> previousAssignment =
                    group.members.containsKey(memberId)
                            ? List.copyOf(group.members.get(memberId).assignedPartitions)
                            : List.of();

            group.members.put(memberId, new MemberState(memberId, System.currentTimeMillis(), List.of()));
            RebalanceResult rebalance = rebalanceLocked(group);

            List<Integer> assigned = rebalance.assignments().getOrDefault(memberId, List.of());
            List<Integer> revoked = new ArrayList<>(previousAssignment);
            revoked.removeAll(assigned);

            return new JoinGroupResult(
                    group.generation,
                    List.copyOf(assigned),
                    List.copyOf(revoked),
                    fetchCommittedOffsetsForMember(groupId, topic, assigned));
        } finally {
            groupLock.unlock();
        }
    }

    @Override
    public void heartbeat(String groupId, String memberId) {
        groupLock.lock();
        try {
            MemberState member = requireMember(groupId, memberId);
            member.lastHeartbeatMillis = System.currentTimeMillis();
        } finally {
            groupLock.unlock();
        }
    }

    @Override
    public LeaveGroupResult leaveGroup(String groupId, String memberId) {
        groupLock.lock();
        try {
            GroupState group = groups.get(groupId);
            if (group == null) {
                return new LeaveGroupResult(0, List.of(), Map.of());
            }
            MemberState removed = group.members.remove(memberId);
            if (removed == null) {
                return new LeaveGroupResult(group.generation, List.of(), Map.of());
            }
            List<Integer> revoked = List.copyOf(removed.assignedPartitions);
            if (group.members.isEmpty()) {
                groups.remove(groupId);
                clearPartitionOwnersForGroup(groupId);
                return new LeaveGroupResult(group.generation, revoked, Map.of());
            }
            RebalanceResult rebalance = rebalanceLocked(group);
            return new LeaveGroupResult(
                    group.generation, revoked, copyAssignmentsExcluding(rebalance.assignments(), memberId));
        } finally {
            groupLock.unlock();
        }
    }

    @Override
    public void commitOffsets(String groupId, String topic, Map<Integer, Long> offsets) {
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(offsets, "offsets");
        if (offsets.isEmpty()) {
            return;
        }
        groupLock.lock();
        try {
            requireGroup(groupId, topic);
            for (var entry : offsets.entrySet()) {
                int partition = entry.getKey();
                long offset = entry.getValue();
                if (partition < 0) {
                    throw new IllegalArgumentException("partition must be non-negative");
                }
                if (offset < 0) {
                    throw new IllegalArgumentException("offset must be non-negative");
                }
                verifyOwnership(groupId, partition, null);
                GroupPartitionOffsetKey key = new GroupPartitionOffsetKey(groupId, topic, partition);
                committedOffsets.merge(key, offset, Math::max);
            }
            offsetStore.persist(Map.copyOf(committedOffsets));
        } finally {
            groupLock.unlock();
        }
    }

    @Override
    public Map<Integer, Long> fetchCommittedOffsets(String groupId, String topic, List<Integer> partitions) {
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(topic, "topic");
        groupLock.lock();
        try {
            GroupState group = groups.get(groupId);
            if (group != null && !group.topic.equals(topic)) {
                throw new JavaStreamException(
                        "Group " + groupId + " is subscribed to " + group.topic + ", not " + topic);
            }
            List<Integer> query;
            if (partitions == null || partitions.isEmpty()) {
                query = group != null ? allPartitionsForGroup(groupId) : List.of();
            } else {
                query = List.copyOf(partitions);
            }
            Map<Integer, Long> result = new LinkedHashMap<>();
            for (int partition : query) {
                GroupPartitionOffsetKey key = new GroupPartitionOffsetKey(groupId, topic, partition);
                long offset = committedOffsets.getOrDefault(key, -1L);
                result.put(partition, offset);
            }
            return Map.copyOf(result);
        } finally {
            groupLock.unlock();
        }
    }

    public Optional<String> partitionOwner(String groupId, int partition) {
        groupLock.lock();
        try {
            return Optional.ofNullable(partitionOwners.get(new GroupPartitionKey(groupId, partition)));
        } finally {
            groupLock.unlock();
        }
    }

    public Map<String, List<Integer>> currentAssignment(String groupId) {
        groupLock.lock();
        try {
            GroupState group = groups.get(groupId);
            if (group == null) {
                return Map.of();
            }
            Map<String, List<Integer>> out = new LinkedHashMap<>();
            for (var entry : group.members.entrySet()) {
                out.put(entry.getKey(), List.copyOf(entry.getValue().assignedPartitions));
            }
            return Map.copyOf(out);
        } finally {
            groupLock.unlock();
        }
    }

    public com.javastream.admin.BrokerAdminService.GroupDescription describeGroup(String groupId) {
        Objects.requireNonNull(groupId, "groupId");
        groupLock.lock();
        try {
            GroupState group = groups.get(groupId);
            if (group == null) {
                throw new JavaStreamException("Unknown consumer group: " + groupId);
            }
            return new com.javastream.admin.BrokerAdminService.GroupDescription(
                    group.groupId, group.topic, group.generation, currentAssignment(groupId));
        } finally {
            groupLock.unlock();
        }
    }

  public Map<GroupPartitionOffsetKey, Long> committedOffsetsSnapshot() {
        return Map.copyOf(committedOffsets);
    }

    void expireInactiveMembersForTests() {
        expireInactiveMembers();
    }

    private void expireInactiveMembersQuietly() {
        if (closed) {
            return;
        }
        try {
            expireInactiveMembers();
        } catch (RuntimeException ignored) {
            // daemon sweep must not kill the broker
        }
    }

    private void expireInactiveMembers() {
        long now = System.currentTimeMillis();
        groupLock.lock();
        try {
            for (GroupState group : new ArrayList<>(groups.values())) {
                List<String> expired = new ArrayList<>();
                for (MemberState member : group.members.values()) {
                    if (now - member.lastHeartbeatMillis > sessionTimeoutMillis) {
                        expired.add(member.memberId);
                    }
                }
                if (expired.isEmpty()) {
                    continue;
                }
                for (String memberId : expired) {
                    group.members.remove(memberId);
                }
                if (group.members.isEmpty()) {
                    groups.remove(group.groupId);
                    clearPartitionOwnersForGroup(group.groupId);
                } else {
                    rebalanceLocked(group);
                }
            }
        } finally {
            groupLock.unlock();
        }
    }

    private RebalanceResult rebalanceLocked(GroupState group) {
        group.generation++;
        List<String> memberIds = new ArrayList<>(new TreeSet<>(group.members.keySet()));
        Map<String, List<Integer>> assignments = assignor.assign(memberIds, group.partitionCount);

        clearPartitionOwnersForGroup(group.groupId);
        for (var entry : assignments.entrySet()) {
            String memberId = entry.getKey();
            List<Integer> partitions = entry.getValue();
            group.members.get(memberId).assignedPartitions = List.copyOf(partitions);
            for (int partition : partitions) {
                GroupPartitionKey ownerKey = new GroupPartitionKey(group.groupId, partition);
                if (partitionOwners.put(ownerKey, memberId) != null) {
                    throw new IllegalStateException(
                            "Duplicate partition owner for group " + group.groupId + " partition " + partition);
                }
            }
        }
        return new RebalanceResult(group.generation, assignments);
    }

    private Map<Integer, Long> fetchCommittedOffsetsForMember(
            String groupId, String topic, List<Integer> partitions) {
        Map<Integer, Long> offsets = new LinkedHashMap<>();
        for (int partition : partitions) {
            GroupPartitionOffsetKey key = new GroupPartitionOffsetKey(groupId, topic, partition);
            offsets.put(partition, committedOffsets.getOrDefault(key, -1L));
        }
        return Map.copyOf(offsets);
    }

    private static Map<String, List<Integer>> copyAssignmentsExcluding(
            Map<String, List<Integer>> assignments, String excludedMember) {
        Map<String, List<Integer>> copy = new LinkedHashMap<>();
        for (var entry : assignments.entrySet()) {
            if (!entry.getKey().equals(excludedMember)) {
                copy.put(entry.getKey(), List.copyOf(entry.getValue()));
            }
        }
        return Map.copyOf(copy);
    }

    private GroupState requireGroup(String groupId, String topic) {
        GroupState group = groups.get(groupId);
        if (group == null) {
            throw new JavaStreamException("Unknown consumer group: " + groupId);
        }
        if (!group.topic.equals(topic)) {
            throw new JavaStreamException("Group " + groupId + " is subscribed to " + group.topic + ", not " + topic);
        }
        return group;
    }

    private MemberState requireMember(String groupId, String memberId) {
        GroupState group = groups.get(groupId);
        if (group == null) {
            throw new JavaStreamException("Unknown consumer group: " + groupId);
        }
        MemberState member = group.members.get(memberId);
        if (member == null) {
            throw new JavaStreamException("Unknown group member " + memberId + " in group " + groupId);
        }
        return member;
    }

    private void verifyOwnership(String groupId, int partition, String memberId) {
        String owner = partitionOwners.get(new GroupPartitionKey(groupId, partition));
        if (owner == null) {
            throw new JavaStreamException(
                    "Partition " + partition + " is not assigned to any member of group " + groupId);
        }
        if (memberId != null && !owner.equals(memberId)) {
            throw new JavaStreamException(
                    "Member " + memberId + " does not own partition " + partition + " in group " + groupId);
        }
    }

    private List<Integer> allPartitionsForGroup(String groupId) {
        GroupState group = groups.get(groupId);
        if (group == null) {
            return List.of();
        }
        List<Integer> partitions = new ArrayList<>(group.partitionCount);
        for (int i = 0; i < group.partitionCount; i++) {
            partitions.add(i);
        }
        return partitions;
    }

    private void clearPartitionOwnersForGroup(String groupId) {
        partitionOwners.keySet().removeIf(key -> key.groupId().equals(groupId));
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        expirationScheduler.shutdownNow();
    }

    private static final class GroupState {
        private final String groupId;
        private final String topic;
        private int partitionCount;
        private int generation;
        private final Map<String, MemberState> members = new HashMap<>();

        private GroupState(String groupId, String topic, int partitionCount) {
            this.groupId = groupId;
            this.topic = topic;
            this.partitionCount = partitionCount;
        }
    }

    private static final class MemberState {
        private final String memberId;
        private volatile long lastHeartbeatMillis;
        private List<Integer> assignedPartitions = List.of();

        private MemberState(String memberId, long lastHeartbeatMillis, List<Integer> assignedPartitions) {
            this.memberId = memberId;
            this.lastHeartbeatMillis = lastHeartbeatMillis;
            this.assignedPartitions = assignedPartitions;
        }
    }

    private record GroupPartitionKey(String groupId, int partition) {}

    private record RebalanceResult(int generation, Map<String, List<Integer>> assignments) {}

    public record JoinGroupResult(
            int generation,
            List<Integer> assignedPartitions,
            List<Integer> revokedPartitions,
            Map<Integer, Long> committedOffsets) {}

    public record LeaveGroupResult(
            int generation, List<Integer> revokedPartitions, Map<String, List<Integer>> remainingAssignments) {}
}
