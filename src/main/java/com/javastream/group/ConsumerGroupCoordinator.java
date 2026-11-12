package com.javastream.group;

import java.util.List;
import java.util.Map;

/**
 * Manages consumer group membership, heartbeats, partition assignment, and offset commits.
 */
public interface ConsumerGroupCoordinator {

    ConsumerGroupManager.JoinGroupResult joinGroup(String groupId, String memberId, String topic);

    void heartbeat(String groupId, String memberId);

    ConsumerGroupManager.LeaveGroupResult leaveGroup(String groupId, String memberId);

    void commitOffsets(String groupId, String topic, Map<Integer, Long> offsets);

    Map<Integer, Long> fetchCommittedOffsets(String groupId, String topic, List<Integer> partitions);
}
