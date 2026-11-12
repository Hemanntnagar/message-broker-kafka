package com.javastream.group;

/**
 * Manages consumer group membership, heartbeats, and partition assignment.
 */
public interface ConsumerGroupCoordinator {

    void joinGroup(String groupId, String memberId);

    void leaveGroup(String groupId, String memberId);
}
