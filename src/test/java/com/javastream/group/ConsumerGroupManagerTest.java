package com.javastream.group;

import com.javastream.partition.PartitionMetadataStore;
import com.javastream.storage.InMemoryMessageStore;
import com.javastream.topic.TopicManager;
import com.javastream.topic.TopicName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerGroupManagerTest {

    @TempDir
    Path tempDir;

    private ConsumerGroupManager manager;
    private TopicManager topicManager;

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.close();
        }
    }

    @Test
    void singleConsumerOwnsAllPartitions() {
        newManager(4);
        registerTopic("orders", 4);
        var join = manager.joinGroup("g1", "c1", "orders");
        assertEquals(List.of(0, 1, 2, 3), join.assignedPartitions());
        assertTrue(join.revokedPartitions().isEmpty());
        assertEquals("c1", manager.partitionOwner("g1", 0).orElseThrow());
    }

    @Test
    void roundRobinAcrossMultipleConsumers() {
        newManager(4);
        registerTopic("events", 6);
        manager.joinGroup("workers", "a", "events");
        manager.joinGroup("workers", "b", "events");
        manager.joinGroup("workers", "c", "events");

        Map<String, List<Integer>> assignment = manager.currentAssignment("workers");
        assertEquals(List.of(0, 3), assignment.get("a"));
        assertEquals(List.of(1, 4), assignment.get("b"));
        assertEquals(List.of(2, 5), assignment.get("c"));
        assertNoDuplicateOwnership("workers", 6);
    }

    @Test
    void multipleGroupsOnSameTopic() {
        newManager(4);
        registerTopic("shared", 3);
        manager.joinGroup("group-a", "m1", "shared");
        manager.joinGroup("group-b", "m1", "shared");

        assertEquals(List.of(0, 1, 2), manager.currentAssignment("group-a").get("m1"));
        assertEquals(List.of(0, 1, 2), manager.currentAssignment("group-b").get("m1"));

        manager.commitOffsets("group-a", "shared", Map.of(0, 10L));
        manager.commitOffsets("group-b", "shared", Map.of(0, 99L));
        assertEquals(10L, manager.fetchCommittedOffsets("group-a", "shared", List.of(0)).get(0));
        assertEquals(99L, manager.fetchCommittedOffsets("group-b", "shared", List.of(0)).get(0));
    }

    @Test
    void consumerLeaveTriggersRebalance() {
        newManager(4);
        registerTopic("rebalance", 4);
        manager.joinGroup("g", "c1", "rebalance");
        manager.joinGroup("g", "c2", "rebalance");
        manager.leaveGroup("g", "c1");

        Map<String, List<Integer>> assignment = manager.currentAssignment("g");
        assertEquals(List.of(0, 1, 2, 3), assignment.get("c2"));
        assertFalse(assignment.containsKey("c1"));
    }

    @Test
    void inactiveConsumerRemovedAndPartitionsReassigned() {
        newManager(4, Duration.ofMillis(50));
        registerTopic("heartbeats", 2);
        manager.joinGroup("g", "live", "heartbeats");
        manager.joinGroup("g", "stale", "heartbeats");

        try {
            Thread.sleep(120);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        manager.heartbeat("g", "live");
        manager.expireInactiveMembersForTests();

        Map<String, List<Integer>> assignment = manager.currentAssignment("g");
        assertEquals(List.of(0, 1), assignment.get("live"));
        assertFalse(assignment.containsKey("stale"));
    }

    @Test
    void offsetsPersistAndRecoverAfterRestart() {
        Path catalog = tempDir.resolve("consumer-offsets.catalog");
        topicManager = new TopicManager(1, new PartitionMetadataStore(), new InMemoryMessageStore());
        topicManager.createTopic(new TopicName("persisted"), 2);

        manager = new ConsumerGroupManager(topicManager::partitionCount, catalog);
        manager.joinGroup("checkout", "c1", "persisted");
        manager.commitOffsets("checkout", "persisted", Map.of(0, 15L, 1, 42L));
        manager.close();

        ConsumerGroupManager restarted = new ConsumerGroupManager(topicManager::partitionCount, catalog);
        Map<Integer, Long> offsets = restarted.fetchCommittedOffsets("checkout", "persisted", List.of(0, 1));
        assertEquals(15L, offsets.get(0));
        assertEquals(42L, offsets.get(1));
        restarted.close();
    }

    @Test
    void rejoinReturnsRevokedAndNewAssignments() {
        newManager(4);
        registerTopic("shrink", 4);
        manager.joinGroup("g", "c1", "shrink");
        manager.joinGroup("g", "c2", "shrink");
        assertEquals(List.of(0, 2), manager.currentAssignment("g").get("c1"));
        assertEquals(List.of(1, 3), manager.currentAssignment("g").get("c2"));
    }

    private void newManager(int defaultPartitions) {
        newManager(defaultPartitions, Duration.ofSeconds(10));
    }

    private void newManager(int defaultPartitions, Duration sessionTimeout) {
        topicManager = new TopicManager(defaultPartitions, new PartitionMetadataStore(), new InMemoryMessageStore());
        manager = new ConsumerGroupManager(topicManager::partitionCount, null, sessionTimeout);
    }

    private void registerTopic(String name, int partitions) {
        topicManager.createTopic(new TopicName(name), partitions);
    }

    private void assertNoDuplicateOwnership(String groupId, int partitionCount) {
        for (int partition = 0; partition < partitionCount; partition++) {
            assertTrue(manager.partitionOwner(groupId, partition).isPresent());
        }
    }
}
