package com.javastream.cluster;

import com.javastream.partition.PartitionId;
import com.javastream.topic.TopicName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderEpochFencingTest {

    @Test
    void epochIncrementsOnFailoverAssignment() {
        PartitionLeaderEpochRegistry registry = new PartitionLeaderEpochRegistry();
        PartitionId partition = new PartitionId(new TopicName("t"), 0);
        registry.apply(new PartitionReplicaAssignment(partition, 0, List.of(0, 1), 1));
        assertEquals(1, registry.epoch(partition));
        registry.apply(new PartitionReplicaAssignment(partition, 1, List.of(0, 1), 2));
        assertEquals(2, registry.epoch(partition));
    }

    @Test
    void leadershipViewRequiresCoordinatorAssignmentAndEpoch() {
        ClusterManager manager =
                new ClusterManager(0, 2, 5_000, new InMemoryClusterMembership(), true);
        ClusterPartitionLeadership leadership = new ClusterPartitionLeadership();
        leadership.attach(manager);
        PartitionId partition = new PartitionId(new TopicName("events"), 0);
        manager.register(new BrokerEndpoint(1, "127.0.0.1", 3, 4));
        manager.register(new BrokerEndpoint(2, "127.0.0.1", 5, 6));
        manager.onTopicCreated(new TopicName("events"), 1);
        var assignment = manager.assignment(partition).orElseThrow();
        leadership.epochRegistry().apply(assignment);
        assertTrue(leadership.isLocalLeader(partition, assignment.leaderId()));
        manager.voluntaryLeave(assignment.leaderId());
        var after = manager.assignment(partition).orElseThrow();
        leadership.epochRegistry().apply(after);
        assertTrue(leadership.isLocalLeader(partition, after.leaderId()));
        assertTrue(after.leaderEpoch() > assignment.leaderEpoch());
    }
}
