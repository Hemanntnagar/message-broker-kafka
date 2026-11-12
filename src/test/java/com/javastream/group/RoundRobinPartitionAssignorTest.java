package com.javastream.group;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RoundRobinPartitionAssignorTest {

    private final RoundRobinPartitionAssignor assignor = new RoundRobinPartitionAssignor();

    @Test
    void assignsRoundRobin() {
        Map<String, List<Integer>> assignment = assignor.assign(List.of("m1", "m2", "m3"), 6);
        assertEquals(List.of(0, 3), assignment.get("m1"));
        assertEquals(List.of(1, 4), assignment.get("m2"));
        assertEquals(List.of(2, 5), assignment.get("m3"));
    }

    @Test
    void singleMemberGetsAllPartitions() {
        Map<String, List<Integer>> assignment = assignor.assign(List.of("solo"), 3);
        assertEquals(List.of(0, 1, 2), assignment.get("solo"));
    }
}
