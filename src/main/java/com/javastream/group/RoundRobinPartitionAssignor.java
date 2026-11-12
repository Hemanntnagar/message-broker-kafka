package com.javastream.group;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Assigns topic partitions to group members in stable round-robin order. */
public final class RoundRobinPartitionAssignor {

    public Map<String, List<Integer>> assign(List<String> sortedMemberIds, int partitionCount) {
        if (partitionCount <= 0) {
            return Map.of();
        }
        if (sortedMemberIds.isEmpty()) {
            return Map.of();
        }
        List<String> members = List.copyOf(sortedMemberIds);
        Map<String, List<Integer>> assignment = new LinkedHashMap<>();
        for (String member : members) {
            assignment.put(member, new ArrayList<>());
        }
        for (int partition = 0; partition < partitionCount; partition++) {
            String owner = members.get(partition % members.size());
            assignment.get(owner).add(partition);
        }
        for (String member : members) {
            assignment.put(member, List.copyOf(assignment.get(member)));
        }
        return Collections.unmodifiableMap(assignment);
    }
}
