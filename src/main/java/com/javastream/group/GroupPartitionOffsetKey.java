package com.javastream.group;

import java.util.Objects;

/** Composite key for a consumer group's committed offset on a topic partition. */
public record GroupPartitionOffsetKey(String groupId, String topic, int partition) {

    public GroupPartitionOffsetKey {
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(topic, "topic");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
    }
}
