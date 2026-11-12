package com.javastream.partition;

import com.javastream.topic.TopicName;

/**
 * Identifies a partition within a topic.
 */
public record PartitionId(TopicName topic, int index) {

    public PartitionId {
        if (index < 0) {
            throw new IllegalArgumentException("partition index must be non-negative");
        }
    }
}
