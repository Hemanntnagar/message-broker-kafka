package com.javastream.topic;

import com.javastream.partition.PartitionMetadata;

import java.util.List;

/** Metadata returned for a topic (partitions and their independent offset sequences). */
public record TopicMetadata(String topicName, int partitionCount, List<PartitionMetadata> partitions) {

    public TopicMetadata {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        partitions = List.copyOf(partitions);
        if (partitions.size() != partitionCount) {
            throw new IllegalArgumentException("partition metadata size must match partitionCount");
        }
    }
}
