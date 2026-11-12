package com.javastream.topic;

import com.javastream.partition.Partition;

import java.util.List;
import java.util.Objects;

/** In-memory representation of a topic and its partitions. */
public final class Topic {

    private final TopicName name;
    private final int partitionCount;
    private final List<Partition> partitions;

    public Topic(TopicName name, int partitionCount, List<Partition> partitions) {
        this.name = Objects.requireNonNull(name, "name");
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        this.partitionCount = partitionCount;
        this.partitions = List.copyOf(Objects.requireNonNull(partitions, "partitions"));
        if (partitions.size() != partitionCount) {
            throw new IllegalArgumentException("partition list size must match partitionCount");
        }
    }

    public TopicName name() {
        return name;
    }

    public int partitionCount() {
        return partitionCount;
    }

    public List<Partition> partitions() {
        return partitions;
    }
}
