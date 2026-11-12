package com.javastream.partition;

import com.javastream.topic.TopicName;

import java.util.Objects;

/** A single shard within a topic (identity only; logs live in storage). */
public final class Partition {

    private final PartitionId id;
    private final int index;

    public Partition(TopicName topic, int index) {
        this.id = new PartitionId(topic, index);
        this.index = index;
    }

    public Partition(PartitionId id) {
        this.id = Objects.requireNonNull(id, "id");
        this.index = id.index();
    }

    public PartitionId id() {
        return id;
    }

    public int index() {
        return index;
    }

    public TopicName topic() {
        return id.topic();
    }
}
