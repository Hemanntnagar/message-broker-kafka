package com.javastream.topic;

import java.util.Optional;

/**
 * Catalog of topics known to this broker (persistence-backed implementation later).
 */
public interface TopicRegistry {

    void register(TopicName name, int partitionCount);

    Optional<Integer> partitionCount(TopicName name);
}
