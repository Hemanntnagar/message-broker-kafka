package com.javastream.topic;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory topic catalog for single-broker Phase 3. */
public final class InMemoryTopicRegistry implements TopicRegistry {

    private final ConcurrentHashMap<TopicName, Integer> topics = new ConcurrentHashMap<>();

    @Override
    public void register(TopicName name, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        topics.put(name, partitionCount);
    }

    @Override
    public Optional<Integer> partitionCount(TopicName name) {
        return Optional.ofNullable(topics.get(name));
    }

    public Map<TopicName, Integer> snapshot() {
        return Map.copyOf(topics);
    }
}
