package com.javastream.broker;

import com.javastream.partition.PartitionId;
import com.javastream.storage.InMemoryMessageStore;
import com.javastream.topic.InMemoryTopicRegistry;
import com.javastream.topic.TopicName;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single-node broker backing services (topics, partition logs, consumer commits).
 */
public final class BrokerServices {

    private final InMemoryTopicRegistry topicRegistry = new InMemoryTopicRegistry();
    private final InMemoryMessageStore messageStore = new InMemoryMessageStore();
    private final ConcurrentHashMap<PartitionId, Long> committedOffsets = new ConcurrentHashMap<>();
    private final AtomicInteger roundRobinPartition = new AtomicInteger();

    public InMemoryTopicRegistry topicRegistry() {
        return topicRegistry;
    }

    public InMemoryMessageStore messageStore() {
        return messageStore;
    }

    public void registerTopic(TopicName name, int partitionCount) {
        topicRegistry.register(name, partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            messageStore.ensurePartition(new PartitionId(name, i));
        }
    }

    public Optional<Integer> partitionCount(TopicName name) {
        return topicRegistry.partitionCount(name);
    }

    public int resolvePartition(TopicName topic, int partitionHint, byte[] key) {
        int partitions =
                topicRegistry.partitionCount(topic).orElseThrow(() -> new IllegalStateException("Unknown topic"));
        if (partitionHint >= 0 && partitionHint < partitions) {
            return partitionHint;
        }
        if (key != null && key.length > 0) {
            return Math.floorMod(hashKey(key), partitions);
        }
        return Math.floorMod(roundRobinPartition.getAndIncrement(), partitions);
    }

    public void acknowledge(TopicName topic, int partition, long offset) {
        PartitionId id = new PartitionId(topic, partition);
        committedOffsets.merge(id, offset, Math::max);
    }

    public long committedOffset(PartitionId partition) {
        return committedOffsets.getOrDefault(partition, -1L);
    }

    private static int hashKey(byte[] key) {
        int hash = 0;
        for (byte b : key) {
            hash = 31 * hash + b;
        }
        return hash;
    }
}
