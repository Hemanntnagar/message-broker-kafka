package com.javastream.broker;

import com.javastream.partition.PartitionId;
import com.javastream.partition.PartitionMetadataStore;
import com.javastream.storage.InMemoryMessageStore;
import com.javastream.topic.TopicManager;
import com.javastream.topic.TopicMetadata;
import com.javastream.topic.TopicName;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-node broker backing services (topics, partition logs, consumer commits).
 */
public final class BrokerServices {

    private final TopicManager topicManager;
    private final InMemoryMessageStore messageStore;
    private final ConcurrentHashMap<PartitionId, Long> committedOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionId, Object> partitionLocks = new ConcurrentHashMap<>();

    public BrokerServices() {
        this(TopicManager.DEFAULT_PARTITION_COUNT);
    }

    public BrokerServices(int defaultPartitionCount) {
        this.messageStore = new InMemoryMessageStore();
        this.topicManager =
                new TopicManager(defaultPartitionCount, new PartitionMetadataStore(), messageStore);
    }

    BrokerServices(TopicManager topicManager, InMemoryMessageStore messageStore) {
        this.topicManager = topicManager;
        this.messageStore = messageStore;
    }

    public TopicManager topicManager() {
        return topicManager;
    }

    public InMemoryMessageStore messageStore() {
        return messageStore;
    }

    public void registerTopic(TopicName name, int partitionCount) {
        topicManager.createTopic(name, partitionCount);
    }

    public Optional<Integer> partitionCount(TopicName name) {
        return topicManager.partitionCount(name);
    }

    public int resolvePartition(TopicName topic, int partitionHint, byte[] key) {
        return topicManager.resolvePartition(topic, partitionHint, key);
    }

    public List<String> listTopics() {
        return topicManager.listTopics();
    }

    public Optional<TopicMetadata> topicMetadata(TopicName name) {
        return topicManager.getTopicMetadata(name);
    }

    public void deleteTopic(TopicName name) {
        topicManager.deleteTopic(name);
    }

    public void acknowledge(TopicName topic, int partition, long offset) {
        PartitionId id = new PartitionId(topic, partition);
        committedOffsets.merge(id, offset, Math::max);
    }

    public long committedOffset(PartitionId partition) {
        return committedOffsets.getOrDefault(partition, -1L);
    }

    public InMemoryMessageStore.AppendResult appendRecord(
            PartitionId partitionId,
            byte[] key,
            byte[] value,
            java.util.Map<String, byte[]> headers,
            long timestampEpochMillis) {
        synchronized (partitionLock(partitionId)) {
            InMemoryMessageStore.AppendResult result =
                    messageStore.appendRecord(partitionId, key, value, headers, timestampEpochMillis);
            topicManager.recordAppend(partitionId, result.offset());
            return result;
        }
    }

    private Object partitionLock(PartitionId partitionId) {
        return partitionLocks.computeIfAbsent(partitionId, ignored -> new Object());
    }
}
