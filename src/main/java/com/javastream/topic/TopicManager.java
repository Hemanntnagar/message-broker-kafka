package com.javastream.topic;

import com.javastream.common.Preconditions;
import com.javastream.exception.JavaStreamException;
import com.javastream.partition.Partition;
import com.javastream.partition.PartitionAssignment;
import com.javastream.partition.PartitionId;
import com.javastream.partition.PartitionMetadataStore;
import com.javastream.storage.InMemoryMessageStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates and deletes topics, lists catalog entries, and exposes per-partition metadata.
 */
public final class TopicManager {

    public static final int DEFAULT_PARTITION_COUNT = 1;

    private final ConcurrentHashMap<TopicName, Topic> topics = new ConcurrentHashMap<>();
    private final PartitionMetadataStore metadataStore;
    private final InMemoryMessageStore messageStore;
    private final PartitionAssignment partitionAssignment;
    private final int defaultPartitionCount;

    public TopicManager(
            int defaultPartitionCount,
            PartitionMetadataStore metadataStore,
            InMemoryMessageStore messageStore) {
        this(
                defaultPartitionCount,
                metadataStore,
                messageStore,
                new PartitionAssignment());
    }

    TopicManager(
            int defaultPartitionCount,
            PartitionMetadataStore metadataStore,
            InMemoryMessageStore messageStore,
            PartitionAssignment partitionAssignment) {
        this.defaultPartitionCount = Preconditions.requirePositive(defaultPartitionCount, "defaultPartitionCount");
        this.metadataStore = metadataStore;
        this.messageStore = messageStore;
        this.partitionAssignment = partitionAssignment;
    }

    public int defaultPartitionCount() {
        return defaultPartitionCount;
    }

    public Topic createTopic(String name, int partitionCount) {
        TopicName topicName = new TopicName(TopicNameValidator.validate(name));
        return createTopic(topicName, partitionCount);
    }

    public Topic createTopic(String name) {
        return createTopic(name, defaultPartitionCount);
    }

    public Topic createTopic(TopicName name, int partitionCount) {
        Preconditions.requirePositive(partitionCount, "partitionCount");
        List<Partition> partitions = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            Partition partition = new Partition(name, i);
            partitions.add(partition);
            PartitionId id = partition.id();
            metadataStore.register(id);
            messageStore.ensurePartition(id);
        }
        Topic topic = new Topic(name, partitionCount, partitions);
        Topic existing = topics.putIfAbsent(name, topic);
        if (existing != null) {
            for (Partition partition : partitions) {
                metadataStore.unregister(partition.id());
                messageStore.removePartition(partition.id());
            }
            throw new JavaStreamException("Topic already exists: " + name.value());
        }
        return topic;
    }

    public void deleteTopic(String name) {
        deleteTopic(new TopicName(TopicNameValidator.validate(name)));
    }

    public void deleteTopic(TopicName name) {
        Topic removed = topics.remove(name);
        if (removed == null) {
            throw new JavaStreamException("Unknown topic: " + name.value());
        }
        for (Partition partition : removed.partitions()) {
            PartitionId id = partition.id();
            metadataStore.unregister(id);
            messageStore.removePartition(id);
        }
    }

    public List<String> listTopics() {
        return topics.keySet().stream().map(TopicName::value).sorted().toList();
    }

    public Optional<TopicMetadata> getTopicMetadata(String name) {
        return getTopicMetadata(new TopicName(TopicNameValidator.validate(name)));
    }

    public Optional<TopicMetadata> getTopicMetadata(TopicName name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            return Optional.empty();
        }
        return Optional.of(
                new TopicMetadata(
                        name.value(),
                        topic.partitionCount(),
                        metadataStore.snapshotsForTopic(name, topic.partitionCount())));
    }

    public Optional<Topic> findTopic(TopicName name) {
        return Optional.ofNullable(topics.get(name));
    }

    public Optional<Integer> partitionCount(TopicName name) {
        return findTopic(name).map(Topic::partitionCount);
    }

    public int resolvePartition(TopicName topic, int partitionHint, byte[] key) {
        int partitions = partitionCount(topic)
                .orElseThrow(() -> new IllegalStateException("Unknown topic: " + topic.value()));
        return partitionAssignment.resolve(topic, partitionHint, key, partitions);
    }

    public void recordAppend(PartitionId partitionId, long assignedOffset) {
        metadataStore.onAppend(partitionId, assignedOffset);
    }

    public PartitionMetadataStore metadataStore() {
        return metadataStore;
    }
}
