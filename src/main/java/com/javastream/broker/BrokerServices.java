package com.javastream.broker;

import com.javastream.partition.PartitionId;
import com.javastream.partition.PartitionMetadataStore;
import com.javastream.config.JavaStreamConfig;
import com.javastream.config.ConfigLoader;
import com.javastream.delivery.ProduceAcknowledgementService;
import com.javastream.delivery.ProducerIdempotencyCache;
import com.javastream.group.ConsumerGroupManager;
import com.javastream.cluster.ClusterPartitionLeadership;
import com.javastream.replication.ClusterReplicaManager;
import com.javastream.replication.ReplicaManager;
import com.javastream.replication.ReplicationEngine;
import com.javastream.replication.SingleBrokerReplicaManager;
import com.javastream.storage.BrokerMessageStore;
import com.javastream.storage.FileMessageStore;
import com.javastream.storage.InMemoryMessageStore;
import com.javastream.storage.log.StorageLayout;
import com.javastream.storage.log.StorageSettings;
import com.javastream.topic.TopicManager;
import com.javastream.topic.TopicMetadata;
import com.javastream.topic.TopicName;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-node broker backing services (topics, partition logs, consumer commits).
 */
public final class BrokerServices {

    private final TopicManager topicManager;
    private final BrokerMessageStore messageStore;
    private final ConsumerGroupManager consumerGroupManager;
    private final ProducerIdempotencyCache producerIdempotencyCache = new ProducerIdempotencyCache();
    private final ProduceAcknowledgementService produceAcknowledgementService;
    private final ReplicaManager replicaManager;
    private final ClusterPartitionLeadership leadership;
    private ReplicationEngine replicationEngine;
    private final int brokerId;
    private final ConcurrentHashMap<PartitionId, Long> committedOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionId, Object> partitionLocks = new ConcurrentHashMap<>();

    public BrokerServices() {
        this(TopicManager.DEFAULT_PARTITION_COUNT);
    }

    public BrokerServices(int defaultPartitionCount) {
        this(new InMemoryMessageStore(), defaultPartitionCount);
    }

    public BrokerServices(JavaStreamConfig config) {
        this(openMessageStore(config), config.defaultTopicPartitionCount());
    }

    public static BrokerServices create(JavaStreamConfig config) {
        return new BrokerServices(config);
    }

    private static BrokerMessageStore openMessageStore(JavaStreamConfig config) {
        try {
            var props = ConfigLoader.loadPropertiesForStorage();
            StorageSettings storageSettings = StorageSettings.from(config, props);
            return new FileMessageStore(storageSettings);
        } catch (Exception e) {
            throw new com.javastream.exception.ConfigurationException(
                    "Failed to open durable message store at " + config.dataDirectory(), e);
        }
    }

    public BrokerServices(BrokerMessageStore messageStore, int defaultPartitionCount) {
        this(messageStore, defaultPartitionCount, 0);
    }

    public BrokerServices(BrokerMessageStore messageStore, int defaultPartitionCount, int brokerId) {
        this(
                messageStore,
                defaultPartitionCount,
                brokerId,
                new SingleBrokerReplicaManager(brokerId),
                null,
                null);
    }

    public BrokerServices(
            BrokerMessageStore messageStore,
            int defaultPartitionCount,
            int brokerId,
            ReplicaManager replicaManager,
            ClusterPartitionLeadership leadership,
            ReplicationEngine replicationEngine) {
        this.messageStore = messageStore;
        this.brokerId = brokerId;
        this.replicaManager = replicaManager;
        this.leadership = leadership;
        this.replicationEngine = replicationEngine;
        this.topicManager =
                new TopicManager(defaultPartitionCount, new PartitionMetadataStore(), messageStore);
        this.consumerGroupManager =
                new ConsumerGroupManager(this::partitionCount, consumerOffsetCatalogPath(messageStore));
        this.produceAcknowledgementService =
                new ProduceAcknowledgementService(replicaManager, brokerId);
        if (messageStore instanceof FileMessageStore fileStore) {
            restoreTopicsFromCatalog(fileStore);
        }
    }

    private void restoreTopicsFromCatalog(FileMessageStore fileStore) {
        for (var entry : fileStore.persistedTopics().entrySet()) {
            TopicName topic = new TopicName(entry.getKey());
            int partitions = entry.getValue();
            try {
                registerTopic(topic, partitions);
            } catch (com.javastream.exception.JavaStreamException ignored) {
                // already registered from concurrent layout discovery
            }
            for (int p = 0; p < partitions; p++) {
                PartitionId id = new PartitionId(topic, p);
                long end = fileStore.logEndOffset(id);
                topicManager.metadataStore().recover(id, end);
            }
        }
    }

    public void close() {
        consumerGroupManager.close();
        messageStore.close();
    }

    BrokerServices(TopicManager topicManager, BrokerMessageStore messageStore) {
        this(topicManager, messageStore, new ConsumerGroupManager(topicManager::partitionCount, null), 0);
    }

    BrokerServices(
            TopicManager topicManager, BrokerMessageStore messageStore, ConsumerGroupManager consumerGroupManager) {
        this(topicManager, messageStore, consumerGroupManager, 0);
    }

    BrokerServices(
            TopicManager topicManager,
            BrokerMessageStore messageStore,
            ConsumerGroupManager consumerGroupManager,
            int brokerId) {
        this.topicManager = topicManager;
        this.messageStore = messageStore;
        this.consumerGroupManager = consumerGroupManager;
        this.brokerId = brokerId;
        this.replicaManager = new SingleBrokerReplicaManager(brokerId);
        this.leadership = null;
        this.replicationEngine = null;
        this.produceAcknowledgementService =
                new ProduceAcknowledgementService(replicaManager, brokerId);
    }

    public void attachReplicationEngine(ReplicationEngine replicationEngine) {
        this.replicationEngine = replicationEngine;
    }

    public int brokerId() {
        return brokerId;
    }

    public Optional<ClusterPartitionLeadership> leadership() {
        return Optional.ofNullable(leadership);
    }

    public Optional<ReplicationEngine> replicationEngine() {
        return Optional.ofNullable(replicationEngine);
    }

    public Optional<ClusterReplicaManager> clusterReplicaManager() {
        if (replicaManager instanceof ClusterReplicaManager cluster) {
            return Optional.of(cluster);
        }
        return Optional.empty();
    }

    private static Path consumerOffsetCatalogPath(BrokerMessageStore messageStore) {
        if (messageStore instanceof FileMessageStore fileStore) {
            StorageLayout layout = new StorageLayout(fileStore.brokerRoot());
            return layout.consumerOffsetsFile();
        }
        return null;
    }

    public TopicManager topicManager() {
        return topicManager;
    }

    public BrokerMessageStore messageStore() {
        return messageStore;
    }

    public ConsumerGroupManager consumerGroupManager() {
        return consumerGroupManager;
    }

    public ProducerIdempotencyCache producerIdempotencyCache() {
        return producerIdempotencyCache;
    }

    public ProduceAcknowledgementService produceAcknowledgementService() {
        return produceAcknowledgementService;
    }

    public void registerTopic(TopicName name, int partitionCount) {
        topicManager.createTopic(name, partitionCount);
        if (messageStore instanceof FileMessageStore fileStore) {
            fileStore.recordTopic(name, partitionCount);
        }
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
        if (messageStore instanceof FileMessageStore fileStore) {
            fileStore.forgetTopic(name);
        }
    }

    public void acknowledge(TopicName topic, int partition, long offset) {
        PartitionId id = new PartitionId(topic, partition);
        committedOffsets.merge(id, offset, Math::max);
    }

    public long committedOffset(PartitionId partition) {
        return committedOffsets.getOrDefault(partition, -1L);
    }

    public BrokerMessageStore.AppendResult appendRecord(
            PartitionId partitionId,
            byte[] key,
            byte[] value,
            java.util.Map<String, byte[]> headers,
            long timestampEpochMillis) {
        synchronized (partitionLock(partitionId)) {
            BrokerMessageStore.AppendResult result =
                    messageStore.appendRecord(partitionId, key, value, headers, timestampEpochMillis);
            topicManager.recordAppend(partitionId, result.offset());
            return result;
        }
    }

    private Object partitionLock(PartitionId partitionId) {
        return partitionLocks.computeIfAbsent(partitionId, ignored -> new Object());
    }
}
