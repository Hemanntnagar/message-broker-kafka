package com.javastream.admin;

import com.javastream.broker.BrokerServices;
import com.javastream.cluster.BrokerEndpoint;
import com.javastream.cluster.BrokerSessionState;
import com.javastream.cluster.ClusterManager;
import com.javastream.exception.JavaStreamException;
import com.javastream.partition.PartitionId;
import com.javastream.partition.PartitionMetadata;
import com.javastream.topic.TopicMetadata;
import com.javastream.topic.TopicName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** In-process administrative operations against {@link BrokerServices}. */
public final class BrokerAdminService implements AdminService {

    private final BrokerServices services;
    private final Optional<ClusterManager> clusterManager;
    private final Instant startedAt;
    private final String listenHost;
    private final int listenPort;

    public BrokerAdminService(
            BrokerServices services,
            Optional<ClusterManager> clusterManager,
            Instant startedAt,
            String listenHost,
            int listenPort) {
        this.services = services;
        this.clusterManager = clusterManager == null ? Optional.empty() : clusterManager;
        this.startedAt = startedAt == null ? Instant.now() : startedAt;
        this.listenHost = listenHost == null ? "127.0.0.1" : listenHost;
        this.listenPort = listenPort;
    }

    @Override
    public List<String> listTopics() {
        return List.copyOf(services.listTopics());
    }

    @Override
    public TopicMetadata describeTopic(TopicName name) {
        return services
                .topicMetadata(name)
                .orElseThrow(() -> new JavaStreamException("Unknown topic: " + name.value()));
    }

    @Override
    public void createTopic(TopicName name, int partitions) {
        if (services.partitionCount(name).isPresent()) {
            throw new JavaStreamException("Topic already exists: " + name.value());
        }
        if (partitions <= 0) {
            throw new IllegalArgumentException("partition count must be positive");
        }
        services.registerTopic(name, partitions);
        services.replicationEngine().ifPresent(engine -> engine.onTopicCreatedLocally(name, partitions));
    }

    @Override
    public void deleteTopic(TopicName name) {
        if (services.partitionCount(name).isEmpty()) {
            throw new JavaStreamException("Unknown topic: " + name.value());
        }
        services.deleteTopic(name);
    }

    @Override
    public List<BrokerInfo> listBrokers() {
        List<BrokerInfo> brokers = new ArrayList<>();
        if (clusterManager.isPresent()) {
            for (BrokerEndpoint endpoint : clusterManager.get().liveBrokers()) {
                brokers.add(toBrokerInfo(endpoint, clusterManager.get()));
            }
            if (!brokers.isEmpty()) {
                return List.copyOf(brokers);
            }
        }
        brokers.add(localBrokerInfo());
        return List.copyOf(brokers);
    }

    @Override
    public GroupDescription describeGroup(String groupId) {
        return services.consumerGroupManager().describeGroup(groupId);
    }

    @Override
    public List<ConsumerLagEntry> getConsumerLag(String groupId, String topic) {
        TopicName topicName = new TopicName(topic);
        if (services.partitionCount(topicName).isEmpty()) {
            throw new JavaStreamException("Unknown topic: " + topic);
        }
        Map<Integer, Long> committed =
                services.consumerGroupManager().fetchCommittedOffsets(groupId, topic, List.of());
        TopicMetadata metadata =
                services
                        .topicMetadata(topicName)
                        .orElseThrow(() -> new JavaStreamException("Unknown topic: " + topic));
        List<ConsumerLagEntry> entries = new ArrayList<>(metadata.partitionCount());
        for (PartitionMetadata partition : metadata.partitions()) {
            long committedOffset = committed.getOrDefault(partition.partitionIndex(), -1L);
            long logEnd = partition.logEndOffset();
            long lag =
                    committedOffset < 0 ? logEnd : Math.max(0, logEnd - committedOffset);
            entries.add(
                    new ConsumerLagEntry(
                            partition.partitionIndex(), committedOffset, logEnd, lag));
        }
        return List.copyOf(entries);
    }

    @Override
    public BrokerInfo getBrokerInfo(int brokerId) {
        if (brokerId < 0 || brokerId == services.brokerId()) {
            return localBrokerInfo();
        }
        if (clusterManager.isEmpty()) {
            throw new JavaStreamException("Broker " + brokerId + " is not known in standalone mode");
        }
        BrokerEndpoint endpoint =
                clusterManager
                        .get()
                        .broker(brokerId)
                        .orElseThrow(() -> new JavaStreamException("Unknown broker: " + brokerId));
        return toBrokerInfo(endpoint, clusterManager.get());
    }

    public Object execute(AdminCommand command, Map<String, String> args) {
        return switch (command) {
            case LIST_TOPICS -> listTopics();
            case DESCRIBE_TOPIC -> describeTopic(new TopicName(require(args, "topic")));
            case CREATE_TOPIC -> {
                String topic = require(args, "topic");
                int partitions = Integer.parseInt(require(args, "partitions"));
                createTopic(new TopicName(topic), partitions);
                yield Map.of("created", topic, "partitions", partitions);
            }
            case DELETE_TOPIC -> {
                String topic = require(args, "topic");
                deleteTopic(new TopicName(topic));
                yield Map.of("deleted", topic);
            }
            case LIST_BROKERS -> listBrokers();
            case DESCRIBE_GROUP -> describeGroup(require(args, "group"));
            case GET_CONSUMER_LAG -> getConsumerLag(require(args, "group"), require(args, "topic"));
            case GET_BROKER_INFO -> getBrokerInfo(Integer.parseInt(args.getOrDefault("brokerId", "-1")));
        };
    }

    private static String require(Map<String, String> args, String key) {
        String value = args.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing argument: " + key);
        }
        return value.trim();
    }

    private BrokerInfo localBrokerInfo() {
        return new BrokerInfo(
                services.brokerId(),
                listenHost,
                listenPort,
                -1,
                clusterManager.flatMap(cm -> cm.brokerState(services.brokerId()))
                        .orElse(BrokerSessionState.AVAILABLE)
                        .name(),
                Duration.between(startedAt, Instant.now()).toSeconds());
    }

    private static BrokerInfo toBrokerInfo(BrokerEndpoint endpoint, ClusterManager clusterManager) {
        String state =
                clusterManager.brokerState(endpoint.brokerId()).orElse(BrokerSessionState.AVAILABLE).name();
        return new BrokerInfo(
                endpoint.brokerId(),
                endpoint.host(),
                endpoint.clientPort(),
                endpoint.replicationPort(),
                state,
                -1L);
    }

    public record BrokerInfo(
            int brokerId,
            String host,
            int clientPort,
            int replicationPort,
            String state,
            long uptimeSeconds) {}

    public record ConsumerLagEntry(int partition, long committedOffset, long logEndOffset, long lag) {}

    public record GroupDescription(
            String groupId, String topic, int generation, Map<String, List<Integer>> members) {}
}
