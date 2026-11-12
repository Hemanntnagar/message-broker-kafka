package com.javastream.admin;

import com.javastream.topic.TopicManager;
import com.javastream.topic.TopicName;

import java.util.List;
import java.util.Map;

/** Delegates administrative actions to {@link TopicManager}. */
public final class InMemoryAdminService implements AdminService {

    private final TopicManager topicManager;

    public InMemoryAdminService(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public List<String> listTopics() {
        return topicManager.listTopics();
    }

    @Override
    public com.javastream.topic.TopicMetadata describeTopic(TopicName name) {
        return topicManager
                .getTopicMetadata(name)
                .orElseThrow(() -> new com.javastream.exception.JavaStreamException("Unknown topic: " + name.value()));
    }

    @Override
    public void createTopic(TopicName name, int partitions) {
        topicManager.createTopic(name, partitions);
    }

    @Override
    public void deleteTopic(TopicName name) {
        topicManager.deleteTopic(name);
    }

    @Override
    public List<BrokerAdminService.BrokerInfo> listBrokers() {
        return List.of(new BrokerAdminService.BrokerInfo(0, "127.0.0.1", 9092, -1, "AVAILABLE", -1));
    }

    @Override
    public BrokerAdminService.GroupDescription describeGroup(String groupId) {
        return new BrokerAdminService.GroupDescription(groupId, "", 0, Map.of());
    }

    @Override
    public List<BrokerAdminService.ConsumerLagEntry> getConsumerLag(String groupId, String topic) {
        return List.of();
    }

    @Override
    public BrokerAdminService.BrokerInfo getBrokerInfo(int brokerId) {
        return new BrokerAdminService.BrokerInfo(brokerId, "127.0.0.1", 9092, -1, "AVAILABLE", -1);
    }
}
