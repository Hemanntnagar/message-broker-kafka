package com.javastream.admin;

import com.javastream.topic.TopicMetadata;
import com.javastream.topic.TopicName;

import java.util.List;

/**
 * Administrative operations: create topics, describe cluster, etc.
 */
public interface AdminService {

    List<String> listTopics();

    TopicMetadata describeTopic(TopicName name);

    void createTopic(TopicName name, int partitions);

    void deleteTopic(TopicName name);

    List<BrokerAdminService.BrokerInfo> listBrokers();

    BrokerAdminService.GroupDescription describeGroup(String groupId);

    List<BrokerAdminService.ConsumerLagEntry> getConsumerLag(String groupId, String topic);

    BrokerAdminService.BrokerInfo getBrokerInfo(int brokerId);
}
