package com.javastream.admin;

import com.javastream.topic.TopicName;

/**
 * Administrative operations: create topics, describe cluster, etc.
 */
public interface AdminService {

    void createTopic(TopicName name, int partitions);

    void deleteTopic(TopicName name);
}
