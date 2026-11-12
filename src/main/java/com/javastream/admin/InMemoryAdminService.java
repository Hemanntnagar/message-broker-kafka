package com.javastream.admin;

import com.javastream.topic.TopicManager;
import com.javastream.topic.TopicName;

/** Delegates administrative actions to {@link TopicManager}. */
public final class InMemoryAdminService implements AdminService {

    private final TopicManager topicManager;

    public InMemoryAdminService(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public void createTopic(TopicName name, int partitions) {
        topicManager.createTopic(name, partitions);
    }

    @Override
    public void deleteTopic(TopicName name) {
        topicManager.deleteTopic(name);
    }
}
