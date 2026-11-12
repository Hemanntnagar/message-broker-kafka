package com.javastream.broker;

/**
 * Coordinates topic, partition, storage, and replication concerns for one broker process.
 * Full implementation arrives in later phases.
 */
public interface BrokerNode {

    void start();

    void stop();

    boolean isStarted();
}
