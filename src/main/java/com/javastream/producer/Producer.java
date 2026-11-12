package com.javastream.producer;

import com.javastream.topic.TopicName;

/**
 * Client-side produce API (broker-facing implementation deferred).
 */
public interface Producer {

    ProduceResult send(TopicName topic, byte[] key, byte[] value);

    record ProduceResult(long offset, int partition) {
    }
}
