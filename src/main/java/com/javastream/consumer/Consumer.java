package com.javastream.consumer;

import com.javastream.topic.TopicName;

import java.util.List;

/**
 * Client-side consume API (broker-facing implementation deferred).
 */
public interface Consumer {

    List<ConsumedRecord> poll(TopicName topic, int maxRecords);

    record ConsumedRecord(long offset, int partition, byte[] key, byte[] value) {
    }
}
