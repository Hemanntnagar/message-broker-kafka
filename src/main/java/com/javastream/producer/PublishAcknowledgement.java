package com.javastream.producer;

/**
 * Broker acknowledgement for a synchronously published message.
 */
public record PublishAcknowledgement(long messageId, String topic, int partition, long offset) {}
