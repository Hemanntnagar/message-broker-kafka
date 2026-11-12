package com.javastream.delivery;

import java.util.Objects;

/**
 * Consumer-side delivery and retry settings for at-least-once processing.
 */
public record ConsumerDeliverySettings(
        boolean autoCommitOffsets,
        int maxProcessingRetries,
        String deadLetterTopic,
        boolean commitAfterSuccessfulProcessing) {

    public static ConsumerDeliverySettings atLeastOnce(String deadLetterTopic) {
        return new ConsumerDeliverySettings(false, 3, deadLetterTopic, true);
    }

    public static ConsumerDeliverySettings atMostOnce() {
        return new ConsumerDeliverySettings(true, 0, null, true);
    }

    public ConsumerDeliverySettings {
        if (maxProcessingRetries < 0) {
            throw new IllegalArgumentException("maxProcessingRetries must be non-negative");
        }
        if (maxProcessingRetries > 0 && deadLetterTopic == null) {
            throw new IllegalArgumentException("deadLetterTopic is required when retries are enabled");
        }
    }

    public static String defaultDeadLetterTopic(String sourceTopic) {
        return sourceTopic + ".dlq";
    }
}
