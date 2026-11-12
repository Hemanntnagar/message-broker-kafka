package com.javastream.message;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable record delivered to consumers or returned from produce acknowledgements.
 */
public final class Message {

    private final long messageId;
    private final byte[] key;
    private final byte[] value;
    private final Map<String, byte[]> headers;
    private final long timestampEpochMillis;
    private final String topic;
    private final int partition;
    private final long offset;

    public Message(
            long messageId,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers,
            long timestampEpochMillis,
            String topic,
            int partition,
            long offset) {
        this.messageId = messageId;
        this.key = copyNullable(key);
        this.value = copyNullable(value);
        this.headers = headers == null ? Map.of() : Map.copyOf(headers);
        this.timestampEpochMillis = timestampEpochMillis;
        this.topic = Objects.requireNonNull(topic, "topic");
        this.partition = partition;
        this.offset = offset;
    }

    public long messageId() {
        return messageId;
    }

    public byte[] key() {
        return copyNullable(key);
    }

    public byte[] value() {
        return copyNullable(value);
    }

    public Map<String, byte[]> headers() {
        return Collections.unmodifiableMap(headers);
    }

    public long timestampEpochMillis() {
        return timestampEpochMillis;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    private static byte[] copyNullable(byte[] bytes) {
        return bytes == null ? null : bytes.clone();
    }
}
