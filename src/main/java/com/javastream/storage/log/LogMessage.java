package com.javastream.storage.log;

import java.util.Map;

/** One append-only log record (logical message). */
public record LogMessage(
        long offset,
        long messageId,
        long timestampEpochMillis,
        byte[] key,
        byte[] value,
        Map<String, byte[]> headers) {

    public LogMessage {
        key = key == null ? new byte[0] : key;
        value = value == null ? new byte[0] : value;
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
