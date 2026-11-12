package com.javastream.security;

import com.javastream.protocol.RecordPayloadCodec;

import java.util.Map;

/** Validates produce record sizes against configured limits. */
public final class MessageSizeValidator {

    private final int maxMessageBytes;

    public MessageSizeValidator(int maxMessageBytes) {
        this.maxMessageBytes = Math.max(1, maxMessageBytes);
    }

    public boolean isWithinLimit(RecordPayloadCodec.ProduceRequest request) {
        long total = 0;
        for (RecordPayloadCodec.ProduceRecord record : request.allRecords()) {
            total += recordSize(record);
            if (total > maxMessageBytes) {
                return false;
            }
        }
        return total <= maxMessageBytes;
    }

    private static long recordSize(RecordPayloadCodec.ProduceRecord record) {
        long size = 0;
        size += safeLength(record.key());
        size += safeLength(record.value());
        Map<String, byte[]> headers = record.headers();
        if (headers != null) {
            for (var entry : headers.entrySet()) {
                size += entry.getKey().getBytes().length;
                size += safeLength(entry.getValue());
            }
        }
        return size;
    }

    private static int safeLength(byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }
}
