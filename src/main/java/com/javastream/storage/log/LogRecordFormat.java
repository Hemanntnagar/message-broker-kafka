package com.javastream.storage.log;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** On-disk record layout with trailing CRC32C. */
final class LogRecordFormat {

    static final int MAGIC = 0x4A534D52; // "JSMR"
    static final int HEADER_SIZE = 4; // leading size field

    private LogRecordFormat() {}

    static int encodedSize(byte[] key, byte[] value, Map<String, byte[]> headers) {
        int size = 4 + 8 + 8 + 8; // magic, offset, messageId, timestamp
        size += 4 + key.length + 4 + value.length + 4;
        for (var entry : headers.entrySet()) {
            byte[] name = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] val = entry.getValue() == null ? new byte[0] : entry.getValue();
            size += 4 + name.length + 4 + val.length;
        }
        size += 4; // crc
        return HEADER_SIZE + size;
    }

    static void encode(
            ByteBuffer buffer,
            long offset,
            long messageId,
            long timestampEpochMillis,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers) {
        int bodyStart = buffer.position() + HEADER_SIZE;
        int sizeFieldPos = buffer.position();
        buffer.putInt(0); // placeholder
        buffer.putInt(MAGIC);
        buffer.putLong(offset);
        buffer.putLong(messageId);
        buffer.putLong(timestampEpochMillis);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.putInt(headers.size());
        for (var entry : headers.entrySet()) {
            byte[] name = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] val = entry.getValue() == null ? new byte[0] : entry.getValue();
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(val.length);
            buffer.put(val);
        }
        int crcStart = bodyStart;
        int crcLength = buffer.position() - crcStart;
        int crc = Crc32c.checksum(buffer, crcStart, crcLength);
        buffer.putInt(crc);
        int totalBody = buffer.position() - sizeFieldPos - HEADER_SIZE;
        buffer.putInt(sizeFieldPos, totalBody);
    }

    static final class DecodedRecord {
        final LogMessage message;
        final int recordBytes;

        DecodedRecord(LogMessage message, int recordBytes) {
            this.message = message;
            this.recordBytes = recordBytes;
        }
    }

    static DecodedRecord decode(ByteBuffer buffer, long expectedOffset) {
        if (buffer.remaining() < HEADER_SIZE + 4 + 4) {
            return null;
        }
        int start = buffer.position();
        int bodySize = buffer.getInt();
        if (bodySize <= 0 || buffer.remaining() < bodySize) {
            buffer.position(start);
            return null;
        }
        int recordEnd = buffer.position() + bodySize;
        if (buffer.capacity() < recordEnd) {
            buffer.position(start);
            return null;
        }
        int magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new LogStorageException("Invalid record magic at offset " + start);
        }
        long offset = buffer.getLong();
        long messageId = buffer.getLong();
        long timestamp = buffer.getLong();
        if (expectedOffset >= 0 && offset != expectedOffset) {
            throw new LogStorageException(
                    "Offset sequence gap: expected " + expectedOffset + ", found " + offset);
        }
        // expectedOffset < 0 skips monotonic validation (used when scanning to a start offset)
        byte[] key = readBytes(buffer);
        byte[] value = readBytes(buffer);
        int headerCount = buffer.getInt();
        Map<String, byte[]> headers = new LinkedHashMap<>();
        for (int i = 0; i < headerCount; i++) {
            byte[] name = readBytes(buffer);
            byte[] val = readBytes(buffer);
            headers.put(new String(name, StandardCharsets.UTF_8), val);
        }
        int crcPos = buffer.position();
        int crcPayloadLength = crcPos - (start + HEADER_SIZE);
        int expectedCrc = Crc32c.checksum(buffer, start + HEADER_SIZE, crcPayloadLength);
        int actualCrc = buffer.getInt();
        if (actualCrc != expectedCrc) {
            throw new LogStorageException(
                    "Checksum mismatch at log position " + start + " (offset " + offset + ")");
        }
        if (buffer.position() != recordEnd) {
            throw new LogStorageException("Record size mismatch at position " + start);
        }
        int recordBytes = HEADER_SIZE + bodySize;
        return new DecodedRecord(
                new LogMessage(offset, messageId, timestamp, key, value, headers), recordBytes);
    }

    private static byte[] readBytes(ByteBuffer buffer) {
        int len = buffer.getInt();
        if (len < 0 || buffer.remaining() < len) {
            throw new LogStorageException("Corrupt length field in log record");
        }
        byte[] out = new byte[len];
        buffer.get(out);
        return out;
    }
}
