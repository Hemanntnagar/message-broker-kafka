package com.javastream.storage.log;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

final class Crc32c {

    private Crc32c() {}

    static int checksum(ByteBuffer buffer, int start, int length) {
        CRC32C crc = new CRC32C();
        int limit = buffer.limit();
        buffer.limit(start + length);
        buffer.position(start);
        crc.update(buffer);
        buffer.limit(limit);
        return (int) crc.getValue();
    }

    static int checksum(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }
}
