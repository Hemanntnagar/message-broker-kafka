package com.javastream.storage.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Sparse offset → file-position index for one log segment. */
public final class SegmentIndex implements AutoCloseable {

    static final int ENTRY_BYTES = 16; // relativeOffset (int) + position (long) — use long for both

    private final FileChannel channel;
    private final List<IndexEntry> entries = new ArrayList<>();
    private final int indexStrideRecords;

    public record IndexEntry(long offset, long position) {}

    public SegmentIndex(Path indexFile, int indexStrideRecords, boolean create) throws IOException {
        this.indexStrideRecords = indexStrideRecords;
        if (create || !Files.exists(indexFile)) {
            channel = FileChannel.open(
                    indexFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
        } else {
            channel = FileChannel.open(
                    indexFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
        loadEntries();
    }

    private void loadEntries() throws IOException {
        entries.clear();
        long size = channel.size();
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_BYTES);
        for (long pos = 0; pos + ENTRY_BYTES <= size; pos += ENTRY_BYTES) {
            buffer.clear();
            int read = channel.read(buffer, pos);
            if (read < ENTRY_BYTES) {
                break;
            }
            buffer.flip();
            long offset = buffer.getLong();
            long position = buffer.getLong();
            entries.add(new IndexEntry(offset, position));
        }
    }

    public List<IndexEntry> entries() {
        return List.copyOf(entries);
    }

    public void maybeAppend(long offset, long logPosition, long recordsSinceBase) throws IOException {
        if (recordsSinceBase % indexStrideRecords != 0 && !entries.isEmpty()) {
            return;
        }
        if (!entries.isEmpty() && entries.getLast().offset() == offset) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_BYTES);
        buffer.putLong(offset);
        buffer.putLong(logPosition);
        buffer.flip();
        channel.position(channel.size());
        channel.write(buffer);
        entries.add(new IndexEntry(offset, logPosition));
    }

    public long lookupPosition(long targetOffset) {
        if (entries.isEmpty()) {
            return 0L;
        }
        long position = 0L;
        for (IndexEntry entry : entries) {
            if (entry.offset() > targetOffset) {
                break;
            }
            position = entry.position();
        }
        return position;
    }

    public void truncateToSize(long validIndexBytes) throws IOException {
        channel.truncate(validIndexBytes);
        long count = validIndexBytes / ENTRY_BYTES;
        while (entries.size() > count) {
            entries.removeLast();
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
