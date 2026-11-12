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
    private final ByteBuffer indexWriteBuffer = ByteBuffer.allocate(ENTRY_BYTES);

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
        indexWriteBuffer.clear();
        indexWriteBuffer.putLong(offset);
        indexWriteBuffer.putLong(logPosition);
        indexWriteBuffer.flip();
        channel.position(channel.size());
        channel.write(indexWriteBuffer);
        entries.add(new IndexEntry(offset, logPosition));
    }

    public long lookupPosition(long targetOffset) {
        if (entries.isEmpty()) {
            return 0L;
        }
        int lo = 0;
        int hi = entries.size() - 1;
        int best = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long offset = entries.get(mid).offset();
            if (offset <= targetOffset) {
                best = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return best < 0 ? 0L : entries.get(best).position();
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
