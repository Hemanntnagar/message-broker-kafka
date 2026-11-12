package com.javastream.storage.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** One append-only {@code segment-NNNNNN.log} file and its companion index. */
public final class LogSegment implements AutoCloseable {

    private final int baseOffset;
    private final Path logFile;
    private final FileChannel logChannel;
    private final SegmentIndex index;
    private final int indexStrideRecords;
    private final long maxSegmentBytes;

    private long recordsInSegment;
    private long logEndPosition;
    private final ByteBuffer recordHeaderScratch = ByteBuffer.allocate(LogRecordFormat.HEADER_SIZE);

    public LogSegment(
            int baseOffset,
            Path logFile,
            Path indexFile,
            int indexStrideRecords,
            long maxSegmentBytes,
            boolean create)
            throws IOException {
        this.baseOffset = baseOffset;
        this.logFile = logFile;
        this.indexStrideRecords = indexStrideRecords;
        this.maxSegmentBytes = maxSegmentBytes;
        if (create) {
            logChannel = FileChannel.open(
                    logFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            index = new SegmentIndex(indexFile, indexStrideRecords, true);
            logEndPosition = 0;
        } else {
            logChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
            index = new SegmentIndex(indexFile, indexStrideRecords, false);
            logEndPosition = logChannel.size();
        }
    }

    public int baseOffset() {
        return baseOffset;
    }

    public long bytesWritten() {
        return logEndPosition;
    }

    public boolean isFull() {
        return logEndPosition >= maxSegmentBytes;
    }

    public long recordsInSegment() {
        return recordsInSegment;
    }

    public synchronized AppendResult append(
            long offset,
            long messageId,
            long timestampEpochMillis,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers,
            boolean forceMetadata)
            throws IOException {
        int encodedSize = LogRecordFormat.encodedSize(key, value, headers);
        ByteBuffer buffer = ByteBuffer.allocate(encodedSize);
        LogRecordFormat.encode(buffer, offset, messageId, timestampEpochMillis, key, value, headers);
        buffer.flip();
        long position = logEndPosition;
        int written = logChannel.write(buffer, position);
        if (written != encodedSize) {
            throw new LogStorageException("Short write to segment " + logFile + " (wrote " + written + " of " + encodedSize + ")");
        }
        logEndPosition += encodedSize;
        recordsInSegment++;
        index.maybeAppend(offset, position, recordsInSegment);
        if (forceMetadata) {
            force(true);
        }
        return new AppendResult(position, encodedSize);
    }

    public record AppendResult(long position, int bytesWritten) {}

    public synchronized void force(boolean metadata) throws IOException {
        logChannel.force(metadata);
    }

    public synchronized List<LogMessage> readFromOffset(long startOffset, int maxMessages, long knownEndOffset)
            throws IOException {
        if (maxMessages <= 0) {
            return List.of();
        }
        long filePos = index.lookupPosition(startOffset);
        List<LogMessage> out = new ArrayList<>(maxMessages);
        long nextExpected = startOffset;
        while (out.size() < maxMessages && nextExpected < knownEndOffset) {
            recordHeaderScratch.clear();
            int read = logChannel.read(recordHeaderScratch, filePos);
            if (read < LogRecordFormat.HEADER_SIZE) {
                break;
            }
            recordHeaderScratch.flip();
            int bodySize = recordHeaderScratch.getInt();
            if (bodySize <= 0) {
                break;
            }
            int recordSize = LogRecordFormat.HEADER_SIZE + bodySize;
            ByteBuffer recordBuf = ByteBuffer.allocate(recordSize);
            recordBuf.putInt(bodySize);
            ByteBuffer body = ByteBuffer.allocate(bodySize);
            int bodyRead = logChannel.read(body, filePos + LogRecordFormat.HEADER_SIZE);
            if (bodyRead < bodySize) {
                break;
            }
            body.flip();
            recordBuf.put(body);
            recordBuf.flip();
            LogRecordFormat.DecodedRecord decoded = LogRecordFormat.decode(recordBuf, -1);
            if (decoded == null) {
                break;
            }
            if (decoded.message.offset() < startOffset) {
                filePos += decoded.recordBytes;
                continue;
            }
            if (decoded.message.offset() != nextExpected) {
                throw new LogStorageException(
                        "Offset sequence gap: expected " + nextExpected + ", found " + decoded.message.offset());
            }
            out.add(decoded.message);
            filePos += decoded.recordBytes;
            nextExpected++;
        }
        return out;
    }

    /**
     * Scans the segment sequentially without loading all messages into heap. Truncates tail corruption.
     *
     * @return recovery stats for this segment
     */
    public synchronized RecoveryResult recoverAndTruncate() throws IOException {
        long pos = 0;
        long fileSize = logChannel.size();
        long lastValidEnd = 0;
        long lastOffset = baseOffset - 1;
        long count = 0;
        long maxMessageId = -1;
        ByteBuffer header = ByteBuffer.allocate(LogRecordFormat.HEADER_SIZE);
        while (pos < fileSize) {
            header.clear();
            int read = logChannel.read(header, pos);
            if (read < LogRecordFormat.HEADER_SIZE) {
                break;
            }
            header.flip();
            int bodySize = header.getInt();
            if (bodySize <= 0 || pos + LogRecordFormat.HEADER_SIZE + bodySize > fileSize) {
                break;
            }
            int recordSize = LogRecordFormat.HEADER_SIZE + bodySize;
            ByteBuffer record = ByteBuffer.allocate(recordSize);
            record.putInt(bodySize);
            ByteBuffer body = ByteBuffer.allocate(bodySize);
            int bodyRead = logChannel.read(body, pos + LogRecordFormat.HEADER_SIZE);
            if (bodyRead < bodySize) {
                break;
            }
            body.flip();
            record.put(body);
            record.flip();
            long expected = count == 0 ? baseOffset : lastOffset + 1;
            try {
                LogRecordFormat.DecodedRecord decoded = LogRecordFormat.decode(record, expected);
                if (decoded == null) {
                    break;
                }
                lastOffset = decoded.message.offset();
                maxMessageId = Math.max(maxMessageId, decoded.message.messageId());
                lastValidEnd = pos + recordSize;
                pos += recordSize;
                count++;
            } catch (LogStorageException e) {
                break;
            }
        }
        if (lastValidEnd < fileSize) {
            logChannel.truncate(lastValidEnd);
        }
        if (lastValidEnd < fileSize) {
            rebuildIndex(lastValidEnd);
        }
        logEndPosition = lastValidEnd;
        recordsInSegment = count;
        return new RecoveryResult(count, lastOffset, maxMessageId, lastValidEnd);
    }

    private void rebuildIndex(long validLength) throws IOException {
        index.truncateToSize(0);
        if (validLength == 0) {
            return;
        }
        long pos = 0;
        long recordIndex = 0;
        long offset = baseOffset;
        ByteBuffer header = ByteBuffer.allocate(LogRecordFormat.HEADER_SIZE);
        while (pos < validLength) {
            header.clear();
            logChannel.read(header, pos);
            header.flip();
            int bodySize = header.getInt();
            int recordSize = LogRecordFormat.HEADER_SIZE + bodySize;
            recordIndex++;
            if (recordIndex == 1 || recordIndex % indexStrideRecords == 0) {
                index.maybeAppend(offset, pos, recordIndex);
            }
            pos += recordSize;
            offset++;
        }
    }

    public record RecoveryResult(long recordCount, long lastOffset, long maxMessageId, long validByteLength) {
        public boolean hasRecords() {
            return recordCount > 0;
        }
    }

    @Override
    public void close() throws IOException {
        index.close();
        logChannel.close();
    }

    public Path logFile() {
        return logFile;
    }
}
