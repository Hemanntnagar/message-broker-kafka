package com.javastream.storage.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Durable {@link MessageLog} backed by rolling {@link LogSegment} files under one partition directory.
 */
public final class FileMessageLog implements MessageLog {

    private static final Pattern SEGMENT_NAME = Pattern.compile("segment-(\\d+)\\.log");

    private final Path partitionDirectory;
    private final StorageLayout layout;
    private final StorageSettings settings;
    private final OffsetManager offsetManager = new OffsetManager();
    private final List<LogSegment> segments = new ArrayList<>();
    private LogSegment active;

    public FileMessageLog(StorageLayout layout, StorageSettings settings, Path partitionDirectory)
            throws IOException {
        this.layout = layout;
        this.settings = settings;
        this.partitionDirectory = partitionDirectory;
        Files.createDirectories(partitionDirectory);
        recover();
    }

    @Override
    public synchronized long append(LogMessage message) {
        try {
            long offset = offsetManager.assignAndIncrement();
            ensureActiveSegment(offset);
            boolean sync = settings.flushPolicy() == FlushPolicy.SYNC;
            active.append(
                    offset,
                    message.messageId(),
                    message.timestampEpochMillis(),
                    message.key(),
                    message.value(),
                    message.headers(),
                    sync);
            if (active.isFull()) {
                rollSegment(offset + 1);
            }
            return offset;
        } catch (IOException e) {
            offsetManager.undoLastAssign();
            throw new LogStorageException("Failed to append to partition log at " + partitionDirectory, e);
        }
    }

    private void ensureActiveSegment(long offset) throws IOException {
        if (active != null && !active.isFull()) {
            return;
        }
        if (active == null && !segments.isEmpty()) {
            active = segments.getLast();
            if (!active.isFull()) {
                return;
            }
        }
        if (active != null && active.isFull()) {
            rollSegment(offset);
            return;
        }
        int base = (int) offset;
        Path logFile = layout.segmentLogFile(partitionDirectory, base);
        Path indexFile = layout.segmentIndexFile(partitionDirectory, base);
        active = new LogSegment(
                base,
                logFile,
                indexFile,
                settings.indexStrideRecords(),
                settings.segmentMaxBytes(),
                true);
        segments.add(active);
    }

    private void rollSegment(long nextBaseOffset) throws IOException {
        if (active != null) {
            active.force(true);
        }
        int base = (int) nextBaseOffset;
        Path logFile = layout.segmentLogFile(partitionDirectory, base);
        Path indexFile = layout.segmentIndexFile(partitionDirectory, base);
        active = new LogSegment(
                base,
                logFile,
                indexFile,
                settings.indexStrideRecords(),
                settings.segmentMaxBytes(),
                true);
        segments.add(active);
    }

    @Override
    public synchronized List<LogMessage> read(long offset, int maxMessages) {
        if (maxMessages <= 0 || offset >= offsetManager.peekNext()) {
            return List.of();
        }
        try {
            List<LogMessage> result = new ArrayList<>();
            long cursor = offset;
            int remaining = maxMessages;
            long end = offsetManager.peekNext();
            while (remaining > 0 && cursor < end) {
                LogSegment segment = segmentForOffset(cursor);
                if (segment == null) {
                    break;
                }
                List<LogMessage> batch = segment.readFromOffset(cursor, remaining, end);
                if (batch.isEmpty()) {
                    break;
                }
                for (LogMessage msg : batch) {
                    result.add(msg);
                    cursor = msg.offset() + 1;
                    remaining--;
                }
            }
            return List.copyOf(result);
        } catch (IOException e) {
            throw new LogStorageException("Failed to read partition log at " + partitionDirectory, e);
        }
    }

    private LogSegment segmentForOffset(long offset) {
        LogSegment match = null;
        for (LogSegment segment : segments) {
            if (segment.baseOffset() <= offset) {
                match = segment;
            }
        }
        return match;
    }

    @Override
    public long logEndOffset() {
        return offsetManager.peekNext();
    }

    @Override
    public long recordCount() {
        return offsetManager.recordCount();
    }

    @Override
    public synchronized void flush() {
        try {
            if (active != null) {
                active.force(false);
            }
        } catch (IOException e) {
            throw new LogStorageException("Flush failed for " + partitionDirectory, e);
        }
    }

    @Override
    public synchronized void close() {
        IOException suppressed = null;
        for (LogSegment segment : segments) {
            try {
                segment.close();
            } catch (IOException e) {
                if (suppressed == null) {
                    suppressed = e;
                } else {
                    suppressed.addSuppressed(e);
                }
            }
        }
        if (suppressed != null) {
            throw new LogStorageException("Error closing partition log", suppressed);
        }
    }

    public long maxRecoveredMessageId() {
        return maxMessageId;
    }

    private long maxMessageId = -1;

    private void recover() throws IOException {
        List<Path> logFiles = listSegmentLogs();
        long nextOffset = 0;
        maxMessageId = -1;
        for (Path logFile : logFiles) {
            int base = parseBaseOffset(logFile);
            Path indexFile = layout.segmentIndexFile(partitionDirectory, base);
            LogSegment segment = new LogSegment(
                    base,
                    logFile,
                    indexFile,
                    settings.indexStrideRecords(),
                    settings.segmentMaxBytes(),
                    false);
            LogSegment.RecoveryResult recovery = segment.recoverAndTruncate();
            segments.add(segment);
            if (recovery.hasRecords()) {
                nextOffset = recovery.lastOffset() + 1;
                maxMessageId = Math.max(maxMessageId, recovery.maxMessageId());
            } else if (base > 0) {
                nextOffset = Math.max(nextOffset, base);
            }
        }
        offsetManager.resetFromRecovery(nextOffset, nextOffset);
        dropEmptyTrailingSegments();
        if (!segments.isEmpty()) {
            active = segments.getLast();
            if (active.isFull()) {
                active = null;
            }
        }
    }

    private void dropEmptyTrailingSegments() throws IOException {
        while (!segments.isEmpty()) {
            LogSegment last = segments.getLast();
            if (last.recordsInSegment() > 0) {
                break;
            }
            segments.removeLast();
            Path logPath = last.logFile();
            int base = parseBaseOffset(logPath);
            last.close();
            Files.deleteIfExists(logPath);
            Files.deleteIfExists(layout.segmentIndexFile(partitionDirectory, base));
        }
    }

    private List<Path> listSegmentLogs() throws IOException {
        if (!Files.isDirectory(partitionDirectory)) {
            return List.of();
        }
        try (var stream = Files.list(partitionDirectory)) {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .sorted(Comparator.comparingInt(p -> parseBaseOffset(p)))
                    .toList();
        }
    }

    static int parseBaseOffset(Path logFile) {
        Matcher matcher = SEGMENT_NAME.matcher(logFile.getFileName().toString());
        if (!matcher.matches()) {
            throw new LogStorageException("Unexpected segment file name: " + logFile);
        }
        return Integer.parseInt(matcher.group(1));
    }
}
