package com.javastream.storage;

import com.javastream.partition.PartitionId;
import com.javastream.storage.log.FileMessageLog;
import com.javastream.storage.log.FlushPolicy;
import com.javastream.storage.log.LogMessage;
import com.javastream.storage.log.LogStorageException;
import com.javastream.storage.log.StorageLayout;
import com.javastream.storage.log.StorageSettings;
import com.javastream.storage.log.TopicCatalog;
import com.javastream.topic.TopicName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** File-backed {@link BrokerMessageStore} using one {@link FileMessageLog} per partition. */
public final class FileMessageStore implements BrokerMessageStore, AutoCloseable {

    private final StorageSettings settings;
    private final StorageLayout layout;
    private final TopicCatalog topicCatalog;
    private final AtomicLong nextMessageId = new AtomicLong(1);
    private final ConcurrentHashMap<PartitionId, FileMessageLog> logs = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread periodicFlusher;

    public FileMessageStore(StorageSettings settings) throws IOException {
        this.settings = settings;
        this.layout = new StorageLayout(settings.brokerRoot());
        Files.createDirectories(layout.metadataDirectory());
        this.topicCatalog = new TopicCatalog(layout.topicCatalogFile());
        long maxId = discoverPartitionsAndMaxMessageId();
        nextMessageId.set(maxId + 1);
        if (settings.flushPolicy() == FlushPolicy.PERIODIC) {
            periodicFlusher =
                    Thread.ofVirtual()
                            .name("log-periodic-flush")
                            .start(this::periodicFlushLoop);
        } else {
            periodicFlusher = null;
        }
    }

    private long discoverPartitionsAndMaxMessageId() throws IOException {
        long maxMessageId = 0;
        Path topicsDir = layout.brokerRoot().resolve("topics");
        if (Files.isDirectory(topicsDir)) {
            try (var topicStream = Files.list(topicsDir)) {
                for (Path topicDir : topicStream.filter(Files::isDirectory).toList()) {
                    String topicName = topicDir.getFileName().toString();
                    TopicName topic = new TopicName(topicName);
                    try (var partStream = Files.list(topicDir)) {
                        for (Path partDir : partStream.filter(Files::isDirectory).toList()) {
                            String partName = partDir.getFileName().toString();
                            if (!partName.startsWith("partition-")) {
                                continue;
                            }
                            int index = Integer.parseInt(partName.substring("partition-".length()));
                            PartitionId id = new PartitionId(topic, index);
                            FileMessageLog log = openPartitionLog(id);
                            logs.put(id, log);
                            maxMessageId = Math.max(maxMessageId, log.maxRecoveredMessageId());
                        }
                    }
                }
            }
        }
        return maxMessageId;
    }

    public Map<String, Integer> persistedTopics() {
        return topicCatalog.load();
    }

    public Path brokerRoot() {
        return layout.brokerRoot();
    }

    public void recordTopic(TopicName name, int partitionCount) {
        topicCatalog.put(name.value(), partitionCount);
    }

    public void forgetTopic(TopicName name) {
        topicCatalog.remove(name.value());
    }

    public long logEndOffset(PartitionId partition) {
        FileMessageLog log = logs.get(partition);
        if (log == null) {
            return 0L;
        }
        return log.logEndOffset();
    }

    @Override
    public long append(PartitionId partition, byte[] key, byte[] value) {
        return appendRecord(partition, key, value, Map.of(), System.currentTimeMillis()).offset();
    }

    @Override
    public StorageRecord read(PartitionId partition, long offset) {
        List<StoredRecord> batch = fetch(partition, offset, 1);
        if (batch.isEmpty()) {
            return null;
        }
        StoredRecord record = batch.getFirst();
        return new StorageRecord(record.offset(), record.key(), record.value());
    }

    @Override
    public AppendResult appendRecord(
            PartitionId partition,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers,
            long timestampEpochMillis) {
        ensureOpen();
        FileMessageLog log = partitionLog(partition);
        long messageId = nextMessageId.getAndIncrement();
        LogMessage message = new LogMessage(-1, messageId, timestampEpochMillis, key, value, headers);
        long offset = log.append(message);
        return new AppendResult(messageId, offset);
    }

    @Override
    public List<StoredRecord> fetch(PartitionId partition, long offset, int maxRecords) {
        if (maxRecords <= 0) {
            return List.of();
        }
        FileMessageLog log = logs.get(partition);
        if (log == null) {
            return List.of();
        }
        List<LogMessage> messages = log.read(offset, maxRecords);
        List<StoredRecord> out = new ArrayList<>(messages.size());
        for (LogMessage message : messages) {
            out.add(
                    new StoredRecord(
                            message.messageId(),
                            message.offset(),
                            message.timestampEpochMillis(),
                            message.key(),
                            message.value(),
                            message.headers()));
        }
        return List.copyOf(out);
    }

    @Override
    public void ensurePartition(PartitionId partition) {
        ensureOpen();
        partitionLog(partition);
    }

    @Override
    public void removePartition(PartitionId partition) {
        FileMessageLog removed = logs.remove(partition);
        if (removed != null) {
            removed.close();
        }
        try {
            Files.deleteIfExists(layout.partitionDirectory(partition));
        } catch (IOException e) {
            throw new LogStorageException("Failed to remove partition directory " + partition, e);
        }
    }

    @Override
    public void flush() {
        for (FileMessageLog log : logs.values()) {
            log.flush();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (periodicFlusher != null) {
            periodicFlusher.interrupt();
            try {
                periodicFlusher.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        flush();
        for (FileMessageLog log : logs.values()) {
            log.close();
        }
        logs.clear();
    }

    private FileMessageLog partitionLog(PartitionId partition) {
        return logs.computeIfAbsent(partition, this::openPartitionLogUnchecked);
    }

    private FileMessageLog openPartitionLog(PartitionId partition) throws IOException {
        return new FileMessageLog(layout, settings, layout.partitionDirectory(partition));
    }

    private FileMessageLog openPartitionLogUnchecked(PartitionId partition) {
        try {
            Files.createDirectories(layout.partitionDirectory(partition));
            return openPartitionLog(partition);
        } catch (IOException e) {
            throw new LogStorageException("Failed to open partition log " + partition, e);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("FileMessageStore is closed");
        }
    }

    private void periodicFlushLoop() {
        while (!closed.get()) {
            try {
                Thread.sleep(settings.flushIntervalMillis());
                if (!closed.get()) {
                    flush();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
