package com.javastream.storage;

import com.javastream.partition.PartitionId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe in-memory append log per partition (Phase 3 broker backing store).
 */
public final class InMemoryMessageStore implements MessageStore {

    private final AtomicLong nextMessageId = new AtomicLong(1);
    private final ConcurrentHashMap<PartitionId, PartitionLog> logs = new ConcurrentHashMap<>();

    @Override
    public long append(PartitionId partition, byte[] key, byte[] value) {
        AppendResult result = appendRecord(partition, key, value, Map.of(), System.currentTimeMillis());
        return result.offset();
    }

    @Override
    public StorageRecord read(PartitionId partition, long offset) {
        PartitionLog log = logs.get(partition);
        if (log == null) {
            return null;
        }
        StoredRecord record = log.read(offset);
        if (record == null) {
            return null;
        }
        return new StorageRecord(record.offset(), record.key(), record.value());
    }

    public AppendResult appendRecord(
            PartitionId partition, byte[] key, byte[] value, Map<String, byte[]> headers, long timestampEpochMillis) {
        PartitionLog log = logs.computeIfAbsent(partition, ignored -> new PartitionLog());
        long messageId = nextMessageId.getAndIncrement();
        return log.append(messageId, key, value, headers, timestampEpochMillis);
    }

    public List<StoredRecord> fetch(PartitionId partition, long offset, int maxRecords) {
        PartitionLog log = logs.get(partition);
        if (log == null || maxRecords <= 0) {
            return List.of();
        }
        return log.fetch(offset, maxRecords);
    }

    public void ensurePartition(PartitionId partition) {
        logs.computeIfAbsent(partition, ignored -> new PartitionLog());
    }

    public record AppendResult(long messageId, long offset) {}

    public record StoredRecord(
            long messageId,
            long offset,
            long timestampEpochMillis,
            byte[] key,
            byte[] value,
            Map<String, byte[]> headers) {}

    private static final class PartitionLog {
        private final List<StoredRecord> records = new ArrayList<>();
        private long nextOffset;

        synchronized AppendResult append(
                long messageId, byte[] key, byte[] value, Map<String, byte[]> headers, long timestampEpochMillis) {
            long offset = nextOffset++;
            records.add(
                    new StoredRecord(messageId, offset, timestampEpochMillis, key, value, Map.copyOf(headers)));
            return new AppendResult(messageId, offset);
        }

        synchronized StoredRecord read(long offset) {
            if (offset < 0 || offset >= records.size()) {
                return null;
            }
            return records.get((int) offset);
        }

        synchronized List<StoredRecord> fetch(long offset, int maxRecords) {
            if (offset < 0) {
                offset = 0;
            }
            int start = (int) offset;
            if (start >= records.size()) {
                return List.of();
            }
            int end = Math.min(records.size(), start + maxRecords);
            return List.copyOf(records.subList(start, end));
        }
    }
}
