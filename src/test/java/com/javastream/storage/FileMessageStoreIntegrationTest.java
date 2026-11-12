package com.javastream.storage;

import com.javastream.partition.PartitionId;
import com.javastream.storage.log.FlushPolicy;
import com.javastream.storage.log.LogStorageException;
import com.javastream.storage.log.StorageSettings;
import com.javastream.topic.TopicName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileMessageStoreIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void append100kMessagesRecoverAfterRestart() throws Exception {
        StorageSettings settings = testSettings(tempDir, 64 * 1024);
        TopicName topic = new TopicName("orders");
        PartitionId p0 = new PartitionId(topic, 0);

        try (FileMessageStore store = newStore(settings)) {
            store.ensurePartition(p0);
            for (int i = 0; i < 100_000; i++) {
                byte[] value = ("msg-" + i).getBytes(StandardCharsets.UTF_8);
                store.appendRecord(p0, null, value, Map.of(), System.currentTimeMillis());
            }
            store.flush();
            assertEquals(100_000, store.logEndOffset(p0));
        }

        try (FileMessageStore restarted = newStore(settings)) {
            restarted.ensurePartition(p0);
            assertEquals(100_000, restarted.logEndOffset(p0));
            for (int i = 0; i < 100_000; i += 10_000) {
                List<BrokerMessageStore.StoredRecord> batch = restarted.fetch(p0, i, 1);
                assertEquals(1, batch.size());
                assertEquals(i, batch.getFirst().offset());
                assertArrayEquals(("msg-" + i).getBytes(StandardCharsets.UTF_8), batch.getFirst().value());
            }
        }
    }

    @Test
    void multiplePartitionsIndependentOffsets() throws Exception {
        StorageSettings settings = testSettings(tempDir, StorageSettings.DEFAULT_SEGMENT_MAX_BYTES);
        TopicName topic = new TopicName("multi");
        PartitionId p0 = new PartitionId(topic, 0);
        PartitionId p1 = new PartitionId(topic, 1);

        try (FileMessageStore store = newStore(settings)) {
            store.ensurePartition(p0);
            store.ensurePartition(p1);
            for (int i = 0; i < 50; i++) {
                store.appendRecord(p0, null, new byte[] {(byte) i}, Map.of(), 0);
                store.appendRecord(p1, null, new byte[] {(byte) (i + 100)}, Map.of(), 0);
            }
            store.flush();
        }

        try (FileMessageStore restarted = newStore(settings)) {
            restarted.ensurePartition(p0);
            restarted.ensurePartition(p1);
            assertEquals(50, restarted.logEndOffset(p0));
            assertEquals(50, restarted.logEndOffset(p1));
            assertEquals(49, restarted.fetch(p0, 49, 1).getFirst().value()[0]);
            assertEquals((byte) 149, restarted.fetch(p1, 49, 1).getFirst().value()[0]);
        }
    }

    @Test
    void segmentRollingCreatesMultipleLogFiles() throws Exception {
        StorageSettings settings = testSettings(tempDir, 512);
        TopicName topic = new TopicName("roll");
        PartitionId partition = new PartitionId(topic, 0);

        try (FileMessageStore store = newStore(settings)) {
            store.ensurePartition(partition);
            for (int i = 0; i < 200; i++) {
                store.appendRecord(
                        partition,
                        null,
                        ("payload-" + i).getBytes(StandardCharsets.UTF_8),
                        Map.of(),
                        0);
            }
            store.flush();
        }

        Path partitionDir = settings.brokerRoot().resolve("topics").resolve("roll").resolve("partition-0");
        long logFiles;
        try (var stream = Files.list(partitionDir)) {
            logFiles = stream.filter(p -> p.toString().endsWith(".log")).count();
        }
        assertTrue(logFiles >= 2, "expected multiple segments, got " + logFiles);

        try (FileMessageStore restarted = newStore(settings)) {
            restarted.ensurePartition(partition);
            assertEquals(200, restarted.logEndOffset(partition));
            List<BrokerMessageStore.StoredRecord> first = restarted.fetch(partition, 0, 1);
            List<BrokerMessageStore.StoredRecord> last = restarted.fetch(partition, 199, 1);
            assertNotNull(first.getFirst());
            assertNotNull(last.getFirst());
        }
    }

    @Test
    void incompleteTrailingRecordIsTruncatedOnRecovery() throws Exception {
        StorageSettings settings = testSettings(tempDir, StorageSettings.DEFAULT_SEGMENT_MAX_BYTES);
        TopicName topic = new TopicName("trunc");
        PartitionId partition = new PartitionId(topic, 0);

        try (FileMessageStore store = newStore(settings)) {
            store.ensurePartition(partition);
            for (int i = 0; i < 10; i++) {
                store.appendRecord(partition, null, new byte[] {(byte) i}, Map.of(), 0);
            }
            store.flush();
        }

        Path logFile = settings.brokerRoot()
                .resolve("topics")
                .resolve("trunc")
                .resolve("partition-0")
                .resolve("segment-000000.log");
        try (FileChannel channel = FileChannel.open(logFile, StandardOpenOption.WRITE)) {
            channel.position(channel.size());
            channel.write(ByteBuffer.wrap(new byte[] {9, 9, 9, 9, 9}));
        }

        try (FileMessageStore restarted = newStore(settings)) {
            restarted.ensurePartition(partition);
            assertEquals(10, restarted.logEndOffset(partition));
        }
    }

    @Test
    void checksumFailureStopsRecoveryBeforeCorruption() throws Exception {
        StorageSettings settings = testSettings(tempDir, StorageSettings.DEFAULT_SEGMENT_MAX_BYTES);
        TopicName topic = new TopicName("crc");
        PartitionId partition = new PartitionId(topic, 0);

        try (FileMessageStore store = newStore(settings)) {
            store.ensurePartition(partition);
            for (int i = 0; i < 5; i++) {
                store.appendRecord(partition, null, new byte[] {(byte) i}, Map.of(), 0);
            }
            store.flush();
        }

        Path logFile = settings.brokerRoot()
                .resolve("topics")
                .resolve("crc")
                .resolve("partition-0")
                .resolve("segment-000000.log");
        byte[] bytes = Files.readAllBytes(logFile);
        bytes[bytes.length / 2] ^= 0x01;
        Files.write(logFile, bytes);

        try (FileMessageStore restarted = newStore(settings)) {
            restarted.ensurePartition(partition);
            long end = restarted.logEndOffset(partition);
            assertTrue(end < 5);
            for (long i = 0; i < end; i++) {
                assertEquals(i, restarted.fetch(partition, i, 1).getFirst().offset());
            }
        }
    }

    @Test
    void logLayoutMatchesBrokerDirectoryContract() throws Exception {
        StorageSettings settings = testSettings(tempDir, StorageSettings.DEFAULT_SEGMENT_MAX_BYTES);
        TopicName topic = new TopicName("orders");
        PartitionId partition = new PartitionId(topic, 1);

        try (FileMessageStore store = newStore(settings)) {
            store.ensurePartition(partition);
            store.appendRecord(partition, null, new byte[] {1}, Map.of(), 0);
            store.flush();
        }

        Path base = settings.brokerRoot();
        assertTrue(Files.isDirectory(base.resolve("metadata")));
        Path partDir = base.resolve("topics").resolve("orders").resolve("partition-1");
        assertTrue(Files.exists(partDir.resolve("segment-000000.log")));
        assertTrue(Files.exists(partDir.resolve("segment-000000.index")));
    }

    private static FileMessageStore newStore(StorageSettings settings) throws Exception {
        return new FileMessageStore(settings);
    }

    private static StorageSettings testSettings(Path tempDir, long segmentMaxBytes) {
        return new StorageSettings(
                tempDir,
                0,
                segmentMaxBytes,
                FlushPolicy.NONE,
                StorageSettings.DEFAULT_FLUSH_INTERVAL_MS,
                8);
    }

    private static void assertArrayEquals(byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }
}
