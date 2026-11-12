package com.javastream.topic;

import com.javastream.exception.JavaStreamException;
import com.javastream.partition.PartitionAssignment;
import com.javastream.partition.PartitionId;
import com.javastream.partition.PartitionMetadataStore;
import com.javastream.storage.InMemoryMessageStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicManagerTest {

    private TopicManager manager;
    private InMemoryMessageStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryMessageStore();
        manager = new TopicManager(3, new PartitionMetadataStore(), store, new PartitionAssignment());
    }

    @Test
    void createTopicWithMultiplePartitions() {
        Topic topic = manager.createTopic("orders", 4);
        assertEquals(4, topic.partitionCount());
        assertEquals(4, topic.partitions().size());
        assertEquals(List.of("orders"), manager.listTopics());
    }

    @Test
    void defaultPartitionCountUsedWhenOmitted() {
        Topic topic = manager.createTopic("events");
        assertEquals(3, topic.partitionCount());
    }

    @Test
    void rejectsDuplicateTopic() {
        manager.createTopic("dup", 2);
        assertThrows(JavaStreamException.class, () -> manager.createTopic("dup", 2));
    }

    @Test
    void rejectsInvalidPartitionCount() {
        assertThrows(JavaStreamException.class, () -> manager.createTopic("bad", 0));
        assertThrows(JavaStreamException.class, () -> manager.createTopic("bad2", -1));
    }

    @Test
    void deleteTopicRemovesCatalogEntry() {
        manager.createTopic("temp", 2);
        manager.deleteTopic("temp");
        assertTrue(manager.listTopics().isEmpty());
        assertTrue(manager.getTopicMetadata("temp").isEmpty());
    }

    @Test
    void deleteUnknownTopicFails() {
        assertThrows(JavaStreamException.class, () -> manager.deleteTopic("missing"));
    }

    @Test
    void getTopicMetadataReflectsPartitionOffsets() {
        manager.createTopic("meta", 2);
        TopicName name = new TopicName("meta");
        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        int p0 = manager.resolvePartition(name, -1, key);
        PartitionId id = new PartitionId(name, p0);
        var append = store.appendRecord(id, key, new byte[] {1}, Map.of(), 1L);
        manager.recordAppend(id, append.offset());
        var metadata = manager.getTopicMetadata("meta").orElseThrow();
        assertEquals(2, metadata.partitionCount());
        long nonEmpty =
                metadata.partitions().stream().filter(pm -> pm.recordCount() > 0).count();
        assertEquals(1, nonEmpty);
    }

    @Test
    void sameKeySelectsSamePartition() {
        manager.createTopic("hash", 8);
        TopicName name = new TopicName("hash");
        byte[] key = "user-42".getBytes(StandardCharsets.UTF_8);
        int first = manager.resolvePartition(name, -1, key);
        for (int i = 0; i < 20; i++) {
            assertEquals(first, manager.resolvePartition(name, -1, key));
        }
    }

    @Test
    void noKeyUsesRoundRobinAcrossPartitions() {
        manager.createTopic("rr", 3);
        TopicName name = new TopicName("rr");
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 9; i++) {
            seen.add(manager.resolvePartition(name, -1, null));
        }
        assertEquals(3, seen.size());
    }

    @Test
    void offsetsIncreasePerPartition() throws Exception {
        manager.createTopic("offsets", 1);
        TopicName name = new TopicName("offsets");
        PartitionId id = new PartitionId(name, 0);
        for (int i = 0; i < 5; i++) {
            var result = store.appendRecord(id, null, new byte[] {(byte) i}, Map.of(), 1L);
            manager.recordAppend(id, result.offset());
            assertEquals(i, result.offset());
        }
        var meta = manager.getTopicMetadata("offsets").orElseThrow();
        assertEquals(5, meta.partitions().getFirst().logEndOffset());
        assertEquals(5, meta.partitions().getFirst().recordCount());
    }

    @Test
    void concurrentPublishingPreservesOffsetSequences() throws Exception {
        manager.createTopic("concurrent", 1);
        TopicName name = new TopicName("concurrent");
        PartitionId id = new PartitionId(name, 0);
        int threads = 8;
        int perThread = 50;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger failures = new AtomicInteger();
        Object lock = new Object();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threads; t++) {
                pool.submit(
                        () -> {
                            try {
                                start.await();
                                for (int i = 0; i < perThread; i++) {
                                    synchronized (lock) {
                                        var append =
                                                store.appendRecord(id, null, new byte[] {1}, Map.of(), System.nanoTime());
                                        try {
                                            manager.recordAppend(id, append.offset());
                                        } catch (RuntimeException e) {
                                            failures.incrementAndGet();
                                        }
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                done.countDown();
                            }
                        });
            }
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
        }
        assertEquals(0, failures.get());
        var meta = manager.getTopicMetadata("concurrent").orElseThrow();
        assertEquals(threads * perThread, meta.partitions().getFirst().recordCount());
    }

    @Test
    void partitionMetadataIndependentAcrossPartitions() {
        manager.createTopic("multi", 3);
        TopicName name = new TopicName("multi");
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 30; i++) {
            byte[] key = ("key-" + (i % 5)).getBytes(StandardCharsets.UTF_8);
            int partition = manager.resolvePartition(name, -1, key);
            PartitionId id = new PartitionId(name, partition);
            var append = store.appendRecord(id, key, new byte[] {(byte) i}, Map.of(), 1L);
            manager.recordAppend(id, append.offset());
            counts.merge(partition, 1, Integer::sum);
        }
        var meta = manager.getTopicMetadata("multi").orElseThrow();
        assertEquals(3, meta.partitions().size());
        long withRecords = meta.partitions().stream().filter(p -> p.recordCount() > 0).count();
        assertTrue(withRecords >= 1);
    }
}
