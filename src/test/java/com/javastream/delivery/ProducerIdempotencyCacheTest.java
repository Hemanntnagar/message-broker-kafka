package com.javastream.delivery;

import com.javastream.partition.PartitionId;
import com.javastream.topic.TopicName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerIdempotencyCacheTest {

    private final ProducerIdempotencyCache cache = new ProducerIdempotencyCache();
    private final PartitionId partition = new PartitionId(new TopicName("orders"), 0);

    @Test
    void firstSequenceMustBeZero() {
        assertTrue(cache.beginIdempotentAppend(7, partition, 0).isEmpty());
        cache.recordSuccessfulAppend(7, partition, 0, 100, 0);

        var duplicate = cache.beginIdempotentAppend(7, partition, 0);
        assertTrue(duplicate.isPresent());
        assertEquals(100, duplicate.get().messageId());
        assertEquals(0, duplicate.get().offset());
    }

    @Test
    void rejectsGapsAndRegressions() {
        cache.recordSuccessfulAppend(1, partition, 0, 10, 0);

        assertThrows(IdempotentSequenceException.class, () -> cache.beginIdempotentAppend(1, partition, 2));
        assertTrue(cache.beginIdempotentAppend(1, partition, 0).isPresent());
        assertTrue(cache.beginIdempotentAppend(1, partition, 1).isEmpty());
        cache.recordSuccessfulAppend(1, partition, 1, 11, 1);
        assertThrows(IdempotentSequenceException.class, () -> cache.beginIdempotentAppend(1, partition, 3));
    }
}
