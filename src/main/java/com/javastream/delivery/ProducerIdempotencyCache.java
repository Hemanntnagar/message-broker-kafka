package com.javastream.delivery;

import com.javastream.partition.PartitionId;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks per-producer partition sequence numbers and deduplicates retried appends.
 */
public final class ProducerIdempotencyCache {

    public record DedupResult(long messageId, int partition, long offset, boolean duplicate) {}

    private record State(long lastSequence, long messageId, int partition, long offset) {}

    private record Key(long producerId, String topic, int partition) {}

    private final ConcurrentHashMap<Key, State> states = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Key, Object> locks = new ConcurrentHashMap<>();

    /**
     * Validates sequence for an idempotent produce. Returns a cached append when the request is an exact retry.
     *
     * @throws IdempotentSequenceException when the sequence is out of order
     */
    public Optional<DedupResult> beginIdempotentAppend(
            long producerId, PartitionId partitionId, long sequenceNumber) {
        if (producerId < 0) {
            return Optional.empty();
        }
        Key key = new Key(producerId, partitionId.topic().value(), partitionId.index());
        synchronized (lock(key)) {
            State state = states.get(key);
            if (state == null) {
                if (sequenceNumber != 0) {
                    throw new IdempotentSequenceException(
                            "First sequence for producer %d on %s-%d must be 0, got %d"
                                    .formatted(
                                            producerId,
                                            partitionId.topic().value(),
                                            partitionId.index(),
                                            sequenceNumber));
                }
                return Optional.empty();
            }
            if (sequenceNumber == state.lastSequence()) {
                return Optional.of(
                        new DedupResult(state.messageId(), state.partition(), state.offset(), true));
            }
            if (sequenceNumber < state.lastSequence()) {
                throw new IdempotentSequenceException(
                        "Sequence %d is behind last committed %d for producer %d on partition %d"
                                .formatted(
                                        sequenceNumber,
                                        state.lastSequence(),
                                        producerId,
                                        partitionId.index()));
            }
            if (sequenceNumber != state.lastSequence() + 1) {
                throw new IdempotentSequenceException(
                        "Gap in producer sequence: expected %d but got %d"
                                .formatted(state.lastSequence() + 1, sequenceNumber));
            }
            return Optional.empty();
        }
    }

    public void recordSuccessfulAppend(
            long producerId, PartitionId partitionId, long sequenceNumber, long messageId, long offset) {
        if (producerId < 0) {
            return;
        }
        Key key = new Key(producerId, partitionId.topic().value(), partitionId.index());
        synchronized (lock(key)) {
            states.put(
                    key, new State(sequenceNumber, messageId, partitionId.index(), offset));
        }
    }

    public Map<Key, State> snapshot() {
        return Map.copyOf(states);
    }

    private Object lock(Key key) {
        return locks.computeIfAbsent(key, ignored -> new Object());
    }
}
